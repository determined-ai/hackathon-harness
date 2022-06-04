#include <stdbool.h>
#include <pthread.h>

#include <uv.h>

#include "dctx.h"

#define rprintf(fmt, ...) printf("[rank=%d] " fmt, dctx->rank, ##__VA_ARGS__)

#define RBUG(msg) fprintf(stderr, "[rank=%d] BUG: " msg "\n", dctx->rank);

// circularly linked lists, where the head element is not part of the list

typedef struct link {
    struct link *prev;
    struct link *next;
} link_t;

void link_init(link_t *l);

/* prepend/append a single element.  You must ensure that link is not in a list
   via link_remove() before calling these; the new and old lists may have
   different thread safety requirements */
void link_list_prepend(link_t *head, link_t *link);
void link_list_append(link_t *head, link_t *link);

// pop a single element, or return NULL if there is none
link_t *link_list_pop_first(link_t *head);
link_t *link_list_pop_last(link_t *head);

void link_remove(link_t *link);

bool link_list_isempty(link_t *head);

/* DEF_CONTAINER_OF should be used right after struct definition to create an
   inline function for dereferencing a struct via a member.  "member_type" is
   required to avoid typeof, which windows doesn't have.  Also, unlike the
   linux kernel version, multi-token types ("struct xyz") are not supported. */
#define DEF_CONTAINER_OF(structure, member, member_type) \
    static inline structure *structure ## _ ## member ## _container_of( \
            const member_type *ptr){ \
        if(ptr == NULL) return NULL; \
        uintptr_t offset = offsetof(structure, member); \
        return (structure*)((uintptr_t)ptr - offset); \
    }

#define CONTAINER_OF(ptr, structure, member) \
    structure ## _ ## member ## _container_of(ptr)

// automate for-loops which call CONTAINER_OF for each link in list
#define LINK_FOR_EACH(var, head, structure, member) \
    for(var = CONTAINER_OF((head)->next, structure, member); \
        var && &var->member != (head); \
        var = CONTAINER_OF(var->member.next, structure, member))

// same thing but use a temporary variable to be safe against link_remove
#define LINK_FOR_EACH_SAFE(var, temp, head, structure, member) \
    for(var = CONTAINER_OF((head)->next, structure, member), \
        temp = !var?NULL:CONTAINER_OF(var->member.next, structure, member); \
        var && &var->member != (head); \
        var = temp, \
        temp = CONTAINER_OF(var->member.next, structure, member))


struct dc_result {
    bool ok;
    // how many data and len pairs
    size_t ndata;
    char **data;
    size_t *len;
};

extern dc_result_t NOT_OK;
extern dc_result_t OK_EMPTY;

typedef struct {
    char type;  // "i"nit, "m"essage, "k"eepalive
    size_t nread_before;
    // init arg
    int32_t rank;
    // msg args
    uint32_t len;
    char *body;
} dc_unmarshal_t;

typedef struct {
    int rank;
    uv_tcp_t tcp;
    dc_unmarshal_t unmarshal;
    link_t link;
} dc_conn_t;
DEF_CONTAINER_OF(dc_conn_t, link, link_t)

enum dc_status {
    // before the thread has begun
    DCTX_PRESTART=0,
    // after the thread has started processing events
    DCTX_RUNNING,
    // after a disconnection has been processed
    // TODO: get rid of STOPPING by allowing reconnecting
    DCTX_STOPPING,
    // after the thread has stopped
    DCTX_DONE,
};

// dc_write_cb is the .data we assign to every uv_write_t
typedef enum {
    // just free a buffer
    WRITE_CB_FREE,
    // pass u.op to dc_op_write_cb
    WRITE_CB_OP,
} dc_write_cb_e;

typedef struct {
    dc_write_cb_e type;
    union {
        void *free;
        dc_op_t *op;
    } u;
} dc_write_cb_t;

typedef enum {
    DC_OP_GATHER,
} dc_op_type_e;

struct dc_op {
    struct dctx *dctx;
    link_t link;  // dctx->a.inflight or dctx->a.completed

    // every operation has a type and a series
    dc_op_type_e type;
    char *series;

    /* ready is set when the op is moved to completed, and only after that can
       an external thread take the operation for itself */
    bool ready;

    union {
        union {
            // a chief gather is complete when nrecvd == dctx->size
            // (gather_start counts for one nrecvd)
            struct {
                // client messages we have received
                char **recvd;
                size_t *len;
                // count of client msgs
                size_t nrecvd;
            } chief;
            // a worker gather is complete when the dc_op_write_cb finishes
            struct {
                // either data or nofree is defined
                char *data;
                const char *nofree;
                size_t len;
                bool sent;
                // the worker's gather finishes in dc_op_write_cb
                dc_write_cb_t cb;
            } worker;
        } gather;
    } u;
};
DEF_CONTAINER_OF(dc_op_t, link, link_t)

struct dctx {
    int rank;
    int size;
    int local_rank;
    int local_size;
    int cross_rank;
    int cross_size;

    char *host;
    char *svc;

    uv_loop_t loop;
    uv_async_t async;
    uv_tcp_t tcp;
    bool tcp_open;
    dc_unmarshal_t unmarshal;

    // XXX: I think a should always be mutex protected
    // Well... I think some things are written in one direction only, so only
    // have to be locked when writing, or when reading from the other direction.
    // a.ready is like that.
    struct {
        bool started;
        bool close;
        // ready means all connections are made
        bool ready;
        // lists of ops
        link_t inflight;  // dc_op_t->link
        link_t complete;  // dc_op_t->link
    } a;

    struct {
        // connections which have not identified themselves yet
        link_t preinit;  // dc_conn_t->link
        // connections of known rank
        dc_conn_t **peers;
        size_t npeers;
    } server;

    struct {
        uv_getaddrinfo_t gai_req;
        struct addrinfo *gai;
        struct addrinfo *ptr;
        uv_connect_t conn_req;
        uv_timer_t timer;
        bool connected;
    } client;

    // called on failed read or failed write
    void (*on_broken_connection)(struct dctx*, uv_stream_t*);

    // called on every successful read
    void (*on_read)(struct dctx*, uv_stream_t*, char*, size_t);

    pthread_t thread;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int status;
    bool failed;
};

dc_result_t *dc_result_new(size_t ndata);
void dc_result_set(dc_result_t *r, size_t i, char *data, size_t len);

void advance_state(struct dctx *dctx);

void noop_handle_closer(uv_handle_t *handle);
void close_everything(struct dctx *dctx);

void uv_perror(const char *msg, int ret);

void allocator(uv_handle_t *handle, size_t suggest, uv_buf_t *buf);
void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);

// calls on_unmarshal once for every message found
int unmarshal(
    dc_unmarshal_t *unmarshal,
    char *buf,
    size_t len,
    void (*on_unmarshal)(dc_unmarshal_t*, void*),
    void *arg
);
void unmarshal_free(dc_unmarshal_t *unmarshal);

char *bytesdup(const char *data, size_t len);

// our generic dc_write_cb reads a dc_write_cb_t* from req->data
void dc_write_cb(uv_write_t *req, int status);

// let *cb handle *base however it chooses
int tcp_write_ex(uv_tcp_t *tcp, char *base, size_t len, dc_write_cb_t *cb);
// will call free(base)
int tcp_write(uv_tcp_t *tcp, char *base, size_t len);
// will copy base first, then call tcp_write
int tcp_write_copy(uv_tcp_t *tcp, const char *base, size_t len);

// op.c

// the caller must insert into inflight in a thread-safe way
dc_op_t *dc_op_new(dctx_t *dctx, dc_op_type_e type, const char *series);
// the caller must have removed from the linked list in a thread-safe way
void dc_op_free(dc_op_t *op, int rank);
void mark_op_completed_locked(dc_op_t *op);
void mark_op_completed_and_notify(dc_op_t *op);
void dc_op_write_cb(dc_op_t *op);
bool dc_op_advance(dc_op_t *op);
dc_op_t *get_op_for_recv(
    dctx_t *dctx, dc_op_type_e type, const char *series, int rank
);
dc_op_t *get_op_for_call_locked(
    dctx_t *dctx, dc_op_type_e type, const char *series
);

// server.c

dc_conn_t *dc_conn_new(void);

void dc_conn_close(dc_conn_t *conn);

int bind_via_gai(uv_tcp_t *srv, const char *addr, const char *svc);

int init_server(struct dctx *dctx);

void server_enable_reads(struct dctx *dctx);

// client.c

int start_gai(struct dctx *dctx);
void gai_cb(uv_getaddrinfo_t *req, int status, struct addrinfo *res);

int conn_next(struct dctx *dctx);
void conn_cb(uv_connect_t *req, int status);

void close_for_retry(struct dctx *dctx);
void close_for_retry_cb(uv_handle_t *handle);

int retry_later(struct dctx *dctx);
void retry_cb(uv_timer_t *handle);

int init_client(struct dctx *dctx);

// const.c
char *i_promise_i_wont_touch(const char *data);
