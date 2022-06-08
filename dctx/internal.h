#include <stdbool.h>
#include <pthread.h>

#include <uv.h>

#include "dctx.h"

#define rprintf(fmt, ...) printf("[rank=%d] " fmt, dctx->rank, ##__VA_ARGS__)

#define RBUG(msg) fprintf(stderr, "[rank=%d] BUG: " msg "\n", dctx->rank)
#define BUG(msg) fprintf(stderr, "BUG: " msg "\n")

#include "link.h"
#include "msg.h"
#include "zstring.h"

struct dc_result {
    bool ok;
    // how many data and len pairs
    size_t ndata;
    char **data;
    size_t *len;
};

extern dc_result_t DC_RESULT_NOT_OK;
extern dc_result_t DC_RESULT_EMPTY;

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

#include "op.h"

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

    // closed is set by close_everything
    bool closed;

    // the async-related stuff must always be async-protected
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

char *bytesdup(const char *data, size_t len);

// our generic dc_write_cb reads a dc_write_cb_t* from req->data
void dc_write_cb(uv_write_t *req, int status);

// let *cb handle *base however it chooses
int tcp_write_ex(uv_tcp_t *tcp, char *base, size_t len, dc_write_cb_t *cb);
// will call free(base)
int tcp_write(uv_tcp_t *tcp, char *base, size_t len);
// will copy base first, then call tcp_write
int tcp_write_copy(uv_tcp_t *tcp, const char *base, size_t len);

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
