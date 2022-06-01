#include <stdbool.h>
#include <pthread.h>

#include <uv.h>

#include "dctx.h"

#define rprintf(fmt, ...) printf("[rank=%d] " fmt, dctx->rank, ##__VA_ARGS__)

struct dc_result {
    bool ok;
    // how many data and len pairs
    size_t ndata;
    char **data;
    size_t *len;
};

extern struct dc_result NOT_OK;

struct dc_buf {
    char *base;
    size_t len;
    size_t skip;
    struct dc_buf *next;
};

struct dc_unmarshal {
    char type;  // "i"nit, "m"essage, "k"eepalive
    size_t nread_before;
    // init arg
    int32_t rank;
    // msg args
    uint32_t len;
    char *body;
};

struct dc_conn {
    int rank;
    uv_tcp_t tcp;
    struct dc_unmarshal unmarshal;
    struct dc_conn *next;
    struct dc_conn *prev;
};

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

struct dctx {
    int rank;
    int size;
    int local_rank;
    int local_size;
    int cross_rank;
    int cross_size;

    uv_loop_t loop;
    uv_async_t async;
    // XXX: track when main tcp needs closing still?
    uv_tcp_t tcp;
    struct dc_unmarshal unmarshal;

    struct {
        bool started;
        bool close;
    } a;

    struct {
        // connections which have not identified themselves yet
        struct dc_conn *preinit;
        // connections of known rank
        struct dc_conn **peers;
    } server;

    struct {
        uv_getaddrinfo_t gai_req;
        struct addrinfo *gai;
        struct addrinfo *ptr;
        uv_connect_t conn_req;
        uv_timer_t timer;
    } client;

    // common hooks
    void (*on_broken_connection)(struct dctx*, uv_stream_t*);
    void (*on_read)(struct dctx*, uv_stream_t*, const uv_buf_t*);
    void (*on_msg)(struct dctx*, uv_stream_t*);

    pthread_t thread;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int status;
    bool failed;
};

void noop_handle_closer(uv_handle_t *handle);
void close_everything(struct dctx *dctx);

void uv_perror(const char *msg, int ret);

void allocator(uv_handle_t *handle, size_t suggest, uv_buf_t *buf);
void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);

// calls on_unmarshal once for every message found
int unmarshal(
    struct dc_unmarshal *unmarshal,
    const uv_buf_t *buf,
    void (*on_unmarshal)(struct dc_unmarshal*, void*),
    void *arg
);
void unmarshal_free(struct dc_unmarshal *unmarshal);

// server

struct dc_conn *dc_conn_new(void);

// returns the next dc_conn, or NULL
struct dc_conn *dc_conn_close(struct dc_conn *conn);

int bind_via_gai(uv_tcp_t *srv, const char *addr, const char *svc);

int init_server(struct dctx *dctx);

// client

int start_gai(struct dctx *dctx);
void gai_cb(uv_getaddrinfo_t *req, int status, struct addrinfo *res);

int conn_next(struct dctx *dctx);
void conn_cb(uv_connect_t *req, int status);

void close_for_retry(struct dctx *dctx);
void close_for_retry_cb(uv_handle_t *handle);

int retry_later(struct dctx *dctx);
void retry_cb(uv_timer_t *handle);

int init_client(struct dctx *dctx);
