typedef enum {
    DC_OP_GATHER,
    DC_OP_BROADCAST,
    DC_OP_ALLGATHER,
} dc_op_type_e;

struct dc_op {
    struct dctx *dctx;
    link_t link;  // dctx->a.inflight or dctx->a.completed

    // was the op created successfully
    bool ok;

    // every operation has a type and a series
    dc_op_type_e type;
    char series[256];
    size_t slen;

    /* ready is set when the op is moved to completed, and only after that can
       an external thread take the operation for itself */
    bool ready;

    union {
        union {
            // a chief gather is complete when nrecvd == dctx->size
            // (gather call counts for one nrecvd)
            struct {
                char **recvd;
                size_t *len;
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
        union {
            // a chief broadcast is complete when all dc_op_write_cbs finish
            struct {
                // chief always copies the input data
                bool write_started;
                char *data;
                size_t len;
                dc_write_cb_t cb;
                size_t nsent;
            } chief;
            /* a worker broadcast is complete when it receives the message and
               has a matching broadcast call */
            struct {
                bool called;
                char *recvd;
                size_t len;
            } worker;
        } broadcast;
        union {
            // a chief allgather is complete when all dc_op_write_cbs finish
            // (allgather call counts for one nrecvd)
            struct {
                // what workers send to us
                // worker messages we have received
                char **recvd;
                size_t *len;
                size_t nrecvd;
                // what we send to workers
                bool write_started;
                dc_write_cb_t cb;
                size_t nsent;
            } chief;
            // a worker allgather is complete when it receives the all messages
            struct {
                // what we send to the chief
                char *data;
                const char *nofree;
                size_t datalen;
                bool sent;
                dc_write_cb_t cb;
                // what the chief sends back
                char **recvd;
                size_t *len;
                size_t nrecvd;
            } worker;
        } allgather;
    } u;
};
DEF_CONTAINER_OF(dc_op_t, link, link_t)

extern dc_op_t DC_OP_NOT_OK;

// the caller must insert into inflight in a thread-safe way
dc_op_t *dc_op_new(
    dctx_t *dctx, dc_op_type_e type, const char *series, size_t slen
);
// the caller must have removed from the linked list in a thread-safe way
void dc_op_free(dc_op_t *op);
void mark_op_completed_locked(dc_op_t *op);
void mark_op_completed_and_notify(dc_op_t *op);
void dc_op_write_cb(dc_op_t *op);
bool dc_op_advance(dc_op_t *op);
dc_op_t *get_op_for_recv(
    dctx_t *dctx, dc_op_type_e type, const char *series, size_t slen, int rank
);
dc_op_t *get_op_for_call_locked(
    dctx_t *dctx, dc_op_type_e type, const char *series, size_t slen
);
