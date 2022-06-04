// REQUIRES: stdbool.h
// REQUIRES: stdint.h

// only support an opaque pointer
struct dc_result;
typedef struct dc_result dc_result_t;
// succesful results (ok == true) MUST be freed
void dc_result_free(dc_result_t **r);
void dc_result_free2(dc_result_t *r);
// if result is not ok, you MAY skip freeing the result
bool dc_result_ok(dc_result_t *r);
// check how many data are being returned
size_t dc_result_count(dc_result_t *r);
// get the length of a particular result
size_t dc_result_len(dc_result_t *r, size_t i);
// each data MAY be taken once, and you MUST free it if you take it
char *dc_result_take(dc_result_t *r, size_t i);
// you can peek as many times as you like, but you MUST NOT free what you get
const char *dc_result_peek(dc_result_t *r, size_t i);

/* dc_op_t*:
    - created by gather_start*
    - cannot be canceled
    - autmoatically freed during dc_op_await or dctx_close
*/
// an op is created by *start*(), freed by either of dc_op_await or dctx_close
struct dc_op;
typedef struct dc_op dc_op_t;

// get a dc_result after a dc_op completes
dc_result_t *dc_op_await(dc_op_t *op);

// only support an opaque pointer
struct dctx;
typedef struct dctx dctx_t;

/* returns:
    0: success
    1: check errno
    2: other TODO fix these

   Any dctx_t created by dctx_open MUST be stopped by dctx_close.
*/
int dctx_open(
    dctx_t **dctx,
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_host,
    const char *chief_svc
);

// can tolerate *dctx=NULL, otherwise eventually sets *dctx=NULL
void dctx_close(dctx_t **dctx);

dctx_t *dctx_open2(
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_host,
    const char *chief_svc
);
void dctx_close2(dctx_t *dctx);


// guarantees an eventual call to free(data)
// (technically the chief's data is passed back out as dc_result_take)
dc_op_t *dctx_gather_start(
    dctx_t *dctx, const char *series, char *data, size_t len
);

// copies data (and frees the copy later)
dc_op_t *dctx_gather_start_copy(
    dctx_t *dctx, const char *series, const char *data, size_t len
);

// caller is required to preserve data until after dctx_gather_end()
/* Note that the chief always makes a copy of the data, which will eventually
   be returned by dc_result_take, to match the memory requirements of the
   worker results, which always require freeing after dc_result_take. */
dc_op_t *dctx_gather_start_nofree(
    dctx_t *dctx, const char *series, const char *data, size_t len
);
