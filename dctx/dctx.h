// REQUIRES: stdbool.h
// REQUIRES: stdint.h

// only support an opaque pointer
struct dc_result;
// succesful results (ok == true) MUST be freed
void dc_result_free(struct dc_result **r);
// if result is not ok, you MAY skip freeing the result
bool dc_result_ok(struct dc_result *r);
// check how many data are being returned
size_t dc_result_count(struct dc_result *r);
// each data MAY be taken once, and you MUST free it if you take it
char *dc_result_take(struct dc_result *r, size_t i, size_t *len);

//

// only support an opaque pointer
struct dctx;

/* returns:
    0: success
    1: check errno
    2: other TODO fix these

   Any struct dctx created by dctx_open MUST be stopped by dctx_close.
*/
int dctx_open(
    struct dctx **dctx,
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
void dctx_close(struct dctx **dctx);

struct dctx *dctx_open2(
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_host,
    const char *chief_svc
);
void dctx_close2(struct dctx *dctx);


// will free(*data) eventually
struct dc_result *dctx_gather(struct dctx *dctx, char *data, size_t len);
int dctx_gather_start(struct dctx *dctx, char *data, size_t len);
struct dc_result *dctx_gather_end(struct dctx *dctx);
