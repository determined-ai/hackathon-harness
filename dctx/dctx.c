#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

dc_result_t DC_RESULT_NOT_OK = { .ok = false };
dc_result_t DC_RESULT_EMPTY = { .ok = true };

void dc_result_free(dc_result_t **rptr) {
    dc_result_t *r = *rptr;
    dc_result_free2(r);
    *rptr = NULL;
}

void dc_result_free2(dc_result_t *r) {
    if(r == NULL || r == &DC_RESULT_NOT_OK || r == &DC_RESULT_EMPTY) return;
    if(r->data != NULL){
        for(size_t i = 0; i < r->ndata; i++){
            char *data = r->data[i];
            if(data != NULL) free(data);
        }
        free(r->data);
    }
    if(r->len != NULL) free(r->len);
    free(r);
}

bool dc_result_ok(dc_result_t *r){
    return r->ok;
}

size_t dc_result_count(dc_result_t *r){
    return r->ndata;
}

size_t dc_result_len(dc_result_t *r, size_t i){
    return r->len[i];
}

char *dc_result_take(dc_result_t *r, size_t i){
    char *out = r->data[i];
    r->data[i] = NULL;
    return out;
}

const char *dc_result_peek(dc_result_t *r, size_t i){
    return r->data[i];
}

dc_result_t *dc_result_new(size_t ndata){
    dc_result_t *out = malloc(sizeof(*out));
    if(!out) return NULL;
    *out = (dc_result_t){ .ok = true, .ndata = ndata };

    out->data = malloc(ndata * sizeof(*out->data));
    if(!out->data){
        free(out);
        return NULL;
    }
    for(size_t i = 0; i < ndata; i++) out->data[i] = NULL;

    out->len = malloc(ndata * sizeof(*out->len));
    if(!out->len){
        free(out->data);
        free(out);
        return NULL;
    }
    for(size_t i = 0; i < ndata; i++) out->len[i] = 0;

    return out;
}

void dc_result_set(dc_result_t *r, size_t i, char *data, size_t len){
    r->data[i] = data;
    r->len[i] = len;
}

static void *dctx_thread(void *arg){
    dctx_t *dctx = arg;

    // trigger initial async_cb
    uv_async_send(&dctx->async);

    int ret = uv_run(&dctx->loop, UV_RUN_DEFAULT);
    if(ret < 0){
        printf("uv_run failed!\n"); // TODO
        dctx->failed = true;
    }

    // notify any listeners we're dead
    pthread_mutex_lock(&dctx->mutex);
    dctx->status = DCTX_DONE;
    pthread_cond_broadcast(&dctx->cond);
    pthread_mutex_unlock(&dctx->mutex);

    return NULL;
}

void advance_state(dctx_t *dctx){
    pthread_mutex_lock(&dctx->mutex);
    bool want_notify = false;

    // a.close: async shuts down loop from within
    if(dctx->a.close){
        close_everything(dctx);
        // do nothing else
        goto unlock;
    }

    // a.started: async alerts main thread loop is running successfully
    if(!dctx->a.started){
        dctx->a.started = true;

        // start connections
        int ret = dctx->rank == 0 ? init_server(dctx) : init_client(dctx);
        if(ret){
            printf("init_%s failed\n", dctx->rank == 0 ? "server" : "client");
            goto fail;
        }

        dctx->status = DCTX_RUNNING;
        want_notify = true;
    }

    // don't allow any writes while we are waiting for peers to connect still
    if(!dctx->a.ready){
        if(dctx->rank == 0){
            // chief checks all peers are connected
            if(dctx->server.npeers + 1 < (size_t)dctx->size) goto unlock;
        }else{
            // worker checks if it has connected to chief
            if(!dctx->client.connected) goto unlock;
        }

        dctx->a.ready = true;
        want_notify = true;
    }

    // check if any inflight operations became completed
    dc_op_t *op, *temp;
    LINK_FOR_EACH_SAFE(op, temp, &dctx->a.inflight, dc_op_t, link){
        // allow op to do some work if necessary
        if(!dc_op_advance(op)) continue;
        // if op is completed, mark it as such
        want_notify = true;
        mark_op_completed_locked(op);
    }

unlock:
    if(want_notify){
        pthread_cond_broadcast(&dctx->cond);
    }
    pthread_mutex_unlock(&dctx->mutex);
    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
    pthread_mutex_unlock(&dctx->mutex);
}


static void async_cb(uv_async_t *handle){
    dctx_t *dctx = handle->loop->data;
    advance_state(dctx);
}

dctx_t *dctx_open2(
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_host,
    const char *chief_svc
){
    if(rank < 0 || rank > INT32_MAX){
        fprintf(stderr, "invalid rank: %d\n", rank);
        return NULL;
    }
    dctx_t *out;
    int ret = dctx_open(
        &out,
        rank,
        size,
        local_rank,
        local_size,
        cross_rank,
        cross_size,
        chief_host,
        chief_svc
    );
    if(ret) return NULL;
    return out;
}

int dctx_open(
    dctx_t **dctx_out,
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_host,
    const char *chief_svc
){

    dctx_t *dctx = malloc(sizeof(*dctx));
    if(!dctx) return 1;
    *dctx = (dctx_t){
        .rank = rank,
        .size = size,
        .local_rank = local_rank,
        .local_size = local_size,
        .cross_rank = cross_rank,
        .cross_size = cross_size,
    };

    dctx->host = strdup(chief_host);
    if(!dctx->host){
        perror("strdup");
        return 1; // TODO
    }

    dctx->svc = strdup(chief_svc);
    if(!dctx->svc){
        perror("strdup");
        return 1; // TODO
    }


    int ret = uv_loop_init(&dctx->loop);
    if(ret < 0){
        uv_perror("uv_loop_init", ret); // TODO
        return 2;
    }
    // keep pointer to dctx from the uv_loop_t
    dctx->loop.data = dctx;

    if(rank == 0){
        // chief
        dctx->server.peers = malloc((size_t)size*sizeof(*dctx->server.peers));
        if(!dctx->server.peers){
            perror("malloc"); // TODO
            return 1;
        }
        for(int i = 0; i < size; i++){
            dctx->server.peers[i] = NULL;
        }
    }

    ret = uv_async_init(&dctx->loop, &dctx->async, async_cb);
    if(ret < 0){
        uv_perror("uv_loop_init", ret); // TODO
        return 2;
    }

    ret = pthread_mutex_init(&dctx->mutex, NULL);
    if(ret != 0){
        perror("pthread_mutex_init"); // TODO
        return 1;
    }

    ret = pthread_cond_init(&dctx->cond, NULL);
    if(ret != 0){
        perror("pthread_cond_init"); // TODO
        return 1;
    }


    ret = pthread_create(&dctx->thread, NULL, dctx_thread, dctx);
    if(ret != 0){
        perror("pthread_create"); // TODO
        return 1;
    }

    // wait for the uv loop to either start running or crash
    pthread_mutex_lock(&dctx->mutex);
    while(dctx->status == DCTX_PRESTART){
        pthread_cond_wait(&dctx->cond, &dctx->mutex);
    }
    pthread_mutex_unlock(&dctx->mutex);

    *dctx_out = dctx;

    return 0;
}


void dctx_close(dctx_t **dctxptr){
    dctx_t *dctx = *dctxptr;
    if(!dctx) return;
    dctx_close2(dctx);
    *dctxptr = NULL;
}

void dctx_close2(dctx_t *dctx){
    rprintf("dctx_close!\n");

    // ask the loop to shutdown
    pthread_mutex_lock(&dctx->mutex);
    dctx->a.close = true;
    pthread_mutex_unlock(&dctx->mutex);
    uv_async_send(&dctx->async);

    // wait for the loop to shutdown
    pthread_join(dctx->thread, NULL);

    bool success = dctx->status == DCTX_DONE && !dctx->failed;
    rprintf("loop ended in %s\n", success ? "success" : "failure");
    if(!success){
        rprintf("status ended as %d\n", dctx->status);
        rprintf("failed? %s\n", dctx->failed ? "true" : "false");
    }

    pthread_cond_destroy(&dctx->cond);
    pthread_mutex_destroy(&dctx->mutex);
    uv_loop_close(&dctx->loop);

    // free inflight and completed ops
    link_t *link;
    while((link = link_list_pop_first(&dctx->a.inflight))){
        dc_op_t *op = CONTAINER_OF(link, dc_op_t, link);
        dc_op_free(op, dctx->rank);
    }
    while((link = link_list_pop_first(&dctx->a.complete))){
        dc_op_t *op = CONTAINER_OF(link, dc_op_t, link);
        dc_op_free(op, dctx->rank);
    }

    if(dctx->rank == 0){
        // chief
        // connections must all have been closed by now
        free(dctx->server.peers);
    }else{
        // client
        uv_freeaddrinfo(dctx->client.gai);
        dctx->client.gai = NULL;
    }
    unmarshal_free(&dctx->unmarshal);
    free(dctx->host);
    free(dctx->svc);
    free(dctx);
}

char *bytesdup(const char *data, size_t len){
    char *out = malloc(len);
    if(!out){
        perror("malloc");
        return NULL;
    }
    memcpy(out, data, len);
    return out;
}

void noop_handle_closer(uv_handle_t *handle){
    (void)handle;
};

void close_everything(dctx_t *dctx){
    // close the async
    uv_close((uv_handle_t*)&dctx->async, noop_handle_closer);
    // close the main tcp
    if(dctx->tcp_open){
        uv_close((uv_handle_t*)&dctx->tcp, noop_handle_closer);
        dctx->tcp_open = false;
    }
    if(dctx->rank == 0){
        // chief closes preinit tcps
        link_t *link;
        while((link = link_list_pop_first(&dctx->server.preinit))){
            dc_conn_t *conn = CONTAINER_OF(link, dc_conn_t, link);
            dc_conn_close(conn);
        }
        // chief closes peer tcps
        for(size_t i = 0; i < (size_t)dctx->size; i++){
            dc_conn_close(dctx->server.peers[i]);
            dctx->server.peers[i] = NULL;
        }
    }
    dctx->closed = true;
}


void uv_perror(const char *msg, int ret){
    fprintf(stderr, "%s: %s\n", msg, uv_strerror(ret));
}

void allocator(uv_handle_t *handle, size_t suggest, uv_buf_t *buf){
    dctx_t *dctx = handle->loop->data;
    buf->base = malloc(suggest);
    buf->len = suggest;
    if(!buf->base){
        perror("malloc");
        goto fail;
    }
    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf){
    dctx_t *dctx = stream->loop->data;
    // handle error cases
    if(nread < 1){
        if(buf->base) free(buf->base);
        if(nread == UV_EOF || nread == UV_ECONNRESET){
            // socket is closed
            dctx->on_broken_connection(dctx, stream);
            return;
        }else if(nread == UV_ENOBUFS){
            // allocator failed
            // (failure handled inside allocator, we can ignore it here)
            return;
        }else if(nread == 0 || nread == UV_ECANCELED){
            // either EAGAIN or EWOULDBLOCK, or read was canceled
            return;
        }else if(nread == 0){
            return;
        }
        // any other error is fatal
        uv_perror("read_cb", (int)nread);
        goto fail;
    }

    dctx->on_read(dctx, stream, buf->base, (size_t)nread);

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}


void dc_write_cb(uv_write_t *req, int status){
    dctx_t *dctx = req->handle->loop->data;

    if(status < 0){
        uv_perror("write_cb", status);
        dctx->on_broken_connection(dctx, req->handle);
        goto fail;
    }

    // success
    goto handle_cb;

fail:
    dctx->failed = true;
    close_everything(dctx);

handle_cb:
    dc_write_cb_t *cb = req->data;
    if(!cb) return;
    switch(cb->type){
        case WRITE_CB_FREE:
            free(cb->u.free);
            free(cb);
            break;
        case WRITE_CB_OP:
            dc_op_write_cb(cb->u.op);
            break;
    }
    return;
}

int tcp_write_ex(uv_tcp_t *tcp, char *base, size_t len, dc_write_cb_t *cb){
    uv_write_t *req = malloc(sizeof(*req));
    if(!req){
        perror("malloc");
        goto fail;
    }
    req->data = cb;

    uv_buf_t buf = { .base = base, .len = len };

    int ret = uv_write(req, (uv_stream_t*)tcp, &buf, 1, dc_write_cb);
    if(ret < 0){
        uv_perror("uv_write", ret);
        goto fail_req;
    }

    return 0;

fail_req:
    free(req);
fail:
    return 1;
}

// owns base
int tcp_write(uv_tcp_t *tcp, char *base, size_t len){
    dc_write_cb_t *cb = malloc(sizeof(*cb));
    if(!cb){
        perror("malloc");
        goto fail_base;
    }

    // configure dc_write_cb to free the object
    *cb = (dc_write_cb_t){
        .type = WRITE_CB_FREE,
        .u = { .free = base },
    };

    int ret = tcp_write_ex(tcp, base, len, cb);
    if(ret) goto fail_cb;

    return 0;

fail_cb:
    free(cb);
fail_base:
    free(base);
    return 1;
}


int tcp_write_copy(uv_tcp_t *tcp, const char *base, size_t len){
    char *copy = bytesdup(base, len);
    if(!copy){
        perror("malloc");
        return 1;
    }
    return tcp_write(tcp, copy, len);
}

static dc_op_t *dctx_gather_start_ex(
    dctx_t *dctx,
    const char *series,
    size_t slen,
    char *data,
    const char *nofree,
    size_t len
){
    if(zstrnlen(series, 257) > 256){
        fprintf(stderr, "series name length must not exceed 256\n");
        return &DC_OP_NOT_OK;
    }
    if(len > UINT32_MAX){
        fprintf(stderr, "data length must not exceed 2**32\n");
        return &DC_OP_NOT_OK;
    }
    pthread_mutex_lock(&dctx->mutex);
    dc_op_t *op;

    if(dctx->rank == 0){
        // chief op may have been created when we received a message
        op = get_op_for_call_locked(dctx, DC_OP_GATHER, series, slen);
        if(!op) goto fail;

        #define OP op->u.gather.chief
        OP.recvd[0] = data;
        OP.len[0] = len;
        if(++OP.nrecvd == (size_t)dctx->size){
            mark_op_completed_locked(op);
        }
        #undef OP
    }else{
        // workers gather ops are never reused, just create a new one
        op = dc_op_new(dctx, DC_OP_GATHER, series, slen);
        if(!op) goto fail;
        link_list_append(&dctx->a.inflight, &op->link);

        #define OP op->u.gather.worker
        OP.data = data;
        OP.nofree = nofree;
        OP.len = len;
        // trigger some work in the loop
        uv_async_send(&dctx->async);
        #undef OP
    }

    pthread_mutex_unlock(&dctx->mutex);
    return op;

fail:
    pthread_mutex_unlock(&dctx->mutex);
    free(data);
    return &DC_OP_NOT_OK;
}

dc_op_t *dctx_gather_start(
    dctx_t *dctx, const char *series, size_t slen, char *data, size_t len
){
    // we own data
    return dctx_gather_start_ex(dctx, series, slen, data, NULL, len);
}

dc_op_t *dctx_gather_start_copy(
    dctx_t *dctx, const char *series, size_t slen, const char *data, size_t len
){
    char *copy = bytesdup(data, len);
    if(!copy){
        perror("malloc");
        return &DC_OP_NOT_OK;
    }
    // we own copy
    return dctx_gather_start_ex(dctx, series, slen, copy, NULL, len);
}

dc_op_t *dctx_gather_start_nofree(
    dctx_t *dctx, const char *series, size_t slen, const char *data, size_t len
){
    if(dctx->rank == 0){
        // chief always makes a copy of data
        return dctx_gather_start_copy(dctx, series, slen, data, len);
    }else{
        char *_data = NULL;
        const char *_nofree = data;
        // worker will cause dc_op_await to block until data is not needed
        return dctx_gather_start_ex(dctx, series, slen, _data, _nofree, len);
    }
}
