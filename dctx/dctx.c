#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

struct dc_result NOT_OK = { .ok = false };
struct dc_result OK_EMPTY = { .ok = true };

void dc_result_free(struct dc_result **rptr) {
    struct dc_result *r = *rptr;
    dc_result_free2(r);
    *rptr = NULL;
}

void dc_result_free2(struct dc_result *r) {
    if(r == NULL || r == &NOT_OK || r == &OK_EMPTY) return;
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

bool dc_result_ok(struct dc_result *r){
    return r->ok;
}

size_t dc_result_count(struct dc_result *r){
    return r->ndata;
}

char *dc_result_take(struct dc_result *r, size_t i, size_t *len){
    char *out = r->data[i];
    size_t lout = r->len[i];
    r->data[i] = NULL;
    r->len[i] = 0;
    if(len) *len = lout;
    // printf("dc_result_take returning out[0] = %d\n", (int)out[0]);
    return out;
}


struct dc_result *dc_result_new(size_t ndata){
    struct dc_result *out = malloc(sizeof(*out));
    if(!out) return NULL;
    *out = (struct dc_result){ .ok = true, .ndata = ndata };

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

void dc_result_set(struct dc_result *r, size_t i, char *data, size_t len){
    r->data[i] = data;
    r->len[i] = len;
}

static void *dctx_thread(void *arg){
    struct dctx *dctx = arg;

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

void advance_state(struct dctx *dctx){
    // a.close: async shuts down loop from within
    if(dctx->a.close){
        close_everything(dctx);
        // do nothing else
        return;
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

        pthread_mutex_lock(&dctx->mutex);
        dctx->status = DCTX_RUNNING;
        pthread_cond_broadcast(&dctx->cond);
        pthread_mutex_unlock(&dctx->mutex);
    }

    // don't allow any writes while we are waiting for peers to connect still
    if(!dctx->a.ready){
        if(dctx->rank == 0){
            // chief checks all peers are connected
            if(dctx->server.npeers + 1 < (size_t)dctx->size) return;
        }else{
            // worker checks if it has connected to chief
            if(!dctx->client.connected) return;
        }

        pthread_mutex_lock(&dctx->mutex);
        dctx->a.ready = true;
        pthread_cond_broadcast(&dctx->cond);
        pthread_mutex_unlock(&dctx->mutex);
    }

    if(dctx->a.op_type == DC_OP_GATHER && !dctx->a.op_done){
        // gather
        if(dctx->rank == 0){
            // chief
            if((int)dctx->server.msgs_recvd == dctx->size - 1){
                rprintf("done receiving\n");
                // done receiving
                server_enable_reads(dctx);

                pthread_mutex_lock(&dctx->mutex);
                dctx->a.op_done = true;
                pthread_cond_broadcast(&dctx->cond);
                pthread_mutex_unlock(&dctx->mutex);
            }
        }else{
            // worker
            pthread_mutex_lock(&dctx->mutex);
            char *data = dctx->a.op.gather_worker.data;
            size_t len = dctx->a.op.gather_worker.len;
            dctx->a.op.gather_worker.data = NULL;
            pthread_mutex_unlock(&dctx->mutex);

            // write header
            char hdr[5] = {0};
            hdr[0] = 'm';
            hdr[1] = (char)(0xFF & ((int)len >> 3));
            hdr[2] = (char)(0xFF & ((int)len >> 2));
            hdr[3] = (char)(0xFF & ((int)len >> 1));
            hdr[4] = (char)(0xFF & ((int)len >> 0));

            int ret = tcp_write_copy(&dctx->tcp, hdr, 5);
            if(ret) goto fail;

            // write body
            ret = tcp_write(&dctx->tcp, data, len);
            if(ret) goto fail;

            // done
            pthread_mutex_lock(&dctx->mutex);
            dctx->a.op_done = true;
            pthread_cond_broadcast(&dctx->cond);
            pthread_mutex_unlock(&dctx->mutex);
        }
    }

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}


static void async_cb(uv_async_t *handle){
    struct dctx *dctx = handle->loop->data;
    advance_state(dctx);
}

struct dctx *dctx_open2(
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_host,
    const char *chief_svc
){
    struct dctx *out;
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
    struct dctx **dctx_out,
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_host,
    const char *chief_svc
){

    struct dctx *dctx = malloc(sizeof(*dctx));
    if(!dctx) return 1;
    *dctx = (struct dctx){
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
        dctx->server.buf = malloc((size_t)size*sizeof(*dctx->server.buf));
        if(!dctx->server.buf){
            perror("malloc"); // TODO
            return 1;
        }
        for(int i = 0; i < size; i++){
            dctx->server.buf[i] = NULL;
        }
        dctx->server.len = malloc((size_t)size*sizeof(*dctx->server.len));
        if(!dctx->server.len){
            perror("malloc"); // TODO
            return 1;
        }
        for(int i = 0; i < size; i++){
            dctx->server.len[i] = 0;
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


void dctx_close(struct dctx **dctxptr){
    struct dctx *dctx = *dctxptr;
    if(!dctx) return;
    dctx_close2(dctx);
    *dctxptr = NULL;
}

void dctx_close2(struct dctx *dctx){
    rprintf("dctx_close!\n");

    // ask the loop to shutdown
    dctx->a.close = true;
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

    if(dctx->rank == 0){
        // chief
        free(dctx->server.peers);
        for(int i = 0; i < dctx->rank; i++){
            if(dctx->server.buf[i]) free(dctx->server.buf[i]);
        }
        free(dctx->server.len);
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

void noop_handle_closer(uv_handle_t *handle){
    (void)handle;
};

void close_everything(struct dctx *dctx){
    // close the async
    uv_close((uv_handle_t*)&dctx->async, noop_handle_closer);
    // close the main tcp
    if(dctx->tcp_open){
        uv_close((uv_handle_t*)&dctx->tcp, noop_handle_closer);
        dctx->tcp_open = false;
    }
    if(dctx->rank == 0){
        // chief closes preinit tcps
        struct dc_conn *ptr = dctx->server.preinit;
        while(ptr) ptr = dc_conn_close(ptr);
        // chief closes peer tcps
        for(size_t i = 0; i < (size_t)dctx->size; i++){
            dctx->server.peers[i] = dc_conn_close(dctx->server.peers[i]);
        }
    }
}


void uv_perror(const char *msg, int ret){
    fprintf(stderr, "%s: %s\n", msg, uv_strerror(ret));
}

void allocator(uv_handle_t *handle, size_t suggest, uv_buf_t *buf){
    struct dctx *dctx = handle->loop->data;
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
    struct dctx *dctx = stream->loop->data;
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

int unmarshal(
    struct dc_unmarshal *u,
    char *base,
    size_t len,
    void (*on_unmarshal)(struct dc_unmarshal*, void*),
    void *arg
){
    int retval = 0;
    size_t nread = 0;
    size_t nskip = 0;

    #define CHECK_LENGTH if(len == nread) goto done
    #define MSG_POS (u->nread_before + nread - nskip)

start:
    CHECK_LENGTH;

    if(!u->type){
        char c = base[nread++];
        switch(c){
            case 'i': // "i"nit
            case 'm': // "m"essage
                u->type = c;
                break;

            case 'k': // "k"eepalive
                // that's it for the keepalive message, no user callback
                unmarshal_free(u);
                nskip = nread;
                goto start;

            default:
                printf("bad message, msg type = %c (%d), len = %zu\n", c, (int)c, len);
                retval = 1;
                goto done;
        }
    }

    CHECK_LENGTH;

    if(u->type == 'i'){
        // fill in the rank arg
        // XXX: triple-check for int bitshift rounding errors
        if(MSG_POS == 1){ u->rank |= base[nread++] << 3; CHECK_LENGTH; }
        if(MSG_POS == 2){ u->rank |= base[nread++] << 2; CHECK_LENGTH; }
        if(MSG_POS == 3){ u->rank |= base[nread++] << 1; CHECK_LENGTH; }
        if(MSG_POS == 4){ u->rank |= base[nread++] << 0; }
        // complete message
        on_unmarshal(u, arg);
        unmarshal_free(u);
        nskip = nread;
        goto start;
    }else if(u->type == 'm'){
        // fill in the len arg
        if(MSG_POS == 1){ u->len |= (uint32_t)(base[nread++] << 3); CHECK_LENGTH; }
        if(MSG_POS == 2){ u->len |= (uint32_t)(base[nread++] << 2); CHECK_LENGTH; }
        if(MSG_POS == 3){ u->len |= (uint32_t)(base[nread++] << 1); CHECK_LENGTH; }
        if(MSG_POS == 4){ u->len |= (uint32_t)(base[nread++] << 0); CHECK_LENGTH; }
        // allocate space for this body
        if(u->body == NULL){
            u->body = malloc(u->len);
            if(!u->body){
                char errmsg[32];
                snprintf(errmsg, sizeof(errmsg), "malloc(%u)\n", u->len);
                perror(errmsg);
                retval = 1;
                goto done;
            }
        }
        size_t body_pos = MSG_POS - 5;
        size_t want = u->len - body_pos;
        size_t have = len - nread;
        if(want <= have){
            memcpy(u->body + body_pos, base + nread, want);
            nread += want;
            // complete message
            on_unmarshal(u, arg);
            unmarshal_free(u);
            nskip = nread;
            goto start;
        }else{
            // copy remainder of buf
            memcpy(u->body + body_pos, base + nread, have);
            nread += have;
            goto done;
        }
    }

done:
    free(base);
    u->nread_before += len - nskip;
    return retval;
}

void unmarshal_free(struct dc_unmarshal *u){
    if(u->body) free(u->body);
    *u = (struct dc_unmarshal){0};
}

static void write_cb(uv_write_t *req, int status){
    struct dctx *dctx = req->handle->loop->data;
    char *base = req->data;

    if(base) free(base);

    if(status < 0){
        uv_perror("write_cb", status);
        dctx->on_broken_connection(dctx, req->handle);
        goto fail;
    }

    // nothing to do on success
    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

// owns base
int tcp_write(uv_tcp_t *tcp, char *base, size_t len){
    uv_write_t *req = malloc(sizeof(*req));
    if(!req){
        perror("malloc");
        goto fail_base;
    }
    req->data = base;

    uv_buf_t buf = { .base = base, .len = len };

    int ret = uv_write(req, (uv_stream_t*)tcp, &buf, 1, write_cb);
    if(ret < 0){
        uv_perror("uv_write", ret);
        goto fail_req;
    }

    return 0;

fail_req:
    free(req);
fail_base:
    free(base);
    return 1;
}

int tcp_write_copy(uv_tcp_t *tcp, const char *base, size_t len){
    char *copy = malloc(len);
    if(!copy){
        perror("malloc");
        return 1;
    }
    memcpy(copy, base, len);
    return tcp_write(tcp, copy, len);
}

int tcp_write_nofree(uv_tcp_t *tcp, char *base, size_t len){
    uv_write_t *req = malloc(sizeof(*req));
    if(!req){
        perror("malloc");
        goto fail;
    }
    req->data = NULL;

    uv_buf_t buf = { .base = base, .len = len };

    int ret = uv_write(req, (uv_stream_t*)tcp, &buf, 1, write_cb);
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

int dctx_gather_start(struct dctx *dctx, char *data, size_t len){
    char *newdata = malloc(len);
    memcpy(newdata, data, len);
    data = newdata;

    // printf("dctx_gather_start(");
    // for(size_t i = 0; i<len; i++){
    //     printf("%s%.2x", i?" ":"", (int)data[i]);
    // }
    // printf(")\n");

    rprintf("locking mutex\n");
    pthread_mutex_lock(&dctx->mutex);
    rprintf("locked mutex\n");
    if(dctx->a.op_type != DC_OP_NONE){
        fprintf(stderr, "other op in progress\n");
        goto fail;
    }
    dctx->a.op_type = DC_OP_GATHER;

    if(dctx->rank == 0){
        // chief
        dctx->a.op.gather_chief.data = data;
        dctx->a.op.gather_chief.len = len;
    }else{
        // worker
        dctx->a.op.gather_worker.data = data;
        dctx->a.op.gather_worker.len = len;
        data = NULL;
    }
    rprintf("async send\n");
    uv_async_send(&dctx->async);
    rprintf("async sent\n");

    rprintf("unlocking mutex\n");
    pthread_mutex_unlock(&dctx->mutex);
    rprintf("unlocked mutex\n");
    return 0;

fail:
    pthread_mutex_unlock(&dctx->mutex);
    free(data);
    return 1;
}

struct dc_result *dctx_gather_end(struct dctx *dctx){
    struct dc_result *result = NULL;

    pthread_mutex_lock(&dctx->mutex);

    // wait for the op to finish
    while(dctx->status == DCTX_RUNNING && !dctx->a.op_done)
        pthread_cond_wait(&dctx->cond, &dctx->mutex);

    // check if the op succeeded
    if(!dctx->a.op_done){
        // TODO: figure out what failed
        printf("dctx crashed\n");
        goto reset;
    }

    if(dctx->rank == 0){
        // chief
        result = dc_result_new((size_t)dctx->size);
        if(!result) goto reset;
        // chief data
        char *data = dctx->a.op.gather_chief.data;
        size_t len = dctx->a.op.gather_chief.len;
        dctx->a.op.gather_chief.data = NULL;
        dc_result_set(result, 0, data, len);
        // worker data
        for(int i = 1; i < dctx->size; i++){
            dc_result_set(
                result,
                (size_t)i,
                dctx->server.buf[i],
                (size_t)dctx->server.len[i]
            );
            dctx->server.buf[i] = NULL;
        }
    }else{
        // worker
        result = &OK_EMPTY;
    }

reset:
    // reset
    dctx->a.op_done = false;
    dctx->a.op_type = DC_OP_NONE;
    if(dctx->rank == 0){
        // chief
        if(dctx->a.op.gather_chief.data) free(dctx->a.op.gather_chief.data);
        dctx->a.op.gather_chief.data = NULL;
    }else{
        // worker
        if(dctx->a.op.gather_worker.data) free(dctx->a.op.gather_worker.data);
        dctx->a.op.gather_worker.data = NULL;
    }
    dctx->a.op = (union dc_op){};
    pthread_mutex_unlock(&dctx->mutex);
    return result ? result : &NOT_OK;
}

struct dc_result *dctx_gather(struct dctx *dctx, char *data, size_t len){
    int ret = dctx_gather_start(dctx, data, len);
    if(ret){
        return &NOT_OK;
    }
    return dctx_gather_end(dctx);
}
