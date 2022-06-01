#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

struct dc_result NOT_OK = { .ok = false };

void dc_result_free(struct dc_result **rptr) {
    struct dc_result *r = *rptr;
    if(r == NULL) return;
    if(r->data != NULL){
        for(size_t i = 0; i < r->ndata; i++){
            char *data = r->data[i];
            if(data != NULL) free(data);
        }
        free(r->data);
    }
    if(r->len != NULL) free(r->len);
    free(r);
    *rptr = NULL;
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
    return out;
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

static void async_cb(uv_async_t *handle){
    struct dctx *dctx = handle->loop->data;

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

        rprintf("loop running\n");

        pthread_mutex_lock(&dctx->mutex);
        dctx->status = DCTX_RUNNING;
        pthread_cond_broadcast(&dctx->cond);
        pthread_mutex_unlock(&dctx->mutex);
    }

    return;

fail:
    printf("async cb failed\n");
    dctx->failed = true;
    close_everything(dctx);
}

int dctx_open(
    struct dctx **dctx_out,
    int rank,
    int size,
    int local_rank,
    int local_size,
    int cross_rank,
    int cross_size,
    const char *chief_addr,
    const size_t len
){
    // TODO
    (void)chief_addr;
    (void)len;

    struct dctx *dctx = malloc(sizeof(struct dctx));
    if(!dctx) return 1;
    *dctx = (struct dctx){
        .rank = rank,
        .size = size,
        .local_rank = local_rank,
        .local_size = local_size,
        .cross_rank = cross_rank,
        .cross_size = cross_size,
    };

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

    ret = uv_tcp_init(&dctx->loop, &dctx->tcp);
    if(ret < 0){
        uv_perror("uv_tcp_init", ret); // TODO
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
    }else{
        // client
        uv_freeaddrinfo(dctx->client.gai);
        dctx->client.gai = NULL;
    }
    unmarshal_free(&dctx->unmarshal);
    free(dctx);
    *dctxptr = NULL;
}

void noop_handle_closer(uv_handle_t *handle){
    (void)handle;
};

void close_everything(struct dctx *dctx){
    // close the async
    uv_close((uv_handle_t*)&dctx->async, noop_handle_closer);
    // close the main tcp
    uv_close((uv_handle_t*)&dctx->tcp, noop_handle_closer);
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

    dctx->on_read(dctx, stream, buf);

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

int unmarshal(
    struct dc_unmarshal *u,
    const uv_buf_t *buf,
    void (*on_unmarshal)(struct dc_unmarshal*, void*),
    void *arg
){
    int retval = 0;
    size_t nread = 0;
    size_t nskip = 0;

    #define CHECK_LENGTH if(buf->len == nread) goto done
    #define MSG_POS (u->nread_before + nread - nskip)

start:
    CHECK_LENGTH;

    if(!u->type){
        char c = buf->base[nread++];
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
                printf("bad message, msg type = %c (%d)\n", c, (int)c);
                retval = 1;
                goto done;
        }
    }

    CHECK_LENGTH;

    if(u->type == 'i'){
        // fill in the rank arg
        // XXX: triple-check for int bitshift rounding errors
        if(MSG_POS == 1){ u->rank |= buf->base[nread++] << 3; CHECK_LENGTH; }
        if(MSG_POS == 2){ u->rank |= buf->base[nread++] << 2; CHECK_LENGTH; }
        if(MSG_POS == 3){ u->rank |= buf->base[nread++] << 1; CHECK_LENGTH; }
        if(MSG_POS == 4){ u->rank |= buf->base[nread++] << 0; CHECK_LENGTH; }
        // complete message
        on_unmarshal(u, arg);
        unmarshal_free(u);
        nskip = nread;
        goto start;
    }else if(u->type == 'm'){
        // fill in the len arg
        if(MSG_POS == 1){ u->len |= (uint32_t)(buf->base[nread++] << 3); CHECK_LENGTH; }
        if(MSG_POS == 2){ u->len |= (uint32_t)(buf->base[nread++] << 2); CHECK_LENGTH; }
        if(MSG_POS == 3){ u->len |= (uint32_t)(buf->base[nread++] << 1); CHECK_LENGTH; }
        if(MSG_POS == 4){ u->len |= (uint32_t)(buf->base[nread++] << 0); CHECK_LENGTH; }
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
        size_t have = buf->len - nread;
        if(want <= have){
            memcpy(u->body + body_pos, buf->base + nread, want);
            nread += want;
            // complete message
            on_unmarshal(u, arg);
            unmarshal_free(u);
            nskip = nread;
            goto start;
        }else{
            // copy remainder of buf
            memcpy(u->body + body_pos, buf->base + nread, have);
            nread += have;
            goto done;
        }
    }

done:
    free(buf->base);
    u->nread_before += buf->len - nskip;
    return retval;
}

void unmarshal_free(struct dc_unmarshal *u){
    if(u->body) free(u->body);
    *u = (struct dc_unmarshal){0};
}

int hello(char *text) {
  printf("Hello, %s\n", text);
  return 7;
}
