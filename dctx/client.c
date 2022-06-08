#include <stdlib.h>

#include "internal.h"

int start_gai(dctx_t *dctx){
    struct addrinfo hints = {0};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    // hints.ai_flags = AI_ADDRCONFIG;

    int ret = uv_getaddrinfo(
        &dctx->loop,
        &dctx->client.gai_req,
        gai_cb,
        dctx->host,
        dctx->svc,
        &hints
    );
    // how does this even happen?
    if(ret < 0){
        uv_perror("uv_getaddrinfo", ret);
        return 1;
    }

    dctx->client.gai_req.data = dctx;

    return 0;
}

void gai_cb(uv_getaddrinfo_t *req, int status, struct addrinfo *res){
    dctx_t *dctx = req->data;
    if(dctx->closed){
        if(res) uv_freeaddrinfo(res);
        return;
    }

    if(status < 0){
        // dns failure
        int ret = retry_later(dctx);
        if(ret) goto fail;
        return;
    }

    dctx->client.gai = res;
    dctx->client.ptr = res;

    int ret = conn_next(dctx);
    if(ret) goto fail;
    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

int conn_next(dctx_t *dctx){
    if(dctx->client.ptr == NULL){
        // out of options
        uv_freeaddrinfo(dctx->client.gai);
        dctx->client.gai = NULL;

        return retry_later(dctx);
    }

    int ret = uv_tcp_connect(
        &dctx->client.conn_req, &dctx->tcp, dctx->client.ptr->ai_addr, conn_cb
    );
    if(ret < 0){
        uv_perror("uv_tcp_connect", ret);
        return ret;
    }
    dctx->client.conn_req.data = dctx;

    // advance the ptr
    dctx->client.ptr = dctx->client.ptr->ai_next;

    return 0;
}

void conn_cb(uv_connect_t *req, int status){
    dctx_t *dctx = req->data;
    if(dctx->closed) return;

    if(status < 0){
        // failure
        uv_perror("uv_tcp_connect(callback)", status);

        // failed to connect, close tcp and try the next one
        close_for_retry(dctx);
        return;
    }

    // rprintf("connection made!\n");
    uv_freeaddrinfo(dctx->client.gai);
    dctx->client.gai = NULL;

    // send our rank as our first message
    char buf[INIT_MSG_SIZE] = {0};
    size_t buflen = marshal_init(buf, dctx->rank);
    int ret = tcp_write_copy(&dctx->tcp, buf, buflen);
    if(ret) goto fail;

    // now we should be promoted to being a peer
    dctx->client.connected = true;
    advance_state(dctx);

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

void close_for_retry(dctx_t *dctx){
    uv_close((uv_handle_t*)&dctx->tcp, close_for_retry_cb);
    dctx->tcp_open = false;
}

void close_for_retry_cb(uv_handle_t *handle){
    dctx_t *dctx = handle->loop->data;
    if(dctx->closed) return;

    int ret = uv_tcp_init(&dctx->loop, &dctx->tcp);
    if(ret < 0){
        uv_perror("uv_tcp_init", ret);
        goto fail;
    }
    dctx->tcp_open = true;
    ret = conn_next(dctx);
    if(ret) goto fail;

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

int retry_later(dctx_t *dctx){
    int ret = uv_timer_start(&dctx->client.timer, retry_cb, 1000, 0);
    if(ret < 0){
        uv_perror("uv_timer_start", ret);
        return 1;
    }
    return 0;
}

void retry_cb(uv_timer_t *handle){
    dctx_t *dctx = handle->loop->data;
    if(dctx->closed) return;

    int ret = start_gai(dctx);
    if(ret){
        dctx->failed = true;
        close_everything(dctx);
    }
}

static void on_broken_connection(dctx_t *dctx, uv_stream_t *stream){
    (void)stream;
    // our main connection died, just crash
    close_everything(dctx);
}

static void on_read(
    dctx_t *dctx, uv_stream_t *stream, char *buf, size_t len
){
    (void)dctx;
    (void)stream;
    (void)len;
    free(buf);
}

int init_client(dctx_t *dctx){
    int ret = uv_tcp_init(&dctx->loop, &dctx->tcp);
    if(ret < 0){
        uv_perror("uv_tcp_init", ret);
        return 1;
    }
    dctx->tcp_open = true;

    ret = uv_timer_init(&dctx->loop, &dctx->client.timer);
    if(ret < 0){
        uv_perror("uv_timer_init", ret);  // TODO
        return 1;
    }

    // worker, start connection process`
    ret = start_gai(dctx);
    if(ret) return 1;

    // client-side hooks
    dctx->on_broken_connection = on_broken_connection;
    dctx->on_read = on_read;

    return 0;
}
