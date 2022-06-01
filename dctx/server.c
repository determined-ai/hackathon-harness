#include <stdlib.h>

#include "internal.h"

struct dc_conn *dc_conn_new(void){
    struct dc_conn *conn = malloc(sizeof(*conn));
    if(conn){
        *conn = (struct dc_conn){.rank = -1};
        conn->tcp.data = conn;
    }
    return conn;
}

static void conn_close_cb(uv_handle_t *handle){
    struct dc_conn *conn = handle->data;
    unmarshal_free(&conn->unmarshal);
    free(conn);
}

struct dc_conn *dc_conn_close(struct dc_conn *conn){
    if(!conn) return NULL;
    struct dc_conn *out = NULL;
    if(conn->next != conn){
        out = conn->next;
    }
    // remove from the dctx so it cannot be closed twice
    conn->prev->next = conn->next;
    conn->next->prev = conn->prev;

    // start the close process
    uv_close((uv_handle_t*)&conn->tcp, conn_close_cb);

    return out;
}

int bind_via_gai(uv_tcp_t *srv, const char *addr, const char *svc){
    // prepare for getaddrinfo
    struct addrinfo hints = {0};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE /*| AI_ADDRCONFIG*/;

    // get address of host
    struct addrinfo *ai;
    int ret = getaddrinfo(addr, svc, &hints, &ai);
    if(ret != 0){
        fprintf(
            stderr, "getaddrinfo(%s:%s): %s\n", addr, svc, gai_strerror(ret)
        );
        return 1;
    }

    // bind to something
    struct addrinfo *p;
    for(p = ai; p != NULL; p = p->ai_next){
        // printf("binding to %x\n", FNTOP(p->ai_addr));

        ret = uv_tcp_bind(srv, p->ai_addr, 0);
        if(ret < 0){
            uv_perror("uv_tcp_bind", ret);
            continue;
        }
        break;
    }
    int retval = 0;
    if(p == NULL){
        // failed to bind to anything
        fprintf(stderr, "failed to bind to anything\n"); // TODO
        retval = 1;
    }

    freeaddrinfo(ai);

    return retval;
}


static void listener_cb(uv_stream_t *srv, int status){
    struct dctx *dctx = srv->loop->data;
    if(status < 0){
        uv_perror("uv_listen(cb)", status);
        goto fail;
    }

    struct dc_conn *conn = malloc(sizeof(*conn));
    if(!conn) goto fail;
    *conn = (struct dc_conn){.rank = -1};
    conn->tcp.data = conn;

    int ret = uv_tcp_init(&dctx->loop, &conn->tcp);
    if(ret < 0){
        uv_perror("uv_tcp_init", ret);
        free(conn);
        goto fail;
    }

    ret = uv_accept((uv_stream_t*)&dctx->tcp, (uv_stream_t*)&conn->tcp);
    if(ret < 0){
        uv_perror("uv_accept", ret);
        dc_conn_close(conn);
        goto fail;
    }

    rprintf("accepted a connection!\n");

    // remember this connection, as a preinit
    if(dctx->server.preinit == NULL){
        dctx->server.preinit = conn;
        conn->prev = conn;
        conn->next = conn;
    }else{
        struct dc_conn *first = dctx->server.preinit;
        struct dc_conn *last = dctx->server.preinit->prev;
        first->prev = conn;
        last->next = conn;
        conn->prev = last;
        conn->next = first;
    }

    // start reading from this connection
    ret = uv_read_start((uv_stream_t*)&conn->tcp, allocator, read_cb);
    if(ret < 0){
        uv_perror("uv_read_start", ret);
        goto fail;
    }

    return;

fail:
    printf("listener cb failed\n");
    dctx->failed = true;
    close_everything(dctx);
}

static void on_broken_connection(struct dctx *dctx, uv_stream_t *stream){
    struct dc_conn *conn = stream->data;

    // go to STOPPING state
    if(dctx->status != DCTX_STOPPING){
        pthread_mutex_lock(&dctx->mutex);
        dctx->status = DCTX_STOPPING;
        pthread_cond_broadcast(&dctx->cond);
        pthread_mutex_unlock(&dctx->mutex);
    }

    if(conn->rank == -1){
        // conn is preinit
        dc_conn_close(conn);
    }else{
        // conn has known rank
        dctx->server.peers[conn->rank] = dc_conn_close(conn);
    }
}

struct unmarshal_data {
    struct dctx *dctx;
    struct dc_conn *conn;
};

static void on_unmarshal(struct dc_unmarshal *unmarshal, void *arg){
    struct unmarshal_data *data = arg;
    struct dctx *dctx = data->dctx;
    struct dc_conn *conn = data->conn;

    if(conn->rank == -1){
        // preinit connection, only "i"int
        if(u->type != 'i'){
            rprintf("got non-init message from preinit connection\n");
            goto fail;
        }
        int i = dctx->rank;
        if(i < 0 || i > dctx->size){
            rprintf("got invalid rank in init message: %d\n", i);
            goto fail;
        }
        if(dctx->server.peers[i] != NULL){
            rprintf("got duplicate rank in init message: %d\n", i);
            goto fail;
        }
        // transition from preinit to
        if(conn->next == conn){
            // last preinit container
        }else{
            struct dc_conn *first = dctx->server.preinit;
            struct dc_conn *last = dctx->server.preinit->prev;
            first->prev = conn;
            last->next = conn;
            conn->prev = last;
            conn->next = first;
        }
        dc->conn

    }
    // non-preinit: only "m"essages

    (void)unmarshal;
    (void)dctx;
    (void)conn;

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

static void on_read(
    struct dctx *dctx, uv_stream_t *stream, const uv_buf_t *buf
){
    (void)dctx;
    struct dc_conn *conn = stream->data;

    struct unmarshal_data data = {dctx, conn};
    int ret = unmarshal(&conn->unmarshal, buf, on_unmarshal, &data);
    if(ret) goto fail;

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

int init_server(struct dctx *dctx){
    int ret = uv_tcp_init(&dctx->loop, &dctx->tcp);
    if(ret < 0){
        uv_perror("uv_tcp_init", ret);
        return 1;
    }

    // chief, bind and listen
    ret = bind_via_gai(&dctx->tcp, "localhost", "1234");
    if(ret < 0){
        return 1;
    }

    ret = uv_listen((uv_stream_t*)&dctx->tcp, 5, listener_cb);
    if(ret < 0){
        uv_perror("uv_listen", ret);
        return 1;
    }

    // server-side hooks
    dctx->on_broken_connection = on_broken_connection;
    dctx->on_read = on_read;

    return 0;
}
