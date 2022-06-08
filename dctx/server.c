#include <stdlib.h>

#include "internal.h"

dc_conn_t *dc_conn_new(void){
    dc_conn_t *conn = malloc(sizeof(*conn));
    if(conn){
        *conn = (dc_conn_t){.rank = -1};
        conn->tcp.data = conn;
    }
    return conn;
}

static void conn_close_cb(uv_handle_t *handle){
    dc_conn_t *conn = handle->data;
    unmarshal_free(&conn->unmarshal);
    free(conn);
}

void dc_conn_close(dc_conn_t *conn){
    if(!conn) return;
    // remove from the dctx so it cannot be closed twice
    if(conn->rank < 0){
        link_remove(&conn->link);
    }else{
        dctx_t *dctx = conn->tcp.loop->data;
        dctx->server.peers[conn->rank] = NULL;
    }

    // start the close process
    uv_close((uv_handle_t*)&conn->tcp, conn_close_cb);
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
    dctx_t *dctx = srv->loop->data;
    if(status < 0){
        uv_perror("uv_listen(cb)", status);
        goto fail;
    }

    dc_conn_t *conn = malloc(sizeof(*conn));
    if(!conn) goto fail;
    *conn = (dc_conn_t){.rank = -1};
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
    link_list_append(&dctx->server.preinit, &conn->link);

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

static void on_broken_connection(dctx_t *dctx, uv_stream_t *stream){
    dc_conn_t *conn = stream->data;

    // go to STOPPING state
    if(dctx->status != DCTX_STOPPING){
        pthread_mutex_lock(&dctx->mutex);
        dctx->status = DCTX_STOPPING;
        pthread_cond_broadcast(&dctx->cond);
        pthread_mutex_unlock(&dctx->mutex);
    }

    if(conn->rank > -1){
        // conn has known rank
        dctx->server.peers[conn->rank] = NULL;
    }
    dc_conn_close(conn);
}

typedef struct {
    dctx_t *dctx;
    dc_conn_t *conn;
} unmarshal_data_t;

static void on_unmarshal(dc_unmarshal_t *u, void *arg){
    unmarshal_data_t *data = arg;
    dctx_t *dctx = data->dctx;
    dc_conn_t *conn = data->conn;

    if(conn->rank == -1){
        // preinit connection, only "i"int
        if(u->type != 'i'){
            rprintf("got non-init message from preinit connection\n");
            goto fail;
        }
        int i = (int)u->rank;
        if(i < 0 || i > dctx->size){
            rprintf("got invalid rank in init message: %d\n", i);
            goto fail;
        }
        if(dctx->server.peers[i] != NULL){
            rprintf("got duplicate rank in init message: %d\n", i);
            goto fail;
        }
        // transition from preinit to a ranked peer
        link_remove(&conn->link);
        // store conn as a ranked peer instead
        dctx->server.peers[i] = conn;
        dctx->server.npeers++;
        conn->rank = i;
        // rprintf("promoted peer=%d\n", i);
        advance_state(dctx);
        return;
    }

    // non-preinit: store message for rank and stop reading
    // rprintf("read: %.*s\n", (int)u->len, u->body);

    // find the op or create a new one
    dc_op_type_e type = DC_OP_GATHER; // XXX: choose the op correctly
    int rank = conn->rank;
    dc_op_t *op = get_op_for_recv(dctx, type, u->series, u->slen, rank);
    if(!op) goto fail;

    // take ownership of the buffer
    char *body = u->body;
    size_t len = u->len;
    u->body = NULL;

    switch(op->type){
        case DC_OP_GATHER:
            #define OP op->u.gather.chief
            OP.recvd[rank] = body;
            OP.len[rank] = len;
            if(++OP.nrecvd == (size_t)dctx->size){
                mark_op_completed_and_notify(op);
            }
            #undef OP
            break;
    }

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

static void on_read(
    dctx_t *dctx, uv_stream_t *stream, char *buf, size_t len
){
    (void)dctx;
    dc_conn_t *conn = stream->data;

    unmarshal_data_t data = {dctx, conn};
    int ret = unmarshal(&conn->unmarshal, buf, len, on_unmarshal, &data);
    if(ret) goto fail;

    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

int init_server(dctx_t *dctx){
    int ret = uv_tcp_init(&dctx->loop, &dctx->tcp);
    if(ret < 0){
        uv_perror("uv_tcp_init", ret);
        return 1;
    }
    dctx->tcp_open = true;

    // chief, bind and listen
    ret = bind_via_gai(&dctx->tcp, dctx->host, dctx->svc);
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
