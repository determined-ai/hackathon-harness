#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

dc_op_t DC_OP_NOT_OK = { .ok = false };

dc_op_t *dc_op_new(dctx_t *dctx, dc_op_type_e type, const char *series, size_t slen){
    if(slen > 256){
        rprintf("series length must not exceed 256!\n");
        return NULL;
    }
    dc_op_t *op = malloc(sizeof(*op));
    if(!op){
        perror("malloc");
        return NULL;
    }
    *op = (dc_op_t){
        .type = type,
        .slen = slen,
        .dctx = dctx,
        .ok = true,
    };
    memcpy(op->series, series, slen);

    switch(type){
        case DC_OP_GATHER:
            if(dctx->rank == 0){
                #define OP op->u.gather.chief
                // allocate and zeroize OP.recvd
                size_t n = (size_t)dctx->size * sizeof(*OP.recvd);
                OP.recvd = malloc(n);
                if(!OP.recvd) goto fail;
                memset(OP.recvd, 0, n);
                // allocate and zeroize OP.len
                n = (size_t)dctx->size * sizeof(*OP.len);
                OP.len = malloc(n);
                if(!OP.len) goto fail;
                memset(OP.len, 0, n);
                #undef OP
            }else{
                // worker gather: nothing to allocate
            }
            break;

        case DC_OP_BROADCAST:
            // nothing to allocate
            break;
    }

    return op;

fail:
    dc_op_free(op);
    return NULL;
}

// the caller must have removed from the linked list in a thread-safe way
void dc_op_free(dc_op_t *op){
    dctx_t *dctx = op->dctx;
    switch(op->type){
        case DC_OP_GATHER:
            if(dctx->rank == 0){
                #define OP op->u.gather.chief
                if(OP.recvd){
                    for(size_t i = 0; i < (size_t)dctx->size; i++){
                        if(OP.recvd[i]) free(OP.recvd[i]);
                    }
                    free(OP.recvd);
                }
                if(OP.len) free(OP.len);
                #undef OP
            }else{
                #define OP op->u.gather.worker
                if(OP.data) free(OP.data);
                #undef OP
            }
            break;

        case DC_OP_BROADCAST:
            if(dctx->rank == 0){
                #define OP op->u.broadcast.chief
                if(OP.data) free(OP.data);
                #undef OP
            }else{
                #define OP op->u.broadcast.worker
                if(OP.recvd) free(OP.recvd);
                #undef OP
            }
            break;
    }
    free(op);
}

void mark_op_completed_locked(dc_op_t *op){
    dctx_t *dctx = op->dctx;
    // remove op from inflight ops
    link_remove(&op->link);
    // insert into complete ops
    link_list_append(&dctx->a.complete, &op->link);
    // mark the op ready for the user
    op->ready = true;
}


void mark_op_completed_and_notify(dc_op_t *op){
    dctx_t *dctx = op->dctx;
    pthread_mutex_lock(&dctx->mutex);
    mark_op_completed_locked(op);
    pthread_cond_broadcast(&dctx->cond);
    pthread_mutex_unlock(&dctx->mutex);
}

void dc_op_write_cb(dc_op_t *op){
    dctx_t *dctx = op->dctx;
    switch(op->type){
        case DC_OP_GATHER:
            if(dctx->rank == 0){
                RBUG("chief doesn't send anything for gather");
                goto fail;
            }else{
                #define OP op->u.gather.worker
                // free OP.data if present, but don't touch OP.constdata
                if(OP.data){
                    free(OP.data);
                    OP.data = NULL;
                }
                // operation is now complete
                mark_op_completed_and_notify(op);
                #undef OP
            }
            break;

        case DC_OP_BROADCAST:
            if(dctx->rank == 0){
                #define OP op->u.broadcast.chief
                if(++OP.nsent == dctx->server.npeers){
                    // leave OP.data for dc_op_await
                    // operation is now complete
                    mark_op_completed_and_notify(op);
                }
                #undef OP
            }else{
                RBUG("worker doesn't send anything for broadcast");
                goto fail;
            }
            break;
    }
    return;

fail:
    dctx->failed = true;
    close_everything(dctx);
}

// may do work, and returns if the op is complete
bool dc_op_advance(dc_op_t *op){
    dctx_t *dctx = op->dctx;
    int ret;
    switch(op->type){
        case DC_OP_GATHER:
            if(dctx->rank == 0){
                // op only receives; never any work to do
                return false;
            }else{
                #define OP op->u.gather.worker
                // worker gather
                if(OP.sent) return false;
                OP.sent = true;

                // write header
                char hdr[GATHER_MSG_HDR_MAXSIZE];
                size_t buflen = marshal_gather(
                    hdr, op->series, op->slen, OP.len
                );
                ret = tcp_write_copy(&dctx->tcp, hdr, buflen);
                if(ret) goto fail;

                // configure our write_cb
                OP.cb = (dc_write_cb_t){
                    .type = WRITE_CB_OP,
                    .u = { .op = op },
                };

                // choose which data to send
                char *data;
                if(OP.data){
                    data = OP.data;
                }else{
                    data = i_promise_i_wont_touch(OP.nofree);
                }

                ret = tcp_write_ex(&dctx->tcp, data, OP.len, &OP.cb);
                if(ret) goto fail;
                return false;
                #undef OP
            }
            break;

        case DC_OP_BROADCAST:
            if(dctx->rank == 0){
                #define OP op->u.broadcast.chief
                if(OP.write_started) return false;
                OP.write_started = true;

                // configure our write_cb
                OP.cb = (dc_write_cb_t){
                    .type = WRITE_CB_OP,
                    .u = { .op = op },
                };

                // write to every peer
                for(size_t i = 0; i < dctx->server.npeers; i++){
                    // write header
                    dc_conn_t *conn = dctx->server.peers[i+1];
                    char hdr[BROADCAST_MSG_HDR_MAXSIZE];
                    size_t buflen = marshal_broadcast(
                        hdr, op->series, op->slen, OP.len
                    );
                    ret = tcp_write_copy(&conn->tcp, hdr, buflen);
                    if(ret) goto fail;

                    ret = tcp_write_ex(&conn->tcp, OP.data, OP.len, &OP.cb);
                    if(ret) goto fail;
                }
                return false;
                #undef OP
            }else{
                // op only receives; never any work to do
                return false;
            }
            break;
    }
    return false;

fail:
    dctx->failed = true;
    close_everything(dctx);
    return false;
}


bool dc_op_ok(dc_op_t *op){
    return op->ok;
}


dc_result_t *dc_op_await(dc_op_t *op){
    dctx_t *dctx = op->dctx;
    dc_result_t *result = NULL;

    pthread_mutex_lock(&dctx->mutex);

    // wait for the op to finish
    while(dctx->status == DCTX_RUNNING && !op->ready)
        pthread_cond_wait(&dctx->cond, &dctx->mutex);

    // remove the op from the linked list
    link_remove(&op->link);

    pthread_mutex_unlock(&dctx->mutex);

    // check if the op succeeded
    if(!op->ready){
        // TODO: figure out what failed
        rprintf("dctx crashed\n");
        goto done;
    }

    switch(op->type){
        case DC_OP_GATHER:
            if(dctx->rank == 0){
                #define OP op->u.gather.chief
                result = dc_result_new((size_t)dctx->size);
                if(!result) goto done;
                // chief data
                for(int i = 0; i < dctx->size; i++){
                    dc_result_set(
                        result, (size_t)i, OP.recvd[i], (size_t)OP.len[i]
                    );
                    OP.recvd[i] = NULL;
                }
                #undef OP
            }else{
                // worker gather
                result = &DC_RESULT_EMPTY;
            }
            break;

        case DC_OP_BROADCAST:
            if(dctx->rank == 0){
                // chief broadcast, chief returns the broadcasted data
                #define OP op->u.broadcast.chief
                result = dc_result_new(1);
                if(!result) goto done;
                dc_result_set(result, 0, OP.data, OP.len);
                OP.data = NULL;
                #undef OP
            }else{
                #define OP op->u.broadcast.worker
                // worker gather
                result = dc_result_new(1);
                if(!result) goto done;
                dc_result_set(result, 0, OP.recvd, OP.len);
                OP.recvd = NULL;
                #undef OP
            }
            break;
    }

done:
    dc_op_free(op);
    return result ? result : &DC_RESULT_NOT_OK;
}


// returns NULL on error
dc_op_t *get_op_for_recv(
    dctx_t *dctx, dc_op_type_e type, const char *series, size_t slen, int rank
){
    pthread_mutex_lock(&dctx->mutex);

    dc_op_t *out = NULL;
    dc_op_t *op, *temp;
    LINK_FOR_EACH_SAFE(op, temp, &dctx->a.inflight, dc_op_t, link){
        if(op->type != type) continue;
        if(!zstreq(op->series, series)) continue;
        switch(op->type){
            case DC_OP_GATHER:
                if(dctx->rank == 0){
                    #define OP op->u.gather.chief
                    // chief gather
                    if(OP.recvd[rank] == NULL){
                        out = op;
                        goto done;
                    }
                    #undef OP
                }else{
                    // worker gather
                    RBUG("worker received a GATHER message");
                    goto done;
                }
                break;

            case DC_OP_BROADCAST:
                if(dctx->rank == 0){
                    RBUG("chief received a BROADCAST message");
                    goto done;
                }else{
                    #define OP op->u.broadcast.worker
                    // worker broadcast
                    if(OP.recvd == NULL){
                        out = op;
                        goto done;
                    }
                    #undef OP
                }
                break;
        }
    }
    // didn't find the op, create a new one
    out = dc_op_new(dctx, type, series, slen);
    if(!out){
        perror("malloc");
        goto done;
    }
    link_list_append(&dctx->a.inflight, &out->link);

done:
    pthread_mutex_unlock(&dctx->mutex);
    return out;
}

dc_op_t *get_op_for_call_locked(
    dctx_t *dctx, dc_op_type_e type, const char *series, size_t slen
){
    dc_op_t *out = NULL;
    dc_op_t *op, *temp;
    LINK_FOR_EACH_SAFE(op, temp, &dctx->a.inflight, dc_op_t, link){
        if(op->type != type) continue;
        if(!zstreq(op->series, series)) continue;
        switch(op->type){
            case DC_OP_GATHER:
                if(dctx->rank == 0){
                    #define OP op->u.gather.chief
                    // chief gather
                    if(OP.recvd[0] == NULL){
                        // here's a gather without its chief data yet
                        out = op;
                        goto done;
                    }
                    #undef OP
                }else{
                    // worker gathers are never reused
                }
                break;

            case DC_OP_BROADCAST:
                if(dctx->rank == 0){
                    // chief broadcast is never reusued
                }else{
                    #define OP op->u.broadcast.worker
                    // worker broadcast, match the first inflight in-series op
                    out = op;
                    goto done;
                    #undef OP
                }
                break;
        }
    }

    // didn't find the op, create a new one
    out = dc_op_new(dctx, type, series, slen);
    if(!out){
        perror("malloc");
        goto done;
    }
    link_list_append(&dctx->a.inflight, &out->link);

done:
    return out;
}
