#include <stdlib.h>
#include <string.h>

#include "internal.h"

size_t marshal_init(char *buf, int rank){
    buf[0] = 'i';
    buf[1] = (char)(0xFF & (rank >> 3));
    buf[2] = (char)(0xFF & (rank >> 2));
    buf[3] = (char)(0xFF & (rank >> 1));
    buf[4] = (char)(0xFF & (rank >> 0));
    return 5;
}

static size_t marshal_b_or_g(
    char type, char *buf, const char *series, size_t slen, size_t body_len
){
    if(slen > 256){
        BUG("series length too long\n");
        exit(1);
    }
    if(body_len > UINT32_MAX){
        BUG("body_len too long\n");
        exit(1);
    }
    buf[0] = type;
    buf[1] = (char)(0xFF & slen);
    memcpy(&buf[2], series, slen);
    buf[slen+2] = (char)(0xFF & (body_len >> 3));
    buf[slen+3] = (char)(0xFF & (body_len >> 2));
    buf[slen+4] = (char)(0xFF & (body_len >> 1));
    buf[slen+5] = (char)(0xFF & (body_len >> 0));
    return slen + 6;
}

size_t marshal_gather(
    char *buf, const char *series, size_t slen, size_t body_len
){
    return marshal_b_or_g('g', buf, series, slen, body_len);
}


size_t marshal_broadcast(
    char *buf, const char *series, size_t slen, size_t body_len
){
    return marshal_b_or_g('b', buf, series, slen, body_len);
}

int unmarshal(
    dc_unmarshal_t *u,
    char *base,
    size_t len,
    void (*on_unmarshal)(dc_unmarshal_t*, void*),
    void *arg
){
    int retval = 0;
    size_t nread = 0;
    size_t nskip = 0;

    // type-pun base
    unsigned char *ubase = (unsigned char*)base;

    // "check length"
    #define CKLEN if(len == nread) goto done
    // "message position"
    #define MPOS (u->nread_before + nread - nskip)

start:
    CKLEN;

    if(!u->type){
        char c = base[nread++];
        switch(c){
            case 'i': // "i"nit
            case 'g': // "g"ather
            case 'b': // "b"roadcast
                u->type = c;
                break;

            case 'k': // "k"eepalive
                // that's it for the keepalive message, no user callback
                unmarshal_free(u);
                nskip = nread;
                goto start;

            default:
                printf(
                    "bad message, msg type = %c (%d), len = %zu\n",
                    c, (int)c, len
                );
                retval = 1;
                goto done;
        }
    }

    CKLEN;

    #define TAKE_BYTE() ((uint32_t)ubase[nread++])

    switch(u->type){
        case 'i':
            // fill in the rank arg
            // XXX: triple-check for int bitshift rounding errors
            // XXX: why even allow signed rank?
            if(MPOS == 1){ u->rank |= TAKE_BYTE() << 3; CKLEN; }
            if(MPOS == 2){ u->rank |= TAKE_BYTE() << 2; CKLEN; }
            if(MPOS == 3){ u->rank |= TAKE_BYTE() << 1; CKLEN; }
            if(MPOS == 4){ u->rank |= TAKE_BYTE() << 0; }
            // complete message
            on_unmarshal(u, arg);
            unmarshal_free(u);
            nskip = nread;
            goto start;

        case 'g':
        case 'b':
            // fill in the slen
            if(MPOS == 1){ u->slen = TAKE_BYTE(); CKLEN; }

            // read the series
            if(MPOS < u->slen + 2){
                size_t s_pos = MPOS - 2;
                size_t want = u->slen - s_pos;
                size_t have = len - nread;
                if(want <= have){
                    // copy the whole series
                    memcpy(u->series + s_pos, base + nread, want);
                    nread += want;
                    CKLEN;
                }else{
                    // copy remainder of buf
                    memcpy(u->series + s_pos, base + nread, have);
                    nread += have;
                    goto done;
                }
            }

            // fill in the len
            if(MPOS == u->slen+2){ u->len |= TAKE_BYTE() << 3; CKLEN; }
            if(MPOS == u->slen+3){ u->len |= TAKE_BYTE() << 2; CKLEN; }
            if(MPOS == u->slen+4){ u->len |= TAKE_BYTE() << 1; CKLEN; }
            if(MPOS == u->slen+5){ u->len |= TAKE_BYTE() << 0; CKLEN; }

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
            size_t body_pos = MPOS - u->slen - 6;
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

        default:
            BUG("type was already checked, but was not valid");
            exit(7);
    }

done:
    free(base);
    u->nread_before += len - nskip;
    return retval;
}

void unmarshal_free(dc_unmarshal_t *u){
    if(u->body) free(u->body);
    *u = (dc_unmarshal_t){0};
}
