#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "internal.h"

#define ASSERT(code) do{ \
    if(!(code)){ \
        printf("assertion failed (%s:%d): "#code"\n", __FILE__, __LINE__); \
        retval = 1; \
        goto done; \
    } \
}while(0)

static int test_links(void){
    // empty linked list is safe to iterate through
    link_t head = {0};
    dc_op_t *op, *temp;
    LINK_FOR_EACH_SAFE(op, temp, &head, dc_op_t, link){
        printf("empty link list did iterate!\n");
        return 1;
    }
    return 0;
}

struct unmarshal_test {
    char type[8];
    int rank[8];
    char *body[8];
    size_t len[8];
    size_t nexpect;
    size_t nchecked;
    bool fail;
};

static void on_unmarshal(dc_unmarshal_t *u, void *arg){
    struct unmarshal_test *data = arg;
    int retval = 0;

    ASSERT(data->nexpect > data->nchecked);
    ASSERT(u->type == data->type[data->nchecked]);
    if(u->type == 'i'){
        // init
        ASSERT(u->rank == data->rank[data->nchecked]);
    }else{
        // message
        ASSERT(u->len == data->len[data->nchecked]);
        ASSERT(strncmp(u->body, data->body[data->nchecked], u->len) == 0);
    }

    data->nchecked++;

done:
    if(retval) data->fail = true;
}

static uv_buf_t _mkbuf(char *buf, size_t n){
    uv_buf_t out = { .base = malloc(n), .len = n };
    if(!out.base) exit(2);
    memcpy(out.base, buf, n);
    return out;
}
#define mkbuf(buf) _mkbuf(buf, sizeof(buf)-1);

static int test_unmarshal(void){
    int retval = 0;
    dc_unmarshal_t u = {0};

    // one message in one buffer
    {
        uv_buf_t buf = mkbuf("m" "\x00\x00\x00\x04" "abcd");

        struct unmarshal_test data = {
            .type = {'m'},
            .body = {"abcd"},
            .len = {4},
            .nexpect = 1,
        };

        int ret = unmarshal(&u, buf.base, buf.len, on_unmarshal, &data);
        ASSERT(ret == 0);
        ASSERT(data.nchecked == data.nexpect);
    }

    // things should be fully reset
    ASSERT(u.type == 0);
    ASSERT(u.nread_before == 0);
    ASSERT(u.rank == 0);
    ASSERT(u.len == 0);
    ASSERT(u.body == NULL);

    // one message in three buffers
    {
        struct unmarshal_test data = {
            .type = {'m'},
            .body = {"abcd"},
            .len = {4},
            .nexpect = 1,
        };

        uv_buf_t buf = mkbuf("m" "\x00\x00");
        int ret = unmarshal(&u, buf.base, buf.len, on_unmarshal, &data);
        ASSERT(ret == 0);

        buf = mkbuf("\x00\x04" "ab");
        ret = unmarshal(&u, buf.base, buf.len, on_unmarshal, &data);
        ASSERT(ret == 0);

        buf = mkbuf("cd");
        ret = unmarshal(&u, buf.base, buf.len, on_unmarshal, &data);
        ASSERT(ret == 0);

        ASSERT(data.nchecked == data.nexpect);
    }

    // things should be fully reset
    ASSERT(u.type == 0);
    ASSERT(u.nread_before == 0);
    ASSERT(u.rank == 0);
    ASSERT(u.len == 0);
    ASSERT(u.body == NULL);

    // two messages in one buffers
    {
        struct unmarshal_test data = {
            .type = {'m', 'm'},
            .body = {"abcd", "efg"},
            .len = {4, 3},
            .nexpect = 2,
        };

        uv_buf_t buf = mkbuf(
            "m" "\x00\x00\x00\x04" "abcd"
            "m" "\x00\x00\x00\x03" "efg"
        );
        int ret = unmarshal(&u, buf.base, buf.len, on_unmarshal, &data);
        ASSERT(ret == 0);
        ASSERT(data.nchecked == data.nexpect);
    }

    // things should be fully reset
    ASSERT(u.type == 0);
    ASSERT(u.nread_before == 0);
    ASSERT(u.rank == 0);
    ASSERT(u.len == 0);
    ASSERT(u.body == NULL);

    // one init, and one message
    {
        uv_buf_t buf = mkbuf(
            "i" "\x00\x00\x00\x02"
            "m" "\x00\x00\x00\x04" "abcd"
        );

        struct unmarshal_test data = {
            .type = {'i', 'm'},
            .rank = {2, 0},
            .body = {NULL, "abcd"},
            .len = {0, 4},
            .nexpect = 2,
        };

        int ret = unmarshal(&u, buf.base, buf.len, on_unmarshal, &data);
        ASSERT(ret == 0);
        ASSERT(data.nchecked == data.nexpect);
    }

done:
    unmarshal_free(&u);
    return retval;
}

static int test_dctx(void){
    int retval = 0;
    struct dc_result *C = NULL;
    struct dc_result *W1 = NULL;
    struct dc_result *W2 = NULL;
    char *data = NULL;

    struct dctx *chief;
    int ret = dctx_open(&chief, 0, 3, 0, 0, 0, 0, "localhost", "1234");
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    struct dctx *worker1;
    ret = dctx_open(&worker1, 1, 3, 1, 0, 0, 0, "localhost", "1234");
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    struct dctx *worker2;
    ret = dctx_open(&worker2, 2, 3, 2, 0, 0, 0, "localhost", "1234");
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    dc_op_t *ac, *a1, *a2;
    ASSERT(ac = dctx_gather_start_nofree(chief, NULL, "chief", 5));
    ASSERT(a1 = dctx_gather_start_nofree(worker1, NULL, "worker1", 7));
    ASSERT(a2 = dctx_gather_start_nofree(worker2, NULL, "worker 2", 8));

    dc_op_t *bc, *b1, *b2;
    ASSERT(bc = dctx_gather_start_nofree(chief, NULL, "CHIEF", 5));
    ASSERT(b1 = dctx_gather_start_nofree(worker1, NULL, "WORKER1", 7));
    ASSERT(b2 = dctx_gather_start_nofree(worker2, NULL, "WORKER 2", 8));

    size_t len;

    // gather ops in reverse order, first the second gather then
    C = dc_op_await(bc);
    W1 = dc_op_await(b1);
    W2 = dc_op_await(b2);
    // operation succeeded
    ASSERT(dc_result_ok(C));
    ASSERT(dc_result_ok(W1));
    ASSERT(dc_result_ok(W2));
    // workers get nothing back
    ASSERT(dc_result_count(W1) == 0);
    ASSERT(dc_result_count(W2) == 0);
    // chief gets 3 back
    ASSERT(dc_result_count(C) == 3);
    ASSERT((len = dc_result_len(C, 0)) == 5);
    data = dc_result_take(C, 0);
    ASSERT(strncmp(data, "CHIEF", len) == 0);
    free(data);
    ASSERT((len = dc_result_len(C, 1)) == 7);
    data = dc_result_take(C, 1);
    ASSERT(strncmp(data, "WORKER1", len) == 0);
    free(data);
    ASSERT((len = dc_result_len(C, 2)) == 8);
    data = dc_result_take(C, 2);
    ASSERT(strncmp(data, "WORKER 2", len) == 0);
    dc_result_free(&C);
    dc_result_free(&W1);
    dc_result_free(&W2);

    C = dc_op_await(ac);
    W1 = dc_op_await(a1);
    W2 = dc_op_await(a2);
    // operation succeeded
    ASSERT(dc_result_ok(C));
    ASSERT(dc_result_ok(W1));
    ASSERT(dc_result_ok(W2));
    // workers get nothing back
    ASSERT(dc_result_count(W1) == 0);
    ASSERT(dc_result_count(W2) == 0);
    // chief gets 3 back
    ASSERT(dc_result_count(C) == 3);
    ASSERT((len = dc_result_len(C, 0)) == 5);
    data = dc_result_take(C, 0);
    ASSERT(strncmp(data, "chief", len) == 0);
    free(data);
    ASSERT((len = dc_result_len(C, 1)) == 7);
    data = dc_result_take(C, 1);
    ASSERT(strncmp(data, "worker1", len) == 0);
    free(data);
    ASSERT((len = dc_result_len(C, 2)) == 8);
    data = dc_result_take(C, 2);
    ASSERT(strncmp(data, "worker 2", len) == 0);

done:
    free(data);
    dc_result_free(&C);
    dc_result_free(&W1);
    dc_result_free(&W2);

    dctx_close(&chief);
    dctx_close(&worker1);
    dctx_close(&worker2);

    return retval;
}

int main(void){
    int retval = 0;
    #define RUN(fn) do{if(fn()){printf(#fn " failed\n"); retval = 1;}}while(0)

    RUN(test_links);
    RUN(test_unmarshal);
    RUN(test_dctx);

    (void)test_dctx;

    #undef RUN
    printf("%s\n", retval ? "FAIL" : "PASS");
    return retval;
}
