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

struct unmarshal_test {
    char type[8];
    int rank[8];
    char *body[8];
    size_t len[8];
    size_t nexpect;
    size_t nchecked;
    bool fail;
};

static void on_unmarshal(struct dc_unmarshal *u, void *arg){
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
    struct dc_unmarshal u = {0};

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
    struct dc_result *c = NULL;
    struct dc_result *w1 = NULL;
    struct dc_result *w2 = NULL;
    char *data = NULL;

    struct dctx *chief;
    int ret = dctx_open(&chief, 0, 3, 0, 3, 0, 1, "localhost", "1234");
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    struct dctx *worker1;
    ret = dctx_open(&worker1, 1, 3, 1, 3, 0, 1, "localhost", "1234");
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    struct dctx *worker2;
    ret = dctx_open(&worker2, 2, 3, 2, 3, 0, 1, "localhost", "1234");
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    ASSERT(dctx_gather_start(chief, strdup("chief"), 5) == 0);
    ASSERT(dctx_gather_start(worker1, strdup("worker1"), 7) == 0);
    ASSERT(dctx_gather_start(worker2, strdup("worker 2"), 8) == 0);
    c = dctx_gather_end(chief);
    w1 = dctx_gather_end(worker1);
    w2 = dctx_gather_end(worker2);
    // operation succeeded
    ASSERT(dc_result_ok(c));
    ASSERT(dc_result_ok(w1));
    ASSERT(dc_result_ok(w2));
    // workers get nothing back
    ASSERT(dc_result_count(w1) == 0);
    ASSERT(dc_result_count(w2) == 0);
    // chief gets 3 back
    size_t len;
    data = dc_result_take(c, 0, &len);
    ASSERT(len == 5);
    ASSERT(strncmp(data, "chief", len) == 0);
    free(data);
    data = dc_result_take(c, 1, &len);
    ASSERT(len == 7);
    ASSERT(strncmp(data, "worker1", len) == 0);
    free(data);
    data = dc_result_take(c, 2, &len);
    ASSERT(len == 8);
    ASSERT(strncmp(data, "worker 2", len) == 0);


done:
    free(data);
    dc_result_free(&c);
    dc_result_free(&w1);
    dc_result_free(&w2);

    dctx_close(&chief);
    dctx_close(&worker1);
    dctx_close(&worker2);

    return retval;
}

int main(void){
    int retval = 0;
    #define RUN(fn) do{if(fn()){printf(#fn " failed\n"); retval = 1;}}while(0)

    RUN(test_unmarshal);
    RUN(test_dctx);

    (void)test_dctx;

    #undef RUN
    printf("%s\n", retval ? "FAIL" : "PASS");
    return retval;
}
