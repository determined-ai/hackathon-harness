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
    char *expect[8];
    size_t len[8];
    size_t nexpect;
    size_t nchecked;
    bool fail;
};

static void on_unmarshal(struct dc_unmarshal *u, void *arg){
    struct unmarshal_test *data = arg;
    int retval = 0;

    ASSERT(data->nexpect > data->nchecked);
    ASSERT(u->type == 'm');
    ASSERT(u->len == data->len[data->nchecked]);
    ASSERT(strncmp(u->body, data->expect[data->nchecked], u->len) == 0);

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
            .expect = {"abcd"},
            .len = {4},
            .nexpect = 1,
        };

        int ret = unmarshal(&u, &buf, on_unmarshal, &data);
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
            .expect = {"abcd"},
            .len = {4},
            .nexpect = 1,
        };

        uv_buf_t buf = mkbuf("m" "\x00\x00");
        int ret = unmarshal(&u, &buf, on_unmarshal, &data);
        ASSERT(ret == 0);

        buf = mkbuf("\x00\x04" "ab");
        ret = unmarshal(&u, &buf, on_unmarshal, &data);
        ASSERT(ret == 0);

        buf = mkbuf("cd");
        ret = unmarshal(&u, &buf, on_unmarshal, &data);
        ASSERT(ret == 0);

        ASSERT(data.nchecked == data.nexpect);
    }

    // two messages in one buffers
    {
        struct unmarshal_test data = {
            .expect = {"abcd", "efg"},
            .len = {4, 3},
            .nexpect = 2,
        };

        uv_buf_t buf = mkbuf(
            "m" "\x00\x00\x00\x04" "abcd"
            "m" "\x00\x00\x00\x03" "efg"
        );
        int ret = unmarshal(&u, &buf, on_unmarshal, &data);
        ASSERT(ret == 0);
        ASSERT(data.nchecked == data.nexpect);
    }

done:
    unmarshal_free(&u);
    return retval;
}

static int test_dctx(void){
    struct dctx *chief;
    int ret = dctx_open(&chief, 0, 2, 0, 2, 0, 1, "localhost:1234", 0);
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    struct dctx *worker;
    ret = dctx_open(&worker, 1, 2, 1, 2, 0, 1, "localhost:1234", 0);
    if(ret){
        printf("dctx_open failed! %d\n", ret);
        return 1;
    }

    usleep(1000000);

    dctx_close(&chief);
    dctx_close(&worker);
    return 0;
}

int main(void){
    int retval = 0;
    #define RUN(fn) do{if(fn()){printf(#fn "failed\n"); retval = 1;}}while(0)

    RUN(test_unmarshal);
    RUN(test_dctx);

    (void)test_dctx;

    #undef RUN
    printf("%s\n", retval ? "FAIL" : "PASS");
    return retval;
}
