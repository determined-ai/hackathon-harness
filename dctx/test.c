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

struct test_case {
    char type;
    char *series;
    uint32_t rank;
    char *body;
};

struct unmarshal_test {
    struct test_case cases[8];
    size_t nexpect;
    size_t nchecked;
    bool fail;
};

static void on_unmarshal(dc_unmarshal_t *u, void *arg){
    struct unmarshal_test *data = arg;
    struct test_case tc = data->cases[data->nchecked];
    int retval = 0;

    ASSERT(data->nexpect > data->nchecked);
    ASSERT(u->type == tc.type);
    switch(u->type){
        case 'i':
            ASSERT(u->rank == tc.rank);
            break;

        case 'g':
            ASSERT(u->slen == strlen(tc.series));
            ASSERT(strncmp(u->series, tc.series, u->slen) == 0);
            ASSERT(u->len == strlen(tc.body));
            ASSERT(strncmp(u->body, tc.body, u->len) == 0);
            break;
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

    #define FEED_BUFFER(buffer) do { \
        uv_buf_t buf = mkbuf(buffer); \
        int ret = unmarshal(&u, buf.base, buf.len, on_unmarshal, &data); \
        ASSERT(ret == 0); \
    }while(0)

    // one gather in one buffer
    {
        struct unmarshal_test data = {
            .cases = {
                { .type = 'g', .series = "ser", .body = "abcd" },
            },
            .nexpect = 1,
        };

        FEED_BUFFER("g" "\x03" "ser" "\x00\x00\x00\x04" "abcd");

        ASSERT(data.nchecked == data.nexpect);
        ASSERT(!data.fail);
    }

    // things should be fully reset
    ASSERT(u.type == 0);
    ASSERT(u.nread_before == 0);
    ASSERT(u.rank == 0);
    ASSERT(u.len == 0);
    ASSERT(u.body == NULL);

    // one gather in many buffers
    {
        struct unmarshal_test data = {
            .cases = {
                { .type = 'g', .series = "seri", .body = "abcd" },
            },
            .nexpect = 1,
        };

        FEED_BUFFER("g");
        FEED_BUFFER("\x04");
        FEED_BUFFER("s");
        FEED_BUFFER("er");
        FEED_BUFFER("i");
        FEED_BUFFER("\x00");
        FEED_BUFFER("\x00");
        FEED_BUFFER("\x00");
        FEED_BUFFER("\x04");
        FEED_BUFFER("a");
        FEED_BUFFER("bc");
        FEED_BUFFER("d");

        ASSERT(data.nchecked == data.nexpect);
        ASSERT(!data.fail);
    }

    // things should be fully reset
    ASSERT(u.type == 0);
    ASSERT(u.nread_before == 0);
    ASSERT(u.rank == 0);
    ASSERT(u.len == 0);
    ASSERT(u.body == NULL);

    // two gathers in one buffers
    {
        struct unmarshal_test data = {
            .cases = {
                { .type = 'g', .series = "ser", .body = "abcd" },
                { .type = 'g', .series = "ser", .body = "efg" },
            },
            .nexpect = 2,
        };

        FEED_BUFFER(
            "g" "\x03" "ser" "\x00\x00\x00\x04" "abcd"
            "g" "\x03" "ser" "\x00\x00\x00\x03" "efg"
        );

        ASSERT(data.nchecked == data.nexpect);
        ASSERT(!data.fail);
    }

    // things should be fully reset
    ASSERT(u.type == 0);
    ASSERT(u.nread_before == 0);
    ASSERT(u.rank == 0);
    ASSERT(u.len == 0);
    ASSERT(u.body == NULL);

    // one init, and one gather
    {
        struct unmarshal_test data = {
            .cases = {
                { .type = 'i', .rank = 2 },
                { .type = 'g', .series = "ser", .body = "abcd" },
            },
            .nexpect = 2,
        };

        FEED_BUFFER(
            "i" "\x00\x00\x00\x02"
            "g" "\x03" "ser" "\x00\x00\x00\x04" "abcd"
        );

        ASSERT(data.nchecked == data.nexpect);
        ASSERT(!data.fail);
    }

done:
    unmarshal_free(&u);
    return retval;
}

static int test_dctx(void){
    int retval = 0;
    dc_result_t *rg0a = NULL;
    dc_result_t *rg1a = NULL;
    dc_result_t *rg2a = NULL;
    dc_result_t *rg0b = NULL;
    dc_result_t *rg1b = NULL;
    dc_result_t *rg2b = NULL;
    dc_result_t *rb0a = NULL;
    dc_result_t *rb1a = NULL;
    dc_result_t *rb2a = NULL;
    char *data = NULL;
    int ret;

    #define ASSERT_RESULT(r, i, buf) do { \
        data = dc_result_take(r, i); \
        size_t len = dc_result_len(r, i); \
        ASSERT(zstrneq(data, len, buf, strlen(buf))); \
        free(data); \
        data = NULL; \
    } while(0)

    struct dctx *chief;
    ret = dctx_open(&chief, 0, 3, 0, 0, 0, 0, "localhost", "1234");
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

    // submit ops in arbitrary order

    dc_op_t *g0a = dctx_gather_nofree(chief, "a", 1, "chief", 5);
    dc_op_t *g1a = dctx_gather_nofree(worker1, "a", 1, "worker1", 7);
    dc_op_t *g2b = dctx_gather_nofree(worker2, "b", 1, "WORKER 2", 8);

    dc_op_t *b0a = dctx_broadcast_copy(chief, "a", 1, "bchief", 6);

    dc_op_t *g0b = dctx_gather_nofree(chief, "b", 1, "CHIEF", 5);
    dc_op_t *g1b = dctx_gather_nofree(worker1, "b", 1, "WORKER1", 7);
    dc_op_t *g2a = dctx_gather_nofree(worker2, "a", 1, "worker 2", 8);

    dc_op_t *b1a = dctx_broadcast(worker1, "a", 1, NULL, 0);
    dc_op_t *b2a = dctx_broadcast(worker2, "a", 1, NULL, 0);

    ASSERT(dc_op_ok(g0a));
    ASSERT(dc_op_ok(g1a));
    ASSERT(dc_op_ok(g2a));
    ASSERT(dc_op_ok(g0b));
    ASSERT(dc_op_ok(g1b));
    ASSERT(dc_op_ok(g2b));
    ASSERT(dc_op_ok(b0a));
    ASSERT(dc_op_ok(b1a));
    ASSERT(dc_op_ok(b2a));

    // await ops in arbitrary order

    // gather series=a
    rg0a = dc_op_await(g0a);
    rg1a = dc_op_await(g1a);
    rg2a = dc_op_await(g2a);
    ASSERT(dc_result_ok(rg0a));
    ASSERT(dc_result_ok(rg1a));
    ASSERT(dc_result_ok(rg2a));
    ASSERT(dc_result_count(rg1a) == 0);
    ASSERT(dc_result_count(rg2a) == 0);
    ASSERT(dc_result_count(rg0a) == 3);
    ASSERT_RESULT(rg0a, 0, "chief");
    ASSERT_RESULT(rg0a, 1, "worker1");
    ASSERT_RESULT(rg0a, 2, "worker 2");

    // gather series=b
    rg0b = dc_op_await(g0b);
    rg1b = dc_op_await(g1b);
    rg2b = dc_op_await(g2b);
    ASSERT(dc_result_ok(rg0b));
    ASSERT(dc_result_ok(rg1b));
    ASSERT(dc_result_ok(rg2b));
    ASSERT(dc_result_count(rg1b) == 0);
    ASSERT(dc_result_count(rg2b) == 0);
    ASSERT(dc_result_count(rg0b) == 3);
    ASSERT_RESULT(rg0b, 0, "CHIEF");
    ASSERT_RESULT(rg0b, 1, "WORKER1");
    ASSERT_RESULT(rg0b, 2, "WORKER 2");

    // broadcast series=b
    rb0a = dc_op_await(b0a);
    rb1a = dc_op_await(b1a);
    rb2a = dc_op_await(b2a);
    ASSERT(dc_result_ok(rb0a));
    ASSERT(dc_result_ok(rb1a));
    ASSERT(dc_result_ok(rb2a));
    ASSERT(dc_result_count(rb1a) == 1);
    ASSERT(dc_result_count(rb2a) == 1);
    ASSERT(dc_result_count(rb0a) == 1);
    ASSERT_RESULT(rb0a, 0, "bchief");
    ASSERT_RESULT(rb1a, 0, "bchief");
    ASSERT_RESULT(rb2a, 0, "bchief");

    #undef ASSERT_RESULT

done:
    free(data);
    dc_result_free(&rg0a);
    dc_result_free(&rg1a);
    dc_result_free(&rg2a);
    dc_result_free(&rg0b);
    dc_result_free(&rg1b);
    dc_result_free(&rg2b);
    dc_result_free(&rb0a);
    dc_result_free(&rb1a);
    dc_result_free(&rb2a);

    dctx_close(&chief);
    dctx_close(&worker1);
    dctx_close(&worker2);

    return retval;
}

int main(void){
    signal(SIGPIPE, SIG_IGN);

    int retval = 0;
    #define RUN(fn) do{if(fn()){printf(#fn " failed\n"); retval = 1;}}while(0)

    RUN(test_links);
    RUN(test_unmarshal);
    RUN(test_dctx);

    #undef RUN
    printf("%s\n", retval ? "FAIL" : "PASS");
    return retval;
}
