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
    dc_result_t *rg0x = NULL;
    dc_result_t *rg1x = NULL;
    dc_result_t *rg2x = NULL;
    dc_result_t *rg0y = NULL;
    dc_result_t *rg1y = NULL;
    dc_result_t *rg2y = NULL;
    dc_result_t *rb0x = NULL;
    dc_result_t *rb1x = NULL;
    dc_result_t *rb2x = NULL;
    dc_result_t *ra0x = NULL;
    dc_result_t *ra1x = NULL;
    dc_result_t *ra2x = NULL;
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

    dc_op_t *a2x = dctx_allgather_nofree(worker2, "x", 1, "ag2", 3);

    dc_op_t *g0x = dctx_gather_nofree(chief, "x", 1, "chief", 5);
    dc_op_t *g1x = dctx_gather_nofree(worker1, "x", 1, "worker1", 7);
    dc_op_t *g2y = dctx_gather_nofree(worker2, "y", 1, "WORKER 2", 8);

    dc_op_t *b0x = dctx_broadcast_copy(chief, "x", 1, "bchief", 6);

    dc_op_t *a1x = dctx_allgather_nofree(worker1, "x", 1, "ag1", 3);

    dc_op_t *g0y = dctx_gather_nofree(chief, "y", 1, "CHIEF", 5);
    dc_op_t *g1y = dctx_gather_nofree(worker1, "y", 1, "WORKER1", 7);
    dc_op_t *g2x = dctx_gather_nofree(worker2, "x", 1, "worker 2", 8);

    dc_op_t *b1x = dctx_broadcast(worker1, "x", 1, NULL, 0);
    dc_op_t *b2x = dctx_broadcast(worker2, "x", 1, NULL, 0);

    dc_op_t *a0x = dctx_allgather_nofree(chief, "x", 1, "ag0", 3);

    ASSERT(dc_op_ok(g0x));
    ASSERT(dc_op_ok(g1x));
    ASSERT(dc_op_ok(g2x));
    ASSERT(dc_op_ok(g0y));
    ASSERT(dc_op_ok(g1y));
    ASSERT(dc_op_ok(g2y));
    ASSERT(dc_op_ok(b0x));
    ASSERT(dc_op_ok(b1x));
    ASSERT(dc_op_ok(b2x));
    ASSERT(dc_op_ok(a0x));
    ASSERT(dc_op_ok(a1x));
    ASSERT(dc_op_ok(a2x));

    // await ops in arbitrary order

    // gather series=x
    rg0x = dc_op_await(g0x);
    rg1x = dc_op_await(g1x);
    rg2x = dc_op_await(g2x);
    ASSERT(dc_result_ok(rg0x));
    ASSERT(dc_result_ok(rg1x));
    ASSERT(dc_result_ok(rg2x));
    ASSERT(dc_result_count(rg1x) == 0);
    ASSERT(dc_result_count(rg2x) == 0);
    ASSERT(dc_result_count(rg0x) == 3);
    ASSERT_RESULT(rg0x, 0, "chief");
    ASSERT_RESULT(rg0x, 1, "worker1");
    ASSERT_RESULT(rg0x, 2, "worker 2");

    // gather series=y
    rg0y = dc_op_await(g0y);
    rg1y = dc_op_await(g1y);
    rg2y = dc_op_await(g2y);
    ASSERT(dc_result_ok(rg0y));
    ASSERT(dc_result_ok(rg1y));
    ASSERT(dc_result_ok(rg2y));
    ASSERT(dc_result_count(rg1y) == 0);
    ASSERT(dc_result_count(rg2y) == 0);
    ASSERT(dc_result_count(rg0y) == 3);
    ASSERT_RESULT(rg0y, 0, "CHIEF");
    ASSERT_RESULT(rg0y, 1, "WORKER1");
    ASSERT_RESULT(rg0y, 2, "WORKER 2");

    // broadcast series=x
    rb0x = dc_op_await(b0x);
    rb1x = dc_op_await(b1x);
    rb2x = dc_op_await(b2x);
    ASSERT(dc_result_ok(rb0x));
    ASSERT(dc_result_ok(rb1x));
    ASSERT(dc_result_ok(rb2x));
    ASSERT(dc_result_count(rb1x) == 1);
    ASSERT(dc_result_count(rb2x) == 1);
    ASSERT(dc_result_count(rb0x) == 1);
    ASSERT_RESULT(rb0x, 0, "bchief");
    ASSERT_RESULT(rb1x, 0, "bchief");
    ASSERT_RESULT(rb2x, 0, "bchief");

    // allgather series=x
    ra0x = dc_op_await(a0x);
    ra1x = dc_op_await(a1x);
    ra2x = dc_op_await(a2x);
    ASSERT(dc_result_ok(ra0x));
    ASSERT(dc_result_ok(ra1x));
    ASSERT(dc_result_ok(ra2x));
    ASSERT(dc_result_count(ra0x) == 3);
    ASSERT(dc_result_count(ra1x) == 3);
    ASSERT(dc_result_count(ra2x) == 3);
    ASSERT_RESULT(ra0x, 0, "ag0");
    ASSERT_RESULT(ra0x, 1, "ag1");
    ASSERT_RESULT(ra0x, 2, "ag2");
    ASSERT_RESULT(ra1x, 0, "ag0");
    ASSERT_RESULT(ra1x, 1, "ag1");
    ASSERT_RESULT(ra1x, 2, "ag2");
    ASSERT_RESULT(ra2x, 0, "ag0");
    ASSERT_RESULT(ra2x, 1, "ag1");
    ASSERT_RESULT(ra2x, 2, "ag2");

    #undef ASSERT_RESULT

done:
    free(data);
    dc_result_free(&rg0x);
    dc_result_free(&rg1x);
    dc_result_free(&rg2x);
    dc_result_free(&rg0y);
    dc_result_free(&rg1y);
    dc_result_free(&rg2y);
    dc_result_free(&rb0x);
    dc_result_free(&rb1x);
    dc_result_free(&rb2x);
    dc_result_free(&ra0x);
    dc_result_free(&ra1x);
    dc_result_free(&ra2x);

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
