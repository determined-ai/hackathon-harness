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
    dc_result_t *rac = NULL;
    dc_result_t *ra1 = NULL;
    dc_result_t *ra2 = NULL;
    dc_result_t *rbc = NULL;
    dc_result_t *rb1 = NULL;
    dc_result_t *rb2 = NULL;
    char *data = NULL;

    #define ASSERT_RESULT(r, i, buf) do { \
        data = dc_result_take(r, i); \
        size_t len = dc_result_len(r, i); \
        ASSERT(zstrneq(data, len, buf, strlen(buf))); \
        free(data); \
        data = NULL; \
    } while(0)

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

    // submit ops in arbitrary order

    dc_op_t *ac = dctx_gather_start_nofree(chief, "a", 1, "chief", 5);
    dc_op_t *a1 = dctx_gather_start_nofree(worker1, "a", 1, "worker1", 7);
    dc_op_t *b2 = dctx_gather_start_nofree(worker2, "b", 1, "WORKER 2", 8);

    dc_op_t *bc = dctx_gather_start_nofree(chief, "b", 1, "CHIEF", 5);
    dc_op_t *b1 = dctx_gather_start_nofree(worker1, "b", 1, "WORKER1", 7);
    dc_op_t *a2 = dctx_gather_start_nofree(worker2, "a", 1, "worker 2", 8);

    ASSERT(dc_op_ok(ac));
    ASSERT(dc_op_ok(a1));
    ASSERT(dc_op_ok(a2));
    ASSERT(dc_op_ok(bc));
    ASSERT(dc_op_ok(b1));
    ASSERT(dc_op_ok(b2));

    // gather ops in arbitrary order
    rac = dc_op_await(ac);
    ra1 = dc_op_await(a1);
    ra2 = dc_op_await(a2);
    // operation succeeded
    ASSERT(dc_result_ok(rac));
    ASSERT(dc_result_ok(ra1));
    ASSERT(dc_result_ok(ra2));
    // workers get nothing back
    ASSERT(dc_result_count(ra1) == 0);
    ASSERT(dc_result_count(ra2) == 0);
    ASSERT(dc_result_count(rac) == 3);
    // chief gets the right results
    ASSERT_RESULT(rac, 0, "chief");
    ASSERT_RESULT(rac, 1, "worker1");
    ASSERT_RESULT(rac, 2, "worker 2");

    rbc = dc_op_await(bc);
    rb1 = dc_op_await(b1);
    rb2 = dc_op_await(b2);
    ASSERT(dc_result_ok(rbc));
    ASSERT(dc_result_ok(rb1));
    ASSERT(dc_result_ok(rb2));
    ASSERT(dc_result_count(rb1) == 0);
    ASSERT(dc_result_count(rb2) == 0);
    ASSERT(dc_result_count(rbc) == 3);
    ASSERT_RESULT(rbc, 0, "CHIEF");
    ASSERT_RESULT(rbc, 1, "WORKER1");
    ASSERT_RESULT(rbc, 2, "WORKER 2");

done:
    free(data);
    dc_result_free(&rac);
    dc_result_free(&ra1);
    dc_result_free(&ra2);
    dc_result_free(&rbc);
    dc_result_free(&rb1);
    dc_result_free(&rb2);

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
    (void)test_links;
    (void)test_dctx;

    (void)test_dctx;

    #undef RUN
    printf("%s\n", retval ? "FAIL" : "PASS");
    return retval;
}
