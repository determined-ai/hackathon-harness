typedef struct {
    char type;  // "i"nit, "g"ather, "k"eepalive
    size_t nread_before;
    // init, allgather arg
    uint32_t rank;
    // gather args
    uint32_t slen;
    char series[256];
    uint32_t len;
    char *body;
} dc_unmarshal_t;

// init msg format: iNNNN (NNNN = MSB-first rank)
#define INIT_MSG_SIZE 5
size_t marshal_init(char *buf, int rank);

// gather msg format: gUseriesNNNNbody (U = series len, NNNN = body len)
// (1 + 1 + 256 + 4)
#define GATHER_MSG_HDR_MAXSIZE 262 // 1 + 1 + 256 + 4
size_t marshal_gather(
    char *buf, const char *series, size_t slen, size_t body_len
);

// broadcast msg format: bUseriesNNNNbody (U = series len, NNNN = body len)
// (1 + 1 + 256 + 4)
#define BROADCAST_MSG_HDR_MAXSIZE 262 // 1 + 1 + 256 + 4
size_t marshal_broadcast(
    char *buf, const char *series, size_t slen, size_t body_len
);

// allgather msg format: aUseriesRRRRNNNNbody
// (U = series len, RRRR = rank, NNNN = body len)
// (1 + 1 + 256 + 4 + 4)
#define ALLGATHER_MSG_HDR_MAXSIZE 266 // 1 + 1 + 256 + 4 + 4
size_t marshal_allgather(
    char *buf, const char *series, size_t slen, uint32_t rank, size_t body_len
);

// calls on_unmarshal once for every message found
int unmarshal(
    dc_unmarshal_t *unmarshal,
    char *buf,
    size_t len,
    void (*on_unmarshal)(dc_unmarshal_t*, void*),
    void *arg
);
void unmarshal_free(dc_unmarshal_t *unmarshal);
