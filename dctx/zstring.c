#include "string.h"

#include "internal.h"

size_t zstrlen(const char *s){
    return s ? strlen(s) : 0;
}

size_t zstrnlen(const char *s, size_t limit){
    return s ? strnlen(s, limit) : 0;
}

size_t zstreq(const char *a, const char *b){
    return strcmp(a ? a : "", b ? b : "") == 0;
}

size_t zstrneq(const char *a, size_t alen, const char *b, size_t blen){
    if(alen != blen) return false;
    return strncmp(a ? a : "", b ? b : "", alen) == 0;
}
