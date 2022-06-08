// handle NULL strings as zero-length strings

size_t zstrlen(const char *s);
size_t zstrnlen(const char *s, size_t limit);
size_t zstreq(const char *a, const char *b);
size_t zstrneq(const char *a, size_t alen, const char *b, size_t blen);
