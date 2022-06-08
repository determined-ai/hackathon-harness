// circularly linked lists, where the head element is not part of the list

typedef struct link {
    struct link *prev;
    struct link *next;
} link_t;

void link_init(link_t *l);

/* prepend/append a single element.  You must ensure that link is not in a list
   via link_remove() before calling these; the new and old lists may have
   different thread safety requirements */
void link_list_prepend(link_t *head, link_t *link);
void link_list_append(link_t *head, link_t *link);

// pop a single element, or return NULL if there is none
link_t *link_list_pop_first(link_t *head);
link_t *link_list_pop_last(link_t *head);

void link_remove(link_t *link);

bool link_list_isempty(link_t *head);

/* DEF_CONTAINER_OF should be used right after struct definition to create an
   inline function for dereferencing a struct via a member.  "member_type" is
   required to avoid typeof, which windows doesn't have.  Also, unlike the
   linux kernel version, multi-token types ("struct xyz") are not supported. */
#define DEF_CONTAINER_OF(structure, member, member_type) \
    static inline structure *structure ## _ ## member ## _container_of( \
            const member_type *ptr){ \
        if(ptr == NULL) return NULL; \
        uintptr_t offset = offsetof(structure, member); \
        return (structure*)((uintptr_t)ptr - offset); \
    }

#define CONTAINER_OF(ptr, structure, member) \
    structure ## _ ## member ## _container_of(ptr)

// automate for-loops which call CONTAINER_OF for each link in list
#define LINK_FOR_EACH(var, head, structure, member) \
    for(var = CONTAINER_OF((head)->next, structure, member); \
        var && &var->member != (head); \
        var = CONTAINER_OF(var->member.next, structure, member))

// same thing but use a temporary variable to be safe against link_remove
#define LINK_FOR_EACH_SAFE(var, temp, head, structure, member) \
    for(var = CONTAINER_OF((head)->next, structure, member), \
        temp = !var?NULL:CONTAINER_OF(var->member.next, structure, member); \
        var && &var->member != (head); \
        var = temp, \
        temp = CONTAINER_OF(var->member.next, structure, member))

