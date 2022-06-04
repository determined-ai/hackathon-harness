#include "internal.h"

static void head_init(link_t *head){
    head->prev = head;
    head->next = head;
}

void link_list_prepend(link_t *head, link_t *link){
    // safe to call on a zeroized link
    if(head->next == NULL) head_init(head);

    // for an empty list, old_next is just head
    link_t *old_next = head->next;
    // in the empty list case, this set's head's .next and .prev
    head->next = link;
    old_next->prev = link;
    // in the empty list case, this points link's .next and .prev to head
    link->prev = head;
    link->next = old_next;
}

void link_list_append(link_t *head, link_t *link){
    // safe to call on a zeroized link
    if(head->next == NULL) head_init(head);

    link_t *old_prev = head->prev;
    old_prev->next = link;
    head->prev = link;
    link->prev = old_prev;
    link->next = head;
}

link_t *link_list_pop_first(link_t *head){
    // safe to call on a zeroized link
    if(head->next == NULL) return NULL;

    link_t *first = head->next;
    if(first == head){
        return NULL;
    }
    link_remove(first);
    return first;
}

link_t *link_list_pop_last(link_t *head){
    // safe to call on a zeroized link
    if(head->next == NULL) return NULL;

    link_t *last = head->prev;
    if(last == head){
        return NULL;
    }
    link_remove(last);
    return last;
}

void link_remove(link_t *link){
    // safe to call on a zeroized link
    if(link->next == NULL) return;

    // for a link not in a list, this will reduce to link->prev = link->prev
    link->next->prev = link->prev;
    link->prev->next = link->next;
    link->next = NULL;
    link->prev = NULL;
}

bool link_list_isempty(link_t *head){
    // safe to call on a zeroized link
    return head == head->next || head->next == NULL;
}
