#ifndef MERKLE_TREE_H
#define MERKLE_TREE_H

#include "hash.h"
#include <stdint.h>

typedef struct merkle_node {
    uint8_t hash[SHA256_SIZE * 2 + 1];
    struct merkle_node *left;
    struct merkle_node *right;
    uint8_t *data; // Only for leaves
} merkle_node_t;

merkle_node_t *merkle_tree_build(merkle_node_t **leaves, size_t count);
void merkle_tree_free(merkle_node_t *root);

#endif
