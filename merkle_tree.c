#include "merkle_tree.h"
#include <stdio.h>
#include <string.h>

static void generate_hash(const uint8_t *data, size_t len,
                          uint8_t out[SHA256_SIZE * 2])
{
    uint8_t hash[SHA256_SIZE] = {0};
    sha256_hash((const uint8_t *)data, len, hash);
    for (size_t i = 0; i < SHA256_SIZE; ++i)
        snprintf((char *)out + (i * 2), 2, "%02x", hash[i]);
    out[SHA256_SIZE * 2] = '\0';
}

static merkle_node_t *merkle_tree_leaf(const uint8_t *data, size_t chunk_size)
{
    merkle_node_t *node = calloc(1, sizeof(*node));
    node->data          = calloc(1, chunk_size);
    memcpy(node->data, data, chunk_size);
    generate_hash(node->data, chunk_size, node->hash);
    return node;
}

static merkle_node_t *merkle_tree_parent(merkle_node_t *left,
                                         merkle_node_t *right)
{
    merkle_node_t *node                   = calloc(1, sizeof(node));
    node->left                            = left;
    node->right                           = right;

    uint8_t combined[SHA256_SIZE * 4 + 1] = {0};
    snprintf((char *)combined, sizeof(combined), "%s%s", left->hash,
             right->hash);
    generate_hash(combined, SHA256_SIZE * 4 + 1, node->hash);
    return node;
}

merkle_node_t *merkle_tree_build(merkle_node_t **leaves, size_t count)
{
    if (count == 1)
        return leaves[0];

    size_t parent_count     = (count + 1) / 2;
    merkle_node_t **parents = calloc(parent_count, sizeof(*parents));

    for (size_t i = 0; i < parent_count; ++i) {
        if (2 * i + 1 > count)
            parents[i] = merkle_tree_parent(leaves[2 * i], leaves[2 * i + 1]);
        else
            parents[i] = leaves[2 * i];
    }

    free(leaves);
    return merkle_tree_build(parents, parent_count);
}

void merkle_tree_free(merkle_node_t *root)
{
    if (!root)
        return;

    merkle_tree_free(root->left);
    merkle_tree_free(root->right);

    free(root->data);
    free(root);
}
