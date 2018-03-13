#ifndef common_H
#define common_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ZBOX_EID_SIZE     32

typedef unsigned char zbox_eid[ZBOX_EID_SIZE];

enum zbox_ops_limit {
    ZBOX_OPS_INTERACTIVE,
    ZBOX_OPS_MODERATE,
    ZBOX_OPS_SENSITIVE
};

enum zbox_mem_limit {
    ZBOX_MEM_INTERACTIVE,
    ZBOX_MEM_MODERATE,
    ZBOX_MEM_SENSITIVE
};

enum zbox_cipher {
    ZBOX_CIPHER_XCHACHA,
    ZBOX_CIPHER_AES
};

enum zbox_file_type {
    ZBOX_FTYPE_FILE,
    ZBOX_FTYPE_DIR
};

struct zbox_metadata {
    enum zbox_file_type ftype;
    size_t len;
    size_t curr_version;
    time_t created;
    time_t modified;
};

struct zbox_dir_entry {
    const char *path;
    const char *file_name;
    struct zbox_metadata metadata;
};

struct zbox_dir_entry_list {
    struct zbox_dir_entry *entries;
    size_t len;
    size_t capacity;
};

struct zbox_version {
    size_t num;
    size_t len;
    time_t created;
};

struct zbox_version_list {
    struct zbox_version *versions;
    size_t len;
    size_t capacity;
};

typedef void *zbox_repo;
typedef void *zbox_file;

// init env
extern int zbox_init_env();

#ifdef __cplusplus
}
#endif

#endif
