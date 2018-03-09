#ifndef repo_H
#define repo_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

struct zbox_repo_info {
    zbox_eid volume_id;
    const char *version;
    const char *uri;
    enum zbox_ops_limit ops_limit;
    enum zbox_mem_limit mem_limit;
    enum zbox_cipher cipher;
    uint8_t version_limit;
    bool is_read_only;
    time_t created;
};

typedef void *zbox_opener;
typedef void *zbox_repo;

// repo opener
extern zbox_opener zbox_create_opener();
extern void zbox_opener_ops_limit(zbox_opener opener, enum zbox_ops_limit limit);
extern void zbox_opener_mem_limit(zbox_opener opener, enum zbox_mem_limit limit);
extern void zbox_opener_cipher(zbox_opener opener, enum zbox_cipher cipher);
extern void zbox_opener_create(zbox_opener opener, bool create);
extern void zbox_opener_create_new(zbox_opener opener, bool create_new);
extern void zbox_opener_version_limit(zbox_opener opener, uint8_t limit);
extern void zbox_opener_read_only(zbox_opener opener, bool read_only);
extern void zbox_free_opener(zbox_opener opener);

// repo
extern int zbox_open_repo(zbox_repo *repo,
                          zbox_opener opener,
                          const char *uri,
                          const char *pwd);
extern void zbox_close_repo(zbox_repo repo);
extern int zbox_repo_exists(bool *out, const char *uri);

// repo info
extern void zbox_get_repo_info(struct zbox_repo_info *info, zbox_repo repo);
extern void zbox_destroy_repo_info(struct zbox_repo_info *info);

#ifdef __cplusplus
}
#endif

#endif
