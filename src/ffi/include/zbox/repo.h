#ifndef repo_H
#define repo_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

// repo info
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

extern int zbox_repo_reset_password(zbox_repo repo,
                                    const char *old_pwd,
                                    const char *new_pwd,
                                    enum zbox_ops_limit ops_limit,
                                    enum zbox_mem_limit mem_limit);
extern bool zbox_repo_path_exists(zbox_repo repo, const char *path);
extern bool zbox_repo_is_file(zbox_repo repo, const char *path);
extern bool zbox_repo_is_dir(zbox_repo repo, const char *path);

// create file
extern int zbox_repo_create_file(zbox_file *file,
                                 zbox_repo repo,
                                 const char *path);

// open file
extern int zbox_repo_open_file(zbox_file *file,
                               zbox_repo repo,
                               const char *path);

// close file
extern void zbox_close_file(zbox_file file);

// create dir
extern int zbox_repo_create_dir(zbox_repo repo, const char *path);
extern int zbox_repo_create_dir_all(zbox_repo repo, const char *path);

// read dir
extern int zbox_repo_read_dir(struct zbox_dir_entry_list *entry_list,
                              zbox_repo repo,
                              const char *path);
extern void zbox_destroy_dir_entry_list(struct zbox_dir_entry_list *entry_list);

// metadata
extern int zbox_repo_metadata(struct zbox_metadata *metadata,
                              zbox_repo repo,
                              const char *path);

// history
extern int zbox_repo_history(struct zbox_version_list *version_list,
                             zbox_repo repo,
                             const char *path);
extern int zbox_destroy_version_list(struct zbox_version_list *version_list);

// copy
extern int zbox_repo_copy(const char *to, const char *from, zbox_repo repo);

// remove file and dir
extern int zbox_repo_remove_file(const char *path, zbox_repo repo);
extern int zbox_repo_remove_dir(const char *path, zbox_repo repo);
extern int zbox_repo_remove_dir_all(const char *path, zbox_repo repo);

// rename
extern int zbox_repo_rename(const char *to, const char *from, zbox_repo repo);

#ifdef __cplusplus
}
#endif

#endif
