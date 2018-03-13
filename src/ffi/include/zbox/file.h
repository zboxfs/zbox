#ifndef file_H
#define file_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *zbox_options;
typedef void *zbox_version_reader;

// file open options
extern zbox_options zbox_create_options();
extern void zbox_options_read(zbox_options options, bool read);
extern void zbox_options_write(zbox_options options, bool write);
extern void zbox_options_append(zbox_options options, bool append);
extern void zbox_options_truncate(zbox_options options, bool truncate);
extern void zbox_options_create(zbox_options options, bool create);
extern void zbox_options_create_new(zbox_options options, bool create_new);
extern void zbox_options_version_limit(zbox_options options, uint8_t limit);
extern void zbox_free_options(zbox_options options);

// metadata
extern int zbox_file_metadata(struct zbox_metadata *metadata,
                              zbox_file file);

// history
extern int zbox_file_history(struct zbox_version_list *version_list,
                             zbox_file file);

// current version
extern int zbox_file_curr_version(size_t *version_num, zbox_file file);

// read
extern int zbox_file_read(uint8_t *dst, size_t len, zbox_file file);

// version reader
extern int zbox_file_version_reader(zbox_version_reader *reader,
                                    size_t ver_num,
                                    zbox_file file);
extern int zbox_file_version_read(uint8_t *dst, size_t len, zbox_file file);
extern int zbox_file_version_reader_seek(zbox_file file,
                                         int64_t offset,
                                         int whence);
extern void zbox_close_version_reader(zbox_version_reader *reader);

// write and finish
extern int zbox_file_write(zbox_file file, uint8_t *buf, size_t len);
extern int zbox_file_finish(zbox_file file);

// write once
extern int zbox_file_write_once(zbox_file file, uint8_t *buf, size_t len);

// seek
extern int zbox_file_seek(zbox_file file, int64_t offset, int whence);

// set length
extern int zbox_file_set_len(zbox_file file, size_t len);

#ifdef __cplusplus
}
#endif

#endif

