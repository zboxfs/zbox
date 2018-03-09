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

#ifdef __cplusplus
}
#endif

#endif

