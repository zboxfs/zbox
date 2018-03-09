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

// init env
extern int zbox_init_env();

#ifdef __cplusplus
}
#endif

#endif
