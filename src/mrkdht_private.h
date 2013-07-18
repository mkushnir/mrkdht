#ifndef MRKDHT_PRIVATE_H_DEFINED
#define MRKDHT_PRIVATE_H_DEFINED

#include <stdint.h>

#include <mrkcommon/list.h>
#include <mrkrpc.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _mrkdht_bucket {
    int idx;
    uint64_t last_accessed;
    list_t nodes;
} mrkdht_bucket_t;

typedef struct _mrkdht_ctx {
    mrkrpc_ctx_t rpc;

} mrkdht_ctx_t;

#ifdef __cplusplus
}

#include <mrkdht.h>
#endif
#endif /* MRKDHT_PRIVATE_H_DEFINED */
