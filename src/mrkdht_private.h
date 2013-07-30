#ifndef MRKDHT_PRIVATE_H_DEFINED
#define MRKDHT_PRIVATE_H_DEFINED

#include <stdint.h>

#include <mrkcommon/list.h>
#include <mrkcommon/dtqueue.h>
#include <mrkrpc.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef mrkrpc_nid_t mrkdht_nid_t;
#define MRKDHT_NID_T_DEFINED

typedef struct _mrkdht_node {
    mrkrpc_node_t rpc_node;
    mrkdht_nid_t distance;
    uint64_t last_seen;
    uint64_t rtt;
    DTQUEUE_ENTRY(_mrkdht_node, link);
} mrkdht_node_t;
#define MRKDHT_NODE_T_DEFINED

typedef struct _mrkdht_bucket {
    int id;
    uint64_t last_accessed;
    DTQUEUE(_mrkdht_node, nodes);
} mrkdht_bucket_t;

typedef struct _mrkdht_ctx {
    mrkrpc_ctx_t rpc;

} mrkdht_ctx_t;

#ifdef __cplusplus
}
#endif

#include <mrkdht.h>

#endif /* MRKDHT_PRIVATE_H_DEFINED */
