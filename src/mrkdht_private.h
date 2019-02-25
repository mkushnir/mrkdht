#ifndef MRKDHT_PRIVATE_H_DEFINED
#define MRKDHT_PRIVATE_H_DEFINED

#include <stdint.h>

#include <mrkcommon/dtqueue.h>
#include <mrkrpc.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MRKDHT_RPC_TIMEOUT 5000

typedef mrkrpc_nid_t mrkdht_nid_t;
#define MRKDHT_NID_T_DEFINED

typedef struct _mrkdht_host_info {
    uint64_t rtt;
    uint64_t last_rpc_call;
} mrkdht_host_info_t;

typedef struct _mrkdht_node {
    mrkrpc_node_t rpc_node;
    mrkdht_nid_t distance;
    uint64_t last_seen;
    DTQUEUE_ENTRY(_mrkdht_node, link);
    struct {
        int unresponsive:1;
    } flags;
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

typedef struct _mrkdht_stat_counter {
    uint64_t ntotal;
    uint64_t nfailures;
} mrkdht_stat_counter_t;
#ifdef __cplusplus
}
#endif

#include <mrkdht.h>

#endif /* MRKDHT_PRIVATE_H_DEFINED */
