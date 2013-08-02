#ifndef MRKDHT_H_DEFINED
#define MRKDHT_H_DEFINED

#include <netinet/in.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef MRKDHT_NODE_T_DEFINED
typedef struct _mrkdht_node mrkdht_node_t;
#define MRKDHT_NODE_T_DEFINED
#endif

#ifndef MRKDHT_NID_T_DEFINED
#include <mrkrpc.h>
typedef mrkrpc_nid_t mrkdht_nid_t;
#define MRKDHT_NID_T_DEFINED
#endif

#define MRKDHT_IDLEN_BITS (sizeof(mrkdht_nid_t) * 8)
#define MRKDHT_BUCKET_MAX 4
#define MRKDHT_ALPHA 3

void mrkdht_init(void);
void mrkdht_shutdown(void);
void mrkdht_fini(void);
void mrkdht_buckets_dump(void);
void mrkdht_set_me(mrkdht_nid_t, const char *, int);
void mrkdht_set_refresh(uint64_t);
void mrkdht_dump_node(mrkdht_node_t *);
int mrkdht_run(void);

#define MRKDHT_FLAG_JOIN_NOPING (0x01)
mrkdht_nid_t mrkdht_node_get_nid(mrkdht_node_t *);
int mrkdht_join(mrkdht_nid_t, const char *, int, unsigned);
int mrkdht_ping(mrkdht_nid_t);
int mrkdht_test_find_closest_nodes(mrkdht_nid_t, size_t);
int mrkdht_lookup_nodes(mrkdht_nid_t, mrkdht_node_t **, size_t *);
int
mrkdht_find_closest_nodes(mrkdht_nid_t, mrkdht_node_t **, size_t *);

#define MRKDHT_STATS_PING 0
#define MRKDHT_STATS_PONG 1
#define MRKDHT_STATS_FIND_NODES 2
#define MRKDHT_STATS_ALL 3
void mrkdht_get_stats(unsigned, uint64_t *, uint64_t *);
#ifdef __cplusplus
}
#endif
#endif /* MRKDHT_H_DEFINED */
