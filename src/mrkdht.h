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

void mrkdht_init(void);
void mrkdht_shutdown(void);
void mrkdht_fini(void);
void mrkdht_set_me(mrkdht_nid_t, const char *, int);
int mrkdht_run(void);
int mrkdht_make_node_from_params(mrkdht_nid_t, const char *, int);
int mrkdht_make_node_from_addr(mrkdht_nid_t, struct sockaddr *, socklen_t);
int mrkdht_node_destroy(mrkdht_node_t **, void *);


int mrkdht_ping(mrkdht_nid_t);

#ifdef __cplusplus
}
#endif
#endif /* MRKDHT_H_DEFINED */
