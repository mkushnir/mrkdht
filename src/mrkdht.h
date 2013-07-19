#ifndef MRKDHT_H_DEFINED
#define MRKDHT_H_DEFINED

#ifdef __cplusplus
extern "C" {
#endif

#ifndef MRKDHT_NODE_T_DEFINED
typedef struct _mrkdht_node mrkdht_node_t;
#define MRKDHT_NODE_T_DEFINED
#endif

void mrkdht_init(void);
void mrkdht_shutdown(void);
void mrkdht_fini(void);
void mrkdht_set_me(uint64_t, const char *, int);
int mrkdht_run(void);
mrkdht_node_t *mrkdht_make_node(uint64_t, const char *, int);
int mrkdht_node_destroy(mrkdht_node_t **);


int mrkdht_ping(mrkdht_node_t *);

#ifdef __cplusplus
}
#endif
#endif /* MRKDHT_H_DEFINED */
