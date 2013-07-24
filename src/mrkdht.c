#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <strings.h>
#include <netinet/in.h>
/* getraddrinfo */
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <mrkcommon/dumpm.h>
#include <mrkcommon/array.h>
#include <mrkcommon/trie.h>
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkdht);
#include <mrkcommon/util.h>
#include <mrkrpc.h>
#include <mrkdata.h>
#include <mrkthr.h>

#include "mrkdht_private.h"
#include "diag.h"

#define MRKDHT_MFLAG_INITIALIZED 0x01
#define MRKDHT_MFLAG_SHUTDOWN 0x02
static unsigned mflags = 0;

/* ctx */

static array_t buckets;
static trie_t nodes;
#define MRKDHT_IDLEN_BITS (sizeof(mrkdht_nid_t) * 8)
#define MRKDHT_BUCKET_MAX 4
#define MRKDHT_ALPHA 3

static mrkrpc_ctx_t rpc;
static mrkdht_node_t me;

static mrkdata_spec_t *node_spec;
#define MRKDHT_NODE_FIELD_NID 0
#define MRKDHT_NODE_FIELD_ADDR 1
#define MRKDHT_NODE_FIELD_PORT 2

static mrkdata_spec_t *node_list_spec;

static mrkdata_spec_t *value_spec;

/* rpc ops */
#define MRKDHT_MSG_PING 0x01
#define MRKDHT_MSG_PONG 0x02
#define MRKDHT_FIND_NODES_REQ 0x03
#define MRKDHT_FIND_NODE_RESP 0x04

static mrkdht_bucket_t *buckets_get_bucket(mrkdht_nid_t);
static void bucket_remove_node(mrkdht_bucket_t *, mrkdht_node_t *);
static void bucket_update_node(mrkdht_bucket_t *, mrkdht_node_t *);
static int bucket_add_node(mrkdht_bucket_t *, mrkdht_node_t *);
static int mrkdht_ping_node(mrkrpc_node_t *);


/* util */

UNUSED static int
distance_to_bucket_id(mrkdht_nid_t distance)
{
    /* XXX we know that this is uint64_t */
    return flsll(distance) - 1;
}


static mrkdht_nid_t
distance(mrkdht_nid_t a, mrkdht_nid_t b)
{
    /* XXX we know that this is uint64_t */
    return a ^ b;
}


UNUSED static int
null_initializer(void **o)
{
    *o = NULL;
    return 0;
}

static int
monitor(UNUSED int argc, UNUSED void **argv)
{
    while (!(mflags & MRKDHT_MFLAG_SHUTDOWN)) {
        size_t sleepq_volume, sleepq_length;
        size_t pending_volume, pending_length;

        mrkthr_sleep(2000);

        sleepq_volume = mrkthr_get_sleepq_volume();
        sleepq_length = mrkthr_get_sleepq_length();
        pending_volume = mrkrpc_ctx_get_pending_volume(&rpc);
        pending_length = mrkrpc_ctx_get_pending_length(&rpc);
        //CTRACE("sleepq: %ld/%ld pending: %ld/%ld",
        //       sleepq_volume,
        //       sleepq_length,
        //       pending_volume,
        //       pending_length);
    }

    mrkrpc_shutdown();
    mrkrpc_ctx_fini(&rpc);
    CTRACE("exiting monitor ...");

    return 0;
}


/* node */

static int
node_init(mrkdht_node_t *node)
{
    mrkrpc_node_init(&node->rpc_node);
    node->distance = 0;
    node->last_seen = 0;
    DTQUEUE_ENTRY_INIT(link, node);
    return 0;
}


static int
node_fini(mrkdht_node_t *node)
{
    mrkrpc_node_fini(&node->rpc_node);
    node->distance = 0;
    node->last_seen = 0;
    DTQUEUE_ENTRY_FINI(link, node);
    return 0;
}

int
mrkdht_make_node_from_params(mrkdht_nid_t nid,
                             const char *hostname,
                             int port)
{
    int res;
    struct addrinfo hints, *ai = NULL, *pai;
    char portstr[32];

    memset(&hints, '\0', sizeof(struct addrinfo));
    hints.ai_family = rpc.family;
    hints.ai_socktype = rpc.socktype;
    hints.ai_protocol = rpc.protocol;
    snprintf(portstr, sizeof(portstr), "%d", port);

    if (getaddrinfo(hostname, portstr, &hints, &ai) != 0) {
        TRRET(MRKDHT_MAKE_NODE_FROM_PARAMS + 1);
    }

    for (pai = ai;
         pai != NULL;
         pai = pai->ai_next) {

        if ((res = mrkdht_make_node_from_addr(nid,
                                              pai->ai_addr,
                                              pai->ai_addrlen)) == 0) {
            break;
        }
    }

    if (ai != NULL) {
        freeaddrinfo(ai);
    }

    return res;
}


int
mrkdht_make_node_from_addr(mrkdht_nid_t nid,
                           struct sockaddr *addr,
                           socklen_t addrlen)
{
    mrkdht_node_t *node = NULL;
    trie_node_t *trn;
    mrkdht_bucket_t *bucket;

    if ((trn = trie_find_exact(&nodes, nid)) != NULL) {
        mrkrpc_node_t *tmp;

        /* XXX crit section around trn and trn->value */
        assert(trn->value != NULL);

        node = trn->value;

        assert(node->rpc_node.nid == nid);

        if ((bucket = buckets_get_bucket(node->distance)) == NULL) {
            TRRET(MRKDHT_MAKE_NODE_FROM_ADDR + 1);
        }

        if ((tmp = mrkrpc_make_node_from_addr(nid,
                                              addr,
                                              addrlen)) != NULL) {

            /*
             * XXX if node's hostname/port is not the same, update and check
             * liveness
             */
            if (!mrkrpc_nodes_equal(&node->rpc_node, tmp)) {
                mrkrpc_node_fini(&node->rpc_node);
                mrkrpc_node_copy(&node->rpc_node, tmp);
                bucket_update_node(bucket, node);
            }

            mrkrpc_node_destroy(&tmp);

        } else {
            /*
             * An attempt to make a node from invalid params, while there
             * is such a node (nid) with different params, forget about
             * this attempt.
             */
            TRRET(MRKDHT_MAKE_NODE_FROM_ADDR + 2);
        }

    } else {
        if ((node = malloc(sizeof(mrkdht_node_t))) == NULL) {
            FAIL("malloc");
        }

        node_init(node);

        node->distance = distance(me.rpc_node.nid, nid);

        if ((bucket = buckets_get_bucket(node->distance)) == NULL) {
            node_fini(node);
            free(node);
            TRRET(MRKDHT_MAKE_NODE_FROM_ADDR + 4);
        }

        if (mrkrpc_node_init_from_addr(&node->rpc_node,
                                       nid,
                                       addr,
                                       addrlen) != 0) {
            node_fini(node);
            free(node);
            TRRET(MRKDHT_MAKE_NODE_FROM_ADDR + 5);

        } else {

            if (bucket_add_node(bucket, node) != 0) {
                node_fini(node);
                free(node);
                TRRET(MRKDHT_MAKE_NODE_FROM_ADDR + 6);
            }

            if ((trn = trie_add_node(&nodes, nid)) == NULL) {
                FAIL("trie_add_node");
            }
            trn->value = node;
        }
    }
    TRRET(0);
}

static int
node_destroy(trie_node_t *trn, UNUSED void *udata)
{
    mrkdht_node_t *node;

    node = trn->value;

    if (node != NULL) {
        node_fini(node);
        free(node);
    }
    return 0;
}


static int
node_dump(mrkdht_node_t **node, UNUSED void *udata)
{
    return mrkrpc_node_dump(&(*node)->rpc_node);
}


/* bucket */

static int
bucket_init(mrkdht_bucket_t *bucket)
{
    bucket->last_accessed = 0;
    DTQUEUE_INIT(&bucket->nodes);
    return 0;
}


static int
bucket_fini(mrkdht_bucket_t *bucket)
{
    DTQUEUE_FINI(&bucket->nodes);
    bucket->last_accessed = 0;
    return 0;
}

static int
bucket_dump(mrkdht_bucket_t *bucket, void *udata)
{
    mrkdht_node_t *node;

    for (node = DTQUEUE_HEAD(&bucket->nodes);
         node != NULL;
         node = DTQUEUE_NEXT(link, node)) {

        node_dump(&node, udata);
    }
    return 0;
}


UNUSED static void
buckets_dump(void)
{
    array_traverse(&buckets,
                   (array_traverser_t)bucket_dump,
                   NULL);
}

static mrkdht_bucket_t *
buckets_get_bucket(mrkdht_nid_t distance)
{
    return (mrkdht_bucket_t *)array_get(&buckets,
                                        distance_to_bucket_id(distance));
}

static void
bucket_remove_node(mrkdht_bucket_t *bucket, mrkdht_node_t *node)
{
    assert(bucket->id == distance_to_bucket_id(node->distance));
    assert(!DTQUEUE_ORPHAN(&bucket->nodes, link, node));

    DTQUEUE_REMOVE(&bucket->nodes, link, node);
}


static void
bucket_update_node(mrkdht_bucket_t *bucket, mrkdht_node_t *node)
{
    assert(bucket->id == distance_to_bucket_id(node->distance));
    assert(!DTQUEUE_ORPHAN(&bucket->nodes, link, node));

    DTQUEUE_REMOVE(&bucket->nodes, link, node);
    node->last_seen = mrkthr_get_now();
    DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
}


static int
bucket_add_node(mrkdht_bucket_t *bucket, mrkdht_node_t *node)
{
    assert(bucket->id == distance_to_bucket_id(node->distance));
    assert(DTQUEUE_ORPHAN(&bucket->nodes, link, node));

    if (DTQUEUE_LENGTH(&bucket->nodes) >= MRKDHT_BUCKET_MAX) {
        mrkdht_node_t *oldest;

        oldest = DTQUEUE_HEAD(&bucket->nodes);

        /* check if the oldest is alive */
        if (mrkdht_ping_node(&oldest->rpc_node) == 0) {
            /* yes, make it the newest one */
            bucket_update_node(bucket, oldest);
            TRRET(BUCKET_ADD_NODE + 1);
        } else {
            /* no, get rid of oldest, and welcome the node */
            bucket_remove_node(bucket, oldest);
            node->last_seen = mrkthr_get_now();
            DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
        }

    } else {
        node->last_seen = mrkthr_get_now();
        DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
    }
    TRRET(0);
}


/* operation */

/* rpc ops */

/* ping */

static int
msg_ping_req_handler(UNUSED mrkrpc_ctx_t *ctx,
                     mrkrpc_queue_entry_t *qe)
{
    int res = 0;

    //TRACE("recvop=%02x -> %02x", qe->recvop, MRKDHT_MSG_PONG);
    if (qe->recvop != MRKDHT_MSG_PING) {
        return 123;
    }
    qe->sendop = MRKDHT_MSG_PONG;
    /*
     * Update this node in our table of nodes.
     */
    if (mrkdht_make_node_from_addr(qe->peer->nid,
                                   qe->peer->addr,
                                   qe->peer->addrlen) != 0) {
        TRACE("mrkdht_make_node_from_addr");
    }
    /* simulate delay */
    res = mrkthr_sleep(197);
    return res;
}

static int
msg_ping_resp_handler(UNUSED mrkrpc_ctx_t *ctx,
                      mrkrpc_queue_entry_t *qe)
{
    //TRACE("recvop=%02x", qe->recvop);
    if (qe->recvop != MRKDHT_MSG_PONG) {
        return 234;
    }
    return 0;
}

static int
msg_pong_req_handler(UNUSED mrkrpc_ctx_t *ctx,
                     UNUSED mrkrpc_queue_entry_t *qe)
{
    /* error ever */
    //TRACE("ERR");
    return 345;
}

static int
msg_pong_resp_handler(UNUSED mrkrpc_ctx_t *ctx,
                      UNUSED mrkrpc_queue_entry_t *qe)
{
    /* OK ever */
    //TRACE("OK");
    return 0;
}

/**/

static int
rpc_server(UNUSED int argc, UNUSED void *argv[])
{
    mrkrpc_run(&rpc);
    mrkrpc_serve(&rpc);
    return 0;
}

void
mrkdht_set_me(mrkdht_nid_t nid, const char *hostname, int port)
{
    if (mrkrpc_ctx_init(&rpc) != 0) {
        FAIL("mrkrpc_ini");
    }

    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_PING,
                       NULL,
                       msg_ping_req_handler,
                       NULL,
                       msg_ping_resp_handler);

    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_PONG,
                       NULL,
                       msg_pong_req_handler,
                       NULL,
                       msg_pong_resp_handler);

    mrkrpc_ctx_set_call_timeout(&rpc, 200);
    mrkrpc_ctx_set_me(&rpc, nid, hostname, port);
    node_init(&me);
    mrkrpc_node_copy(&me.rpc_node, &rpc.me);
}


int
mrkdht_run(void)
{
    mrkthr_spawn("rpc_server", rpc_server, 0);
    mrkthr_spawn("monitor", monitor, 0);
    return 0;
}

static int
mrkdht_ping_node(mrkrpc_node_t *node)
{
    int res;
    mrkdata_datum_t *rv = NULL;

    res = mrkrpc_call(&rpc, node, MRKDHT_MSG_PING, NULL, &rv);
    //TRACE("res=%d rv=%p", res, rv);
    if (rv != NULL) {
        mrkdata_datum_dump(rv);
    }
    mrkdata_datum_destroy(&rv);
    return res;
}


int
mrkdht_ping(mrkdht_nid_t nid)
{
    trie_node_t *trn;
    mrkdht_node_t *node;

    if ((trn = trie_find_exact(&nodes, nid)) == NULL) {
        TRRET(MRKDHT_PING + 1);
    }
    assert(trn->value != NULL);
    node = trn->value;

    return mrkdht_ping_node(&node->rpc_node);
}


/* module */

void
mrkdht_shutdown(void)
{
    mflags |= MRKDHT_MFLAG_SHUTDOWN;
}


void
mrkdht_init(void)
{
    if (mflags & MRKDHT_MFLAG_INITIALIZED) {
        return;
    }

    MEMDEBUG_REGISTER(mrkdht);

    mrkdata_init();

    if ((node_spec = mrkdata_make_spec(MRKDATA_STRUCT)) == NULL) {
        FAIL("mrkdata_make_spec()");
    }
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_UINT64));
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_STR8));
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_UINT16));

    if ((node_list_spec = mrkdata_make_spec(MRKDATA_SEQ)) == NULL) {
        FAIL("mrkdata_make_spec()");
    }
    mrkdata_spec_add_field(node_list_spec, node_spec);

    if ((value_spec = mrkdata_make_spec(MRKDATA_STR64)) == NULL) {
        FAIL("mrkdata_make_spec()");
    }

    mrkrpc_init();

    if (array_init(&buckets, sizeof(mrkdht_bucket_t *), MRKDHT_IDLEN_BITS,
                    (array_initializer_t)bucket_init,
                    (array_finalizer_t)bucket_fini) != 0) {
        FAIL("list_init");
    }

    trie_init(&nodes);

    mflags |= MRKDHT_MFLAG_INITIALIZED;
}

void
mrkdht_fini(void)
{
    if (!(mflags & MRKDHT_MFLAG_INITIALIZED)) {
        return;
    }

    trie_traverse(&nodes, node_destroy, NULL);
    trie_fini(&nodes);

    array_fini(&buckets);

    mrkrpc_fini();

    node_fini(&me);

    mrkdata_fini();

    mflags &= ~MRKDHT_MFLAG_INITIALIZED;
}

