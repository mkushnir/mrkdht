#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <netinet/in.h>
/* getraddrinfo */
#include <sys/types.h>
#include <sys/socket.h>
/* inet_ntop(), ... */
#include <arpa/inet.h>
#include <netdb.h>

//#define TRRET_DEBUG
#include <mrkcommon/dumpm.h>
#include <mrkcommon/array.h>
#include <mrkcommon/list.h>
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

static mrkrpc_ctx_t rpc;
static mrkdht_node_t me;

static mrkdata_spec_t *node_spec;
#define MRKDHT_NODE_FIELD_NID 0
#define MRKDHT_NODE_FIELD_ADDR 1
#define MRKDHT_NODE_FIELD_PORT 2

static mrkdata_spec_t *nid_spec;

static mrkdata_spec_t *node_list_spec;

static mrkdata_spec_t *value_spec;

/* rpc ops */
#define MRKDHT_MSG_PING 0x01
#define MRKDHT_MSG_PONG 0x02
#define MRKDHT_MSG_FIND_NODES_CALL 0x03
#define MRKDHT_MSG_FIND_NODES_RET 0x04

static mrkdht_bucket_t *buckets_get_bucket(mrkdht_nid_t);
static void bucket_remove_node(mrkdht_bucket_t *, mrkdht_node_t *);
static void bucket_update_node(mrkdht_bucket_t *, mrkdht_node_t *);
static int bucket_add_node(mrkdht_bucket_t *, mrkdht_node_t *);
static int ping_node(mrkrpc_node_t *);
static int register_node_from_addr(mrkdht_nid_t,
                                   struct sockaddr *,
                                   socklen_t,
                                   mrkdht_node_t **);


/* util */

static int
distance_to_bucket_id(mrkdht_nid_t distance)
{
    /* XXX we know that this is uint64_t */
    return flsll(distance);
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
    node->rtt = 0;
    DTQUEUE_ENTRY_INIT(link, node);
    return 0;
}


static int
node_fini(mrkdht_node_t *node)
{
    mrkrpc_node_fini(&node->rpc_node);
    node->distance = 0;
    node->last_seen = 0;
    node->rtt = 0;
    DTQUEUE_ENTRY_FINI(link, node);
    return 0;
}

static int
register_node_from_params(mrkdht_nid_t nid,
                          const char *hostname,
                          int port,
                          mrkdht_node_t **rnode)
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
        TRRET(REGISTER_NODE_FROM_PARAMS + 1);
    }

    for (pai = ai;
         pai != NULL;
         pai = pai->ai_next) {

        if ((res = register_node_from_addr(nid,
                                           pai->ai_addr,
                                           pai->ai_addrlen,
                                           rnode)) == 0) {
            break;
        }
    }

    if (ai != NULL) {
        freeaddrinfo(ai);
    }

    return res;
}


static int
register_node_from_addr(mrkdht_nid_t nid,
                        struct sockaddr *addr,
                        socklen_t addrlen,
                        mrkdht_node_t **rnode)
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
            TRRET(REGISTER_NODE_FROM_ADDR + 1);
        }

        if ((tmp = mrkrpc_make_node_from_addr(nid,
                                              addr,
                                              addrlen)) != NULL) {

            /*
             * basic node compare
             */
            if (!mrkrpc_nodes_equal(&node->rpc_node, tmp)) {
                mrkrpc_node_fini(&node->rpc_node);
                mrkrpc_node_copy(&node->rpc_node, tmp);
                bucket_update_node(bucket, node);
            }

            /*
             * XXX if node's hostname/port is not the same, update and check
             * XXX liveness
             */

            /*
             *
             */
            mrkrpc_node_destroy(&tmp);

        } else {
            /*
             * An attempt to make a node from invalid params, while there
             * is such a node (nid) with different params, forget about
             * this attempt.
             */
            TRRET(REGISTER_NODE_FROM_ADDR + 2);
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
            TRRET(REGISTER_NODE_FROM_ADDR + 4);
        }

        if (mrkrpc_node_init_from_addr(&node->rpc_node,
                                       nid,
                                       addr,
                                       addrlen) != 0) {
            node_fini(node);
            free(node);
            TRRET(REGISTER_NODE_FROM_ADDR + 5);

        } else {

            if (bucket_add_node(bucket, node) != 0) {
                node_fini(node);
                free(node);
                TRRET(REGISTER_NODE_FROM_ADDR + 6);
            }

            trn = trie_add_node(&nodes, nid);
            trn->value = node;
        }
    }
    if (rnode != NULL) {
        *rnode = node;
    }
    TRRET(0);
}

static int
node_destroy(trie_node_t *trn, UNUSED uint64_t key, UNUSED void *udata)
{
    mrkdht_node_t *node;

    node = trn->value;

    if (node != NULL) {
        node_fini(node);
        free(node);
        trn->value = NULL;
    }
    return 0;
}


static int
forget_node(mrkdht_node_t *node)
{
    trie_node_t *trn;
    mrkdht_bucket_t *bucket;

    if ((bucket = buckets_get_bucket(node->distance)) == NULL) {
        TRRET(FORGET_NODE + 1);
    }
    bucket_remove_node(bucket, node);

    if ((trn = trie_find_exact(&nodes, node->rpc_node.nid)) != NULL) {
        assert(trn->value == node);
        trn->value = NULL;
        trie_remove_node(&nodes, trn);
    }

    TRRET(0);
}


static int
node_dump(mrkdht_node_t **node, UNUSED void *udata)
{
    char buf[1024];
    mrkrpc_node_str(&(*node)->rpc_node, buf, countof(buf));
    CTRACE("<%s rtt=%016lx lseen=%016lx>",
           buf,
           (*node)->rtt,
           (*node)->last_seen);
    return 0;
}


void
mrkdht_dump_node(mrkdht_node_t *node)
{
    node_dump(&node, NULL);
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

    CTRACE("bucket %d la=%016lx:", bucket->id, bucket->last_accessed);
    for (node = DTQUEUE_HEAD(&bucket->nodes);
         node != NULL;
         node = DTQUEUE_NEXT(link, node)) {

        node_dump(&node, udata);
    }
    return 0;
}


void
mrkdht_buckets_dump(void)
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
    DTQUEUE_REMOVE(&bucket->nodes, link, node);
    DTQUEUE_ENTRY_FINI(link, node);
}


static void
bucket_update_node(mrkdht_bucket_t *bucket, mrkdht_node_t *node)
{
    assert(bucket->id == distance_to_bucket_id(node->distance));
    assert(!DTQUEUE_ORPHAN(&bucket->nodes, link, node));

    DTQUEUE_REMOVE(&bucket->nodes, link, node);
    node->last_seen = mrkthr_get_now();
    DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
    bucket->last_accessed = mrkthr_get_now();
}


static int
bucket_add_node(mrkdht_bucket_t *bucket, mrkdht_node_t *node)
{
    //CTRACE("bucket->id=%d did=%d", bucket->id, distance_to_bucket_id(node->distance));
    assert(bucket->id == distance_to_bucket_id(node->distance));
    assert(DTQUEUE_ORPHAN(&bucket->nodes, link, node));

    if (DTQUEUE_LENGTH(&bucket->nodes) >= MRKDHT_BUCKET_MAX) {
        mrkdht_node_t *oldest;

        oldest = DTQUEUE_HEAD(&bucket->nodes);

        /* check if the oldest is alive */
        if (ping_node(&oldest->rpc_node) == 0) {
            /* yes, make it the newest one */
            bucket_update_node(bucket, oldest);

        } else {
            /* no, get rid of oldest, and welcome the node */
            bucket_remove_node(bucket, oldest);
            node->last_seen = mrkthr_get_now();
            DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
            bucket->last_accessed = mrkthr_get_now();
        }

    } else {
        node->last_seen = mrkthr_get_now();
        DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
        bucket->last_accessed = mrkthr_get_now();
    }

    TRRET(0);
}


/*
 * basic find nodes
 */

static size_t
fill_nodes_from_bucket_datum(mrkdht_bucket_t *bucket,
                             size_t sz,
                             mrkdata_datum_t *rv,
                             mrkdht_nid_t exc)
{
    mrkdht_node_t *node;
    size_t i;

    node = DTQUEUE_HEAD(&bucket->nodes);
    for (i = 0; i < sz && node != NULL;) {

        if (node->rpc_node.nid != exc) {
            mrkdata_datum_t *dat;
            struct sockaddr_in *a;
            char buf[32 + INET6_ADDRSTRLEN];
            const char *pbuf;

            /*
             * sin_len, sin_family, and sin_port in struct sockaddr_in are
             * indentical to sin6_len, sin6_family, and sin6_port in
             * struct sockaddr_in6. It is safe to use one set of these two
             * to correctly cover both families.
             */
            a = (struct sockaddr_in *)(node->rpc_node.addr);
            pbuf = inet_ntop(a->sin_family, &a->sin_addr, buf, a->sin_len);

            if (pbuf == NULL) {
                CTRACE("invalid node addr");
                D8(node->rpc_node.addr, node->rpc_node.addrlen);
                continue;
            }

            if ((dat = mrkdata_datum_from_spec(node_spec, NULL, 0)) == NULL) {
                FAIL("mrkdata_datum_from_spec");
            }

            mrkdata_datum_add_field(dat,
                                    mrkdata_datum_make_u64(
                                        node->rpc_node.nid));

            mrkdata_datum_add_field(dat,
                                    mrkdata_datum_make_str8(
                                        buf,
                                        (uint8_t)strlen(buf) + 1));

            mrkdata_datum_add_field(dat,
                                    mrkdata_datum_make_u16(
                                        (uint16_t)ntohs(a->sin_port)));

            mrkdata_datum_add_field(rv, dat);
            ++i;
        }

        node = DTQUEUE_NEXT(link, node);
    }

    return i;
}


static int
find_closest_nodes_datum(mrkdht_nid_t nid,
                         mrkdata_datum_t *rv,
                         mrkdht_nid_t exc)
{
    mrkdht_nid_t dist;
    int bucket_id;
    int incr;
    mrkdht_bucket_t *bucket;
    size_t nadded = 0;

    //CTRACE("will find for %016lx", nid);
    dist = distance(nid, me.rpc_node.nid);

    if (dist == 0) {
        /* nid must be my own */
        TRRET(FIND_CLOSEST_NODES_DATUM + 1);
    }

    bucket_id = distance_to_bucket_id(dist);
    bucket = array_get(&buckets, bucket_id);

    /*
     * We start with the bucket containing the distance(mynid, nid). Until
     * we fill up rv to the required number of nodes, we iterate through
     * the neighbouring buckets below and above that first, widening the
     * circle.
     */

    nadded += fill_nodes_from_bucket_datum(bucket,
                                           MRKDHT_IDLEN_BITS - nadded,
                                           rv,
                                           exc);

    if (nadded >= MRKDHT_BUCKET_MAX) {
        TRRET(0);
    }

    for (incr = 1; incr < (int)MRKDHT_IDLEN_BITS; ++ incr) {
        int bucket_id_incr;

        /* lower */
        bucket_id_incr = bucket_id - incr;

        bucket = array_get(&buckets, bucket_id_incr);

        if (bucket != NULL) {
            nadded += fill_nodes_from_bucket_datum(bucket,
                                                   MRKDHT_IDLEN_BITS - nadded,
                                                   rv,
                                                   exc);

            if (nadded >= MRKDHT_BUCKET_MAX) {
                break;
            }
        }


        /* higher */
        bucket_id_incr = bucket_id + incr;

        bucket = array_get(&buckets, bucket_id_incr);

        if (bucket != NULL) {
            nadded += fill_nodes_from_bucket_datum(bucket,
                                                   MRKDHT_IDLEN_BITS - nadded,
                                                   rv,
                                                   exc);

            if (nadded >= MRKDHT_BUCKET_MAX) {
                break;
            }
        }

    }
    TRRET(0);
}


static size_t
fill_nodes_from_bucket_array(mrkdht_bucket_t *bucket,
                             size_t sz,
                             mrkdht_node_t *rv[],
                             mrkdht_nid_t exc)
{
    mrkdht_node_t *tmp;
    size_t i;

    tmp = DTQUEUE_HEAD(&bucket->nodes);
    for (i = 0; i < sz && tmp != NULL;) {

        if (tmp->rpc_node.nid != exc) {
            rv[i] = tmp;
            ++i;
        }
        tmp = DTQUEUE_NEXT(link, tmp);
    }
    return i;
}
static int
find_closest_nodes_array(mrkdht_nid_t nid,
                         mrkdht_node_t *rv[],
                         size_t *sz,
                         mrkdht_nid_t exc)
{
    mrkdht_nid_t dist;
    int bucket_id;
    int incr;
    mrkdht_bucket_t *bucket;
    size_t nadded = 0;

    dist = distance(nid, me.rpc_node.nid);

    //if (dist == 0) {
    //    /* nid must be my own */
    //    *sz = 0;
    //    TRRET(FIND_CLOSEST_NODES_ARRAY + 1);
    //}

    bucket_id = distance_to_bucket_id(dist);
    bucket = array_get(&buckets, bucket_id);

    //CTRACE("dist=%016lx id=%d b=%p", dist, bucket_id, bucket);

    /*
     * We start with the bucket containing the distance(mynid, nid). Until
     * we fill up rv to the required number of nodes, we iterate through
     * the neighbouring buckets below and above that first, widening the
     * circle.
     */

    nadded += fill_nodes_from_bucket_array(bucket,
                                           (*sz) - nadded,
                                           rv + nadded,
                                           exc);
    //CTRACE("nadded=%ld", nadded);

    if (nadded >= *sz) {
        TRRET(0);
    }

    for (incr = 1; incr < (int)MRKDHT_IDLEN_BITS; ++ incr) {
        int bucket_id_incr;

        /* lower */
        bucket_id_incr = bucket_id - incr;

        bucket = array_get(&buckets, bucket_id_incr);

        if (bucket != NULL) {
            nadded += fill_nodes_from_bucket_array(bucket,
                                                   (*sz) - nadded,
                                                   rv + nadded,
                                                   exc);
            //CTRACE("nadded=%ld", nadded);

            if (nadded >= *sz) {
                break;
            }
        }


        /* higher */
        bucket_id_incr = bucket_id + incr;

        bucket = array_get(&buckets, bucket_id_incr);

        if (bucket != NULL) {
            nadded += fill_nodes_from_bucket_array(bucket,
                                                   (*sz) - nadded,
                                                   rv + nadded,
                                                   exc);

            //CTRACE("nadded=%ld", nadded);

            if (nadded >= *sz) {
                break;
            }
        }

    }
    *sz = nadded;
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
    if (register_node_from_addr(qe->peer->nid,
                                qe->peer->addr,
                                qe->peer->addrlen,
                                NULL) != 0) {
        TRACE("register_node_from_addr failed");
    }
    /* simulate delay */
    //res = mrkthr_sleep(199);
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
                      mrkrpc_queue_entry_t *qe)
{
    /* OK ever */
    //TRACE("OK");
    if (register_node_from_addr(qe->peer->nid,
                                qe->peer->addr,
                                qe->peer->addrlen,
                                NULL) != 0) {
        TRACE("register_node_from_addr");
        return 456;
    }
    return 0;
}

/* find nodes */

static int
msg_find_nodes_call_req_handler(UNUSED mrkrpc_ctx_t *ctx,
                                mrkrpc_queue_entry_t *qe)
{
    int res = 0;

    //TRACE("recvop=%02x -> %02x", qe->recvop, MRKDHT_MSG_FIND_NODES_CALL);
    if (qe->recvop != MRKDHT_MSG_FIND_NODES_CALL) {
        return 123;
    }

    /* set up send things */
    qe->sendop = MRKDHT_MSG_FIND_NODES_RET;
    qe->senddat = mrkdata_datum_from_spec(node_list_spec, NULL, 0);
    if (qe->recvdat == NULL) {
        return 1230;
    }

    /* qe->recvdat is UINT64 */
    res = find_closest_nodes_datum(qe->recvdat->value.u64, qe->senddat,
                                   qe->peer->nid);

    //CTRACE("find_closest_nodes_datum res=%s", diag_str(res));

    /*
     * Update this node in our table of nodes.
     */
    if (register_node_from_addr(qe->peer->nid,
                                qe->peer->addr,
                                qe->peer->addrlen,
                                NULL) != 0) {
        TRACE("register_node_from_addr");
    }

    /*
     * clean up recv things
     */
    mrkdata_datum_destroy(&qe->recvdat);

    return res;
}

static int
msg_find_nodes_call_resp_handler(UNUSED mrkrpc_ctx_t *ctx,
                                 mrkrpc_queue_entry_t *qe)
{
    //TRACE("recvop=%02x", qe->recvop);
    if (qe->recvop != MRKDHT_MSG_FIND_NODES_RET) {
        return 234;
    }
    return 0;
}

static int
msg_find_nodes_ret_req_handler(UNUSED mrkrpc_ctx_t *ctx,
                               UNUSED mrkrpc_queue_entry_t *qe)
{
    /* error ever */
    //TRACE("ERR");
    return 345;
}

static int
msg_find_nodes_ret_resp_handler(UNUSED mrkrpc_ctx_t *ctx,
                                mrkrpc_queue_entry_t *qe)
{
    /* OK ever */
    //TRACE("OK");
    if (register_node_from_addr(qe->peer->nid,
                                qe->peer->addr,
                                qe->peer->addrlen,
                                NULL) != 0) {
        TRACE("register_node_from_addr");
        return 456;
    }
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

    /* ping, pong */
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

    /* find_nodes_call, find_nodes_ret */
    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_FIND_NODES_CALL,
                       nid_spec,
                       msg_find_nodes_call_req_handler,
                       NULL,
                       msg_find_nodes_call_resp_handler);

    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_FIND_NODES_RET,
                       NULL,
                       msg_find_nodes_ret_req_handler,
                       node_list_spec,
                       msg_find_nodes_ret_resp_handler);

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
ping_node(mrkrpc_node_t *node)
{
    int res;
    mrkdata_datum_t *rv = NULL;
    res = mrkrpc_call(&rpc, node, MRKDHT_MSG_PING, NULL, &rv);
    //TRACE("res=%d rv=%p", res, rv);
    //if (rv != NULL) {
    //    mrkdata_datum_dump(rv);
    //}
    mrkdata_datum_destroy(&rv);
    return res;
}


int
mrkdht_ping(mrkdht_nid_t nid)
{
    trie_node_t *trn;
    mrkdht_node_t *node;
    uint64_t before;
    int res;

    if ((trn = trie_find_exact(&nodes, nid)) == NULL) {
        TRRET(MRKDHT_PING + 1);
    }
    assert(trn->value != NULL);
    node = trn->value;

    before = mrkthr_get_now_ticks();
    res = ping_node(&node->rpc_node);
    node->rtt = mrkthr_get_now_ticks() - before;
    return res;
}


int
mrkdht_test_find_closest_nodes(mrkdht_nid_t nid, size_t sz)
{
    mrkdht_node_t *nodes[sz];
    size_t i;
    int res;


    memset(nodes, '\0', sizeof(nodes));

    res = find_closest_nodes_array(nid, nodes, &sz, 0);
    CTRACE("res=%d sz=%ld", res, sz);

    for (i = 0; i < sz; ++i) {
        node_dump(&nodes[i], NULL);
    }
    return 0;
}


static void
fill_shortlist(mrkdht_node_t *node,
               trie_t *shortlist,
               trie_t *contacted,
               mrkdht_nid_t nid)
{
    int res;
    trie_node_t *ctrn;
    trie_node_t *strn;

    if ((ctrn = trie_find_exact(contacted,
                                node->rpc_node.nid)) == NULL) {

        if (nid == node->rpc_node.nid) {
            uint64_t before;
            int res;

            /*
             * just ping it.
             * XXX think of not pinging in case it's not expired
             */
            before = mrkthr_get_now_ticks();
            res = ping_node(&node->rpc_node);
            node->rtt = mrkthr_get_now_ticks() - before;

            if (res == 0) {
                mrkdht_nid_t dist;

                /* add the active contact to shortlist and to contacted */
                dist = distance(nid, node->rpc_node.nid);
                strn = trie_add_node(shortlist, dist);

                if (strn->value == NULL) {
                    /* first seen */
                    strn->value = node;
                    //CTRACE("shortlist: contact added first time "
                    //       "dist=%016lx nid=%016lx",
                    //       dist,
                    //       node->rpc_node.nid);
                } else {
                    //CTRACE("shortlist: contact already seen "
                    //       "dist=%016lx nid=%016lx",
                    //       dist,
                    //       node->rpc_node.nid);
                }

                ctrn = trie_add_node(contacted, node->rpc_node.nid);
                ctrn->value = node;

            } else {
                CTRACE("RPC to %016lx failed", node->rpc_node.nid);
                if ((strn = trie_find_exact(shortlist, node->rpc_node.nid)) != NULL) {
                    trie_remove_node(shortlist, strn);
                }
                if ((ctrn = trie_find_exact(contacted, node->rpc_node.nid)) != NULL) {
                    trie_remove_node(contacted, ctrn);
                }
                forget_node(node);
            }


        } else {
            mrkdata_datum_t *arg;
            mrkdata_datum_t *rv;

            arg = mrkdata_datum_make_u64(nid);
            rv = NULL;

            res = mrkrpc_call(&rpc,
                              &node->rpc_node,
                              MRKDHT_MSG_FIND_NODES_CALL,
                              arg,
                              &rv);

            mrkdata_datum_destroy(&arg);

            if (rv != NULL) {
                mrkdht_nid_t dist;
                mrkdata_datum_t **dat;
                list_iter_t it;

                /* add the active contact to shortlist and to contacted */
                dist = distance(nid, node->rpc_node.nid);
                strn = trie_add_node(shortlist, dist);

                if (strn->value == NULL) {
                    /* first seen */
                    strn->value = node;
                    //CTRACE("shortlist: contact added first time "
                    //       "dist=%016lx nid=%016lx",
                    //       dist,
                    //       node->rpc_node.nid);
                } else {
                    //CTRACE("shortlist: contact already seen "
                    //       "dist=%016lx nid=%016lx",
                    //       dist,
                    //       node->rpc_node.nid);
                }

                ctrn = trie_add_node(contacted, node->rpc_node.nid);
                ctrn->value = node;


                //mrkdata_datum_dump(rv);

                for (dat = list_first(&rv->data.fields, &it);
                     dat != NULL;
                     dat = list_next(&rv->data.fields, &it)) {

                    mrkdata_datum_t **fdat;
                    mrkdht_nid_t rnid;
                    char *addr;
                    int port;

                    if ((fdat = list_get(&(*dat)->data.fields,
                                         MRKDHT_NODE_FIELD_NID)) == NULL) {
                        CTRACE("invalid datum");
                        mrkdata_datum_dump(*dat);
                    }
                    rnid = (*fdat)->value.u64;
                    if ((fdat = list_get(&(*dat)->data.fields,
                                         MRKDHT_NODE_FIELD_ADDR)) == NULL) {
                        CTRACE("invalid datum");
                        mrkdata_datum_dump(*dat);
                    }
                    addr = (*fdat)->data.str;
                    if ((fdat = list_get(&(*dat)->data.fields,
                                         MRKDHT_NODE_FIELD_PORT)) == NULL) {
                        CTRACE("invalid datum");
                        mrkdata_datum_dump(*dat);
                    }
                    port = (*fdat)->value.u16;

                    /*
                     * first check out with shortlist indexed by dist to
                     * nid
                     */
                    dist = distance(nid, rnid);
                    strn = trie_add_node(shortlist, dist);

                    if (strn->value == NULL) {
                        mrkdht_node_t *node;

                        /*
                         * most likely not yet registered ...
                         */

                        node = NULL;
                        //CTRACE("shortlist: not found, "
                        //       "will try to register %016lx,%s,%d",
                        //       rnid, addr, port);

                        if (register_node_from_params(rnid,
                                                      addr,
                                                      port,
                                                      &node) != 0) {
                            CTRACE("register_node_from_params failed ...");

                        } else {
                            strn->value = node;
                        }

                    } else {
                        /* registered, check it */
                        //CTRACE("shortlist: already registered %016lx,%s,%d",
                        //       rnid, addr, port);
                    }
                }

            } else {
                /* remove this node from nodes and buckets */
                CTRACE("RPC to %016lx failed", node->rpc_node.nid);
                if ((strn = trie_find_exact(shortlist, node->rpc_node.nid)) != NULL) {
                    trie_remove_node(shortlist, strn);
                }
                if ((ctrn = trie_find_exact(contacted, node->rpc_node.nid)) != NULL) {
                    trie_remove_node(contacted, ctrn);
                }
                forget_node(node);
            }

            mrkdata_datum_destroy(&rv);
        }

    } else {
        //CTRACE("node %016lx already contacted", node->rpc_node.nid);
    }
}

static int
select_alpha(trie_node_t *trn, UNUSED uint64_t key, void *udata)
{
    struct {
        mrkdht_node_t **alpha;
        int i;
        trie_t *contacted;
    } *params = udata;
    mrkdht_node_t *node;
    trie_node_t *ctrn;

    if (trn->value == NULL) {
        return 0;
    }

    if (params->i >= 3) {
        return 1;
    }

    node = trn->value;

    ctrn = trie_find_exact(params->contacted, node->rpc_node.nid);
    if (ctrn == NULL) {
        params->alpha[params->i] = node;
        //CTRACE("selected for alpha %016lx", node->rpc_node.nid);
        ++(params->i);
    } else {
        //CTRACE("cannot select for alpha %016lx", node->rpc_node.nid);
    }


    return 0;
}

static int
select_result(trie_node_t *trn, UNUSED uint64_t key, void *udata)
{
    mrkdht_node_t *node;
    struct {
        mrkdht_node_t **nodes;
        size_t i;
        size_t sz;
    } *params = udata;

    if (trn->value == NULL) {
        return 0;
    }

    if (params->i >= params->sz) {
        return 1;
    }

    node = trn->value;
    params->nodes[params->i] = node;
    ++(params->i);

    return 0;
}

int
mrkdht_lookup_nodes(mrkdht_nid_t nid, mrkdht_node_t **rnodes, size_t *rsz)
{
    mrkdht_node_t *alpha[MRKDHT_ALPHA];
    mrkdht_node_t *closest_node;
    size_t i;
    int res;
    size_t sz;
    trie_t shortlist;
    trie_t contacted;
    struct {
        mrkdht_node_t **nodes;
        size_t i;
        size_t sz;
    } rparams;

    trie_init(&shortlist);
    trie_init(&contacted);

    /*
     * initial alpha
     */
    sz = MRKDHT_ALPHA;
    memset(alpha, '\0', sizeof(alpha));
    res = find_closest_nodes_array(nid, alpha, &sz, 0);

    while (!(mflags & MRKDHT_MFLAG_SHUTDOWN)) {
        trie_node_t *trn;
        struct {
            mrkdht_node_t **alpha;
            int i;
            trie_t *contacted;
        } params;

        for (i = 0; i < sz; ++i) {
            //CTRACE("trying contact from alpha: %016lx",
            //       alpha[i]->rpc_node.nid);
            fill_shortlist(alpha[i], &shortlist, &contacted, nid);
        }

        trn = TRIE_MIN(&shortlist);
        if (trn != NULL) {
            if (trn->value == closest_node) {
                //CTRACE("finished lookup at:");
                //node_dump(&closest_node, NULL);
                break;
            }
            closest_node = trn->value;
        } else {
            break;
        }

        /* select new alpha from the shortlist  */
        memset(alpha, '\0', sizeof(alpha));
        params.alpha = alpha;
        params.i = 0;
        params.contacted = &contacted;
        trie_traverse(&shortlist, select_alpha, &params);
        sz = params.i;

        //CTRACE("selected for alpha: %d", params.i);
    }

    //CTRACE("shortlist:");
    //trie_traverse(&shortlist, trie_node_dump_cb, (void *)1);

    rparams.nodes = rnodes;
    rparams.i = 0;
    rparams.sz = *rsz;
    trie_traverse(&shortlist, select_result, &rparams);
    *rsz = rparams.i;

    //CTRACE("contacted:");
    //trie_traverse(&contacted, trie_node_dump_cb, (void *)1);
    //mrkdht_buckets_dump();

    trie_fini(&shortlist);
    trie_fini(&contacted);

    return 0;
}


int
mrkdht_join(mrkdht_nid_t nid,
            const char *hostname,
            int port,
            unsigned flags)
{
    if ((register_node_from_params(nid, hostname, port, NULL)) != 0) {
        TRRET(MRKDHT_JOIN + 1);
    }

    if (!(flags & MRKDHT_FLAG_JOIN_NOPING)) {
        mrkdht_node_t *nodes[MRKDHT_BUCKET_MAX];
        size_t sz = MRKDHT_BUCKET_MAX;
        int res;
        UNUSED size_t i;

        if ((res = mrkdht_ping(nid)) != 0) {
            TRRET(res);
        }

        memset(nodes, '\0', sizeof(nodes));
        mrkdht_lookup_nodes(me.rpc_node.nid, nodes, &sz);
        //CTRACE("looked up with res=%d sz=%ld", res, sz);
        //for (i = 0; i < sz; ++i) {
        //    mrkdht_dump_node(nodes[i]);
        //}

    }

    TRRET(0);
}

/* module */

void
mrkdht_shutdown(void)
{
    mflags |= MRKDHT_MFLAG_SHUTDOWN;
}


static int
bucket_set_id(mrkdht_bucket_t *bucket, int *id)
{
    bucket->last_accessed = mrkthr_get_now();
    bucket->id = *id;
    ++(*id);
    return 0;
}


void
mrkdht_init(void)
{
    int id;

    if (mflags & MRKDHT_MFLAG_INITIALIZED) {
        return;
    }

    MEMDEBUG_REGISTER(mrkdht);

    mrkdata_init();

    nid_spec = mrkdata_make_spec(MRKDATA_UINT64);

    node_spec = mrkdata_make_spec(MRKDATA_STRUCT);
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_UINT64));
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_STR8));
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_UINT16));

    node_list_spec = mrkdata_make_spec(MRKDATA_SEQ);
    mrkdata_spec_add_field(node_list_spec, node_spec);

    value_spec = mrkdata_make_spec(MRKDATA_STR64);

    mrkrpc_init();

    if (array_init(&buckets, sizeof(mrkdht_bucket_t), MRKDHT_IDLEN_BITS + 1,
                    (array_initializer_t)bucket_init,
                    (array_finalizer_t)bucket_fini) != 0) {
        FAIL("list_init");
    }

    id = 0;
    array_traverse(&buckets, (array_traverser_t)bucket_set_id, &id);

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

