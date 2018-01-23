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

//#define TRRET_DEBUG_VERBOSE
#include <mrkcommon/dumpm.h>
#include <mrkcommon/array.h>
#include <mrkcommon/btrie.h>
#include <mrkcommon/util.h>
#include <mrkrpc.h>
#include <mrkdata.h>
#include <mrkthr.h>

#include "mrkdht_private.h"
#include "diag.h"

#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(mrkdht);

#define MRKDHT_MFLAG_INITIALIZED 0x01
#define MRKDHT_MFLAG_SHUTDOWN 0x02
static unsigned mflags = 0;

/* ctx */

static mnarray_t buckets;
static mnbtrie_t nodes;

static mrkrpc_ctx_t rpc;
static mrkdht_node_t me;

static uint64_t trefresh;
static mrkthr_ctx_t *refresher_thr;

static mrkthr_ctx_t *monitor_thr;

static mrkdata_spec_t *node_spec;
#define MRKDHT_NODE_FIELD_NID 0
#define MRKDHT_NODE_FIELD_ADDR 1
#define MRKDHT_NODE_FIELD_PORT 2

static mrkdata_spec_t *nid_spec;

static mrkdata_spec_t *node_list_spec;

static mrkdata_spec_t *value_spec;

/* traffic */
static mnbtrie_t host_infos;

/* stats */
static mrkdht_stat_counter_t stats[MRKDHT_STATS_ALL];

/* rpc ops */
#define MRKDHT_MSG_PING 0x01
#define MRKDHT_MSG_PONG 0x02
#define MRKDHT_MSG_FIND_NODES_CALL 0x03
#define MRKDHT_MSG_FIND_NODES_RET 0x04

#define MRKDHT_BUCKET_PUT_NODE_FPING 0x01
#define MRKDHT_REGISTER_NODE_BUCKET_OPTIONAL 0x02

static mrkdht_bucket_t *buckets_get_bucket(mrkdht_nid_t);
static void bucket_remove_node(mrkdht_bucket_t *, mrkdht_node_t *);
static int bucket_put_node(mrkdht_bucket_t *, mrkdht_node_t *, unsigned);
static int ping_node(mrkdht_node_t *);
static int node_dump(mrkdht_node_t **, void *);
static void forget_node_bucket(mrkdht_bucket_t *, mrkdht_node_t *);
static mrkdht_bucket_t *node_get_bucket(mrkdht_node_t *);


/* util */

static int
distance_to_bucket_id(mrkdht_nid_t distance)
{
    /* XXX we know that this is uint64_t */
    return flsll(distance);
}


static mrkdht_nid_t
bucket_id_to_distance(int id)
{
    /* XXX we know that this is uint64_t */
    if (id == 0) {
        return 0ul;
    }
    return 1ul << (id - 1);
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

size_t
mrkdht_get_rpc_pending_volume(void)
{
    return mrkrpc_ctx_get_pending_volume(&rpc);
}

size_t
mrkdht_get_rpc_pending_length(void)
{
    return mrkrpc_ctx_get_pending_length(&rpc);
}

size_t
mrkdht_get_rpc_sendq_length(void)
{
    return mrkrpc_ctx_get_sendq_length(&rpc);
}

static int
monitor(UNUSED int argc, UNUSED void **argv)
{
    while (!(mflags & MRKDHT_MFLAG_SHUTDOWN)) {
        UNUSED size_t sleepq_volume, sleepq_length;
        UNUSED size_t pending_volume, pending_length;

        if (mrkthr_sleep(2000) != 0) {
            break;
        }

        //sleepq_volume = mrkthr_get_sleepq_volume();
        //sleepq_length = mrkthr_get_sleepq_length();
        //pending_volume = mrkrpc_ctx_get_pending_volume(&rpc);
        //pending_length = mrkrpc_ctx_get_pending_length(&rpc);
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

static int
refresher(UNUSED int argc, UNUSED void **argv)
{
    while (!(mflags & MRKDHT_MFLAG_SHUTDOWN)) {
        mrkdht_bucket_t *bucket;
        mnarray_iter_t it;
        uint64_t now, trefresh_nsec;
        mrkdht_node_t *nodes[MRKDHT_BUCKET_MAX];
        size_t sz;

        if (mrkthr_sleep(trefresh) != 0) {
            break;
        }

        now = mrkthr_get_now();
        trefresh_nsec = trefresh * 1000000;


        /* first lookup the closest nodes */
        memset(nodes, '\0', sizeof(nodes));
        sz = MRKDHT_BUCKET_MAX;
        mrkdht_lookup_nodes(me.rpc_node.nid, nodes, &sz);

        for (bucket = array_first(&buckets, &it);
             bucket != NULL;
             bucket = array_next(&buckets, &it)) {

            if ((bucket->last_accessed + trefresh) < now) {
                mrkdht_nid_t dist, lookup_nid;
                UNUSED size_t i;

                if (DTQUEUE_EMPTY(&bucket->nodes)) {
                    //CTRACE("bucket %d is empty, moving forward ...",
                    //       bucket->id);
                    continue;
                }

                dist = bucket_id_to_distance(bucket->id);
                lookup_nid = distance(me.rpc_node.nid, dist);

                //CTRACE("bucket %d is stale, will lookup at dist %016lx for %016lx",
                //       bucket->id, dist, lookup_nid);

                memset(nodes, '\0', sizeof(nodes));
                sz = MRKDHT_BUCKET_MAX;
                mrkdht_lookup_nodes(lookup_nid, nodes, &sz);

                //CTRACE("Lookup returned %ld nodes", sz);
                //for (i = 0; i < sz; ++i) {
                //    node_dump(&nodes[i], NULL);
                //}

                if (sz == 0) {
                    break;
                }

            } else {
                //CTRACE("bucket %d is fresh", bucket->id);
            }
        }
    }

    return 0;
}

/* host info */

static uint64_t
host_key(mrkrpc_node_t *node)
{
    uint64_t key = 0;

    if (node->addr->sa_family == AF_INET) {
        key = ((struct sockaddr_in *)(node->addr))->sin_addr.s_addr;
    } else if (node->addr->sa_family == AF_INET6) {
#       define u6(i) ((((struct sockaddr_in6 *)(node->addr))->sin6_addr).__u6_addr.__u6_addr32[i])
        key = (((uint64_t)(u6(0))) << 32 | ((uint64_t)(u6(1)))) ^
              (((uint64_t)(u6(2))) << 32 | ((uint64_t)(u6(3))));
    } else {
        TRACE("Unknown protocol faimly: %d", node->addr->sa_family);
    }
    return key;
}

static mrkdht_host_info_t *
get_host_info(mrkrpc_node_t *node)
{
    uint64_t key;
    mnbtrie_node_t *trn;
    mrkdht_host_info_t *hi;

    key = host_key(node);

    //CTRACE("key=%016lx", key);

    if ((trn = btrie_add_node(&host_infos, (uintptr_t)key)) == NULL) {
        FAIL("btrie_add_node");
    }

    if (trn->value == NULL) {

        if ((hi = malloc(sizeof(mrkdht_host_info_t))) == NULL) {
            FAIL("malloc");
        }
        hi->rtt = 0;
        hi->last_rpc_call = 0;
        trn->value = hi;
    } else {
        hi = trn->value;
    }

    return hi;
}

static void
save_host_info(mrkrpc_node_t *node, uint64_t before)
{
    mrkdht_host_info_t *hi;

    hi = get_host_info(node);
    hi->last_rpc_call = mrkthr_get_now_ticks();
    hi->rtt = hi->last_rpc_call - before;
    //TRACE("saved for %s:%d", node->hostname, node->port);
}

/* node */

static int
node_init(mrkdht_node_t *node)
{
    mrkrpc_node_init(&node->rpc_node);
    node->distance = 0;
    node->last_seen = 0;
    node->flags.unresponsive = 0;
    DTQUEUE_ENTRY_INIT(link, node);
    return 0;
}


static int
node_fini(mrkdht_node_t *node)
{
    mrkrpc_node_fini(&node->rpc_node);
    node->distance = 0;
    node->last_seen = 0;
    node->flags.unresponsive = 0;
    DTQUEUE_ENTRY_FINI(link, node);
    return 0;
}

static int
register_node_from_addr(mrkdht_nid_t nid,
                        struct sockaddr *addr,
                        socklen_t addrlen,
                        mrkdht_node_t **rnode,
                        unsigned flags)
{
    mrkdht_node_t *node = NULL;
    mnbtrie_node_t *trn;
    mrkdht_bucket_t *bucket;

    if ((trn = btrie_find_exact(&nodes, nid)) != NULL) {
        mrkrpc_node_t *tmp;

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

            }

            mrkrpc_node_destroy(&tmp);


            /*
             * put into the bucket
             */
            if (bucket_put_node(bucket, node, flags) != 0) {
                if (!(flags & MRKDHT_REGISTER_NODE_BUCKET_OPTIONAL)) {
                    forget_node_bucket(bucket, node);
                    TRRET(REGISTER_NODE_FROM_ADDR + 2);
                }
            }

            /*
             * XXX if node's hostname/port is not the same, update and check
             * XXX liveness
             */

            /*
             *
             */
        } else {
            /*
             * An attempt to make a node from invalid params, while there
             * is such a node (nid) with different params, forget about
             * this attempt.
             */
            forget_node_bucket(bucket, node);
            TRRET(REGISTER_NODE_FROM_ADDR + 3);
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

            if (bucket_put_node(bucket, node, flags) != 0) {
                if (!(flags & MRKDHT_REGISTER_NODE_BUCKET_OPTIONAL)) {
                    node_fini(node);
                    free(node);
                    TRRET(REGISTER_NODE_FROM_ADDR + 6);
                }
            }

            trn = btrie_add_node(&nodes, nid);
            trn->value = node;
        }
    }
    if (rnode != NULL) {
        *rnode = node;
    }
    TRRET(0);
}

static int
register_node_from_params(mrkdht_nid_t nid,
                          const char *hostname,
                          int port,
                          mrkdht_node_t **rnode,
                          unsigned flags)
{
    int res = 0;
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
                                           rnode,
                                           flags)) == 0) {
            break;
        } else {
            /* let's take the first one */
            break;
        }
    }

    if (ai != NULL) {
        freeaddrinfo(ai);
    }

    return res;
}


static int
node_destroy(mnbtrie_node_t *trn, UNUSED void *udata)
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


static void
forget_node_bucket(mrkdht_bucket_t *bucket, mrkdht_node_t *node)
{
    bucket_remove_node(bucket, node);
    node->flags.unresponsive = 1;
}


static int
forget_node(mrkdht_node_t *node)
{
    mrkdht_bucket_t *bucket;

    if ((bucket = buckets_get_bucket(node->distance)) == NULL) {
        TRRET(FORGET_NODE + 1);
    }

    forget_node_bucket(bucket, node);

    TRRET(0);
}


static void
revive_node_from_addr(mrkdht_nid_t nid,
                      struct sockaddr *addr,
                      socklen_t addrlen,
                      unsigned flags)
{
    mrkdht_node_t *node = NULL;

    if (register_node_from_addr(nid,
                                addr,
                                addrlen,
                                &node,
                                flags) != 0) {
        //CTRACE("register_node_from_addr %016lx failed", nid);
    } else {
        node->flags.unresponsive = 0;
        //CTRACE("revived %016lx", node->rpc_node.nid);
    }
}


static int
node_dump(mrkdht_node_t **node, UNUSED void *udata)
{
    char buf[1024];
    mrkrpc_node_str(&(*node)->rpc_node, buf, countof(buf));
    CTRACE("<%s %s lseen=%016lx>",
           buf,
           (*node)->flags.unresponsive ? "down" : "up",
           (*node)->last_seen);
    return 0;
}

void
mrkdht_dump_node(mrkdht_node_t *node)
{
    node_dump(&node, NULL);
}

static int
node_dump_trie(mnbtrie_node_t *trn, UNUSED void *udata)
{
    if (trn->value != NULL) {
        mrkdht_dump_node(trn->value);
    }
    return 0;
}

void
mrkdht_nodes_dump(void)
{
    btrie_traverse(&nodes, node_dump_trie, NULL);
}



mrkdht_nid_t
mrkdht_node_get_nid(mrkdht_node_t *node)
{
    return node->rpc_node.nid;
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

    if (DTQUEUE_EMPTY(&bucket->nodes)) {
        return 0;
    }
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
    if (DTQUEUE_PREV(link, node) != NULL) {
        //CTRACE("prev:");
        //node_dump(&DTQUEUE_PREV(link, node), NULL);
    } else {
        //CTRACE("prev: NULL");
    }
    if (DTQUEUE_NEXT(link, node) != NULL) {
        //CTRACE("next:");
        //node_dump(&DTQUEUE_NEXT(link, node), NULL);
    } else {
        //CTRACE("next: NULL");
    }
    DTQUEUE_REMOVE(&bucket->nodes, link, node);
    //CTRACE("after removal len=%ld", DTQUEUE_LENGTH(&bucket->nodes));
    //bucket_dump(bucket, NULL);
}


static int
bucket_put_node(mrkdht_bucket_t *bucket, mrkdht_node_t *node, unsigned flags)
{
    int res;

    //CTRACE("bucket->id=%d did=%d", bucket->id, distance_to_bucket_id(node->distance));
    assert(bucket->id == distance_to_bucket_id(node->distance));
    //assert(DTQUEUE_ORPHAN(&bucket->nodes, link, node));

    if (!DTQUEUE_ORPHAN(&bucket->nodes, link, node)) {
        //CTRACE("first removing:");
        //node_dump(&node, NULL);
        bucket_remove_node(bucket, node);
    }

    if (DTQUEUE_LENGTH(&bucket->nodes) >= MRKDHT_BUCKET_MAX) {
        mrkdht_node_t *oldest;

        oldest = DTQUEUE_HEAD(&bucket->nodes);

        //CTRACE("bucket before ping (%ld):", DTQUEUE_LENGTH(&bucket->nodes));
        //bucket_dump(bucket, NULL);

        if (flags & MRKDHT_BUCKET_PUT_NODE_FPING) {
            res = ping_node(oldest);
        } else {
            res = 0;
        }

        /* check if the oldest is alive */
        if (res == 0) {
            /* yes, make it the newest one */
            //CTRACE("bucket before D/E (%ld):", DTQUEUE_LENGTH(&bucket->nodes));
            //bucket_dump(bucket, NULL);

            DTQUEUE_DEQUEUE(&bucket->nodes, link);

            //CTRACE("bucket after D (%ld):", DTQUEUE_LENGTH(&bucket->nodes));
            //bucket_dump(bucket, NULL);

            DTQUEUE_ENTRY_FINI(link, oldest);
            DTQUEUE_ENQUEUE(&bucket->nodes, link, oldest);

            //CTRACE("bucket after E (%ld):", DTQUEUE_LENGTH(&bucket->nodes));
            //bucket_dump(bucket, NULL);

            bucket->last_accessed = mrkthr_get_now();

            //CTRACE("not putting node:");
            //node_dump(&node, NULL);
            //CTRACE("bucket is full (%ld):", DTQUEUE_LENGTH(&bucket->nodes));
            //bucket_dump(bucket, NULL);

            TRRET(BUCKET_PUT_NODE + 1);

        } else {
            /* no, get rid of oldest, ... */
            forget_node_bucket(bucket, oldest);

            /* ... and welcome the newcomer */
            node->last_seen = mrkthr_get_now();
            DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
            bucket->last_accessed = mrkthr_get_now();
        }

    } else {
        node->last_seen = mrkthr_get_now();
        DTQUEUE_ENQUEUE(&bucket->nodes, link, node);
        bucket->last_accessed = mrkthr_get_now();
    }

    //CTRACE("final bucket:");
    //bucket_dump(bucket, NULL);
    TRRET(0);
}

static void
stamp_bucket(mrkdht_node_t *node)
{
    mrkdht_bucket_t *bucket;

    bucket = buckets_get_bucket(node->distance);
    bucket->last_accessed = mrkthr_get_now();
}


UNUSED static mrkdht_bucket_t *
node_get_bucket(mrkdht_node_t *node)
{
    mrkdht_bucket_t *bucket = NULL;
    mrkdht_node_t *tmp;

    bucket = buckets_get_bucket(node->distance);
    for (tmp = DTQUEUE_HEAD(&bucket->nodes);
         tmp != NULL;
         tmp = DTQUEUE_NEXT(link, tmp)) {

        if (tmp == node) {
            return bucket;
        }
    }

    return NULL;
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

        if (node->rpc_node.nid != exc && !node->flags.unresponsive) {
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

    //CTRACE("nadded=%ld", nadded);
    TRRET(0);
}


static size_t
fill_nodes_from_bucket_array(mrkdht_bucket_t *bucket,
                             size_t sz,
                             mrkdht_node_t *rv[],
                             mrkdht_nid_t exc)
{
    mrkdht_node_t *node;
    size_t i;

    node = DTQUEUE_HEAD(&bucket->nodes);
    for (i = 0; i < sz && node != NULL;) {

        if (node->rpc_node.nid != exc && !node->flags.unresponsive) {
            rv[i] = node;
            ++i;
        }
        node = DTQUEUE_NEXT(link, node);
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

    //CTRACE("recvop=%02x -> %02x", qe->recvop, MRKDHT_MSG_PONG);
    if (qe->recvop != MRKDHT_MSG_PING) {
        ++(stats[MRKDHT_STATS_PONG].nfailures);
        return 123;
    }
    qe->sendop = MRKDHT_MSG_PONG;
    ++(stats[MRKDHT_STATS_PONG].ntotal);
    /*
     * Revive this node.
     */
    revive_node_from_addr(qe->peer->nid,
                          qe->peer->addr,
                          qe->peer->addrlen,
                          0);
    /* simulate delay */
    //res = mrkthr_sleep(3);
    return res;
}

/* find nodes */

static int
msg_find_nodes_call_req_handler(UNUSED mrkrpc_ctx_t *ctx,
                                mrkrpc_queue_entry_t *qe)
{
    int res = 0;

    //CTRACE("recvop=%02x -> %02x", qe->recvop, MRKDHT_MSG_FIND_NODES_CALL);
    if (qe->recvop != MRKDHT_MSG_FIND_NODES_CALL) {
        return MSG_FIND_NODES_CALL_REQ_HANDLER + 1;
    }

    /* set up send things */
    qe->sendop = MRKDHT_MSG_FIND_NODES_RET;
    qe->senddat = mrkdata_datum_from_spec(node_list_spec, NULL, 0);
    if (qe->recvdat == NULL) {
        return MSG_FIND_NODES_CALL_REQ_HANDLER + 2;
    }

    /* qe->recvdat is UINT64 */
    res = find_closest_nodes_datum(qe->recvdat->value.u64, qe->senddat,
                                   qe->peer->nid);

    //CTRACE("find_closest_nodes_datum res=%s", diag_str(res));

    /*
     * Update this node in our table of nodes.
     */
    revive_node_from_addr(qe->peer->nid,
                          qe->peer->addr,
                          qe->peer->addrlen,
                          0);

    /*
     * clean up recv things
     */
    mrkdata_datum_destroy(&qe->recvdat);

    return res;
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
mrkdht_set_local_node(mrkdht_nid_t nid, const char *hostname, int port)
{
    if (mrkrpc_ctx_init(&rpc) != 0) {
        FAIL("mrkrpc_ini");
    }

    /* ping, pong */
    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_PING,
                       NULL,
                       msg_ping_req_handler);

    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_PONG,
                       NULL,
                       NULL);

    /* find_nodes_call, find_nodes_ret */
    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_FIND_NODES_CALL,
                       nid_spec,
                       msg_find_nodes_call_req_handler);

    mrkrpc_ctx_register_msg(&rpc,
                       MRKDHT_MSG_FIND_NODES_RET,
                       node_list_spec,
                       NULL);

    mrkrpc_ctx_set_call_timeout(&rpc, MRKDHT_RPC_TIMEOUT);
    mrkrpc_ctx_set_local_node(&rpc, nid, hostname, port);
    node_init(&me);
    mrkrpc_node_copy(&me.rpc_node, &rpc.me);
}


void
mrkdht_set_refresh(uint64_t msec)
{
    if (msec < 1000) {
        msec = 1000;
    }
    trefresh = msec;
}


int
mrkdht_run(void)
{
    mrkthr_spawn("rpc_server", rpc_server, 0);
    monitor_thr = mrkthr_spawn("monitor", monitor, 0);
    refresher_thr = mrkthr_spawn("refresher", refresher, 0);
    return 0;
}

static int
hold_on_rtt(mrkrpc_node_t *node)
{
    int res = 0;
    uint64_t before;
    mrkdht_host_info_t *hi;
    int64_t tts;

    before = mrkthr_get_now_ticks();
    hi = get_host_info(node);

    tts = (int64_t)(hi->last_rpc_call + hi->rtt) - (int64_t)before;
    //CTRACE("rtt=%Lf tts=%Lf", mrkthr_ticksdiff2sec(hi->rtt), mrkthr_ticksdiff2sec(tts));
    if (tts > 0) {
        res = mrkthr_sleep_ticks(tts);
    }
    return res;
}

static int
ping_node(mrkdht_node_t *node)
{
    int res;
    mrkdata_datum_t *rv = NULL;
    uint64_t before;

    if (node->flags.unresponsive) {
        TRRET(PING_NODE + 1);
    }

    if (hold_on_rtt(&node->rpc_node) != 0) {
        TRRET(PING_NODE + 2);
    }

    before = mrkthr_get_now_ticks();
    res = mrkrpc_call(&rpc, &node->rpc_node, MRKDHT_MSG_PING, NULL, &rv);
    save_host_info(&node->rpc_node, before);

    //CTRACE("res=%s rv=%p", mrkrpc_diag_str(res), rv);
    //if (rv != NULL) {
    //    mrkdata_datum_dump(rv);
    //}

    mrkdata_datum_destroy(&rv);

    if (res != 0) {
        if (res == MRKRPC_CALL_TIMEOUT) {
            //CTRACE("RPC call timed out to this node:");
            //mrkdht_dump_node(node);
        }
        forget_node(node);
        ++(stats[MRKDHT_STATS_PING].nfailures);
        res = PING_NODE + 3;
    } else {
        stamp_bucket(node);
    }
    ++(stats[MRKDHT_STATS_PING].ntotal);

    TRRET(res);
}


int
mrkdht_ping(mrkdht_nid_t nid)
{
    mnbtrie_node_t *trn;
    mrkdht_node_t *node;
    int res;

    if ((trn = btrie_find_exact(&nodes, nid)) == NULL) {
        TRRET(MRKDHT_PING + 1);
    }
    assert(trn->value != NULL);
    node = trn->value;

    /* let's think it's alive (public API only) */
    node->flags.unresponsive = 0;
    res = ping_node(node);

    return res;
}


int
mrkdht_find_closest_nodes(mrkdht_nid_t nid, mrkdht_node_t **nodes, size_t *sz)
{
    return find_closest_nodes_array(nid, nodes, sz, me.rpc_node.nid);
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
               mnbtrie_t *shortlist,
               mnbtrie_t *contacted,
               mrkdht_nid_t nid)
{
    int res = 0;
    mrkdht_nid_t dist;
    mnbtrie_node_t *ctrn;
    mnbtrie_node_t *strn;


    if ((ctrn = btrie_find_exact(contacted,
                                node->rpc_node.nid)) == NULL) {

        if (nid == node->rpc_node.nid) {
            /*
             * just ping it (forsibly).
             * XXX think of not pinging in case it's not expired
             */
            node->flags.unresponsive = 0;
            res = ping_node(node);

            if (res == 0) {

                /* add the active contact to shortlist and to contacted */
                dist = distance(nid, node->rpc_node.nid);
                strn = btrie_add_node(shortlist, dist);

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

                ctrn = btrie_add_node(contacted, node->rpc_node.nid);
                ctrn->value = node;

            } else {
                CTRACE("Ping to %016lx failed (%s)", node->rpc_node.nid, diag_str(res));
                goto bad;
            }


        } else {
            mrkdata_datum_t *arg;
            mrkdata_datum_t *rv;
            uint64_t before;

            arg = mrkdata_datum_make_u64(nid);
            rv = NULL;

            if (hold_on_rtt(&node->rpc_node) != 0) {
                goto bad;
            }

            before = mrkthr_get_now_ticks();
            res = mrkrpc_call(&rpc,
                              &node->rpc_node,
                              MRKDHT_MSG_FIND_NODES_CALL,
                              arg,
                              &rv);

            save_host_info(&node->rpc_node, before);

            mrkdata_datum_destroy(&arg);

            if (rv != NULL) {
                mrkdht_nid_t dist;
                mrkdata_datum_t **dat;
                mnarray_iter_t it;

                /* update bucket*/
                revive_node_from_addr(node->rpc_node.nid,
                                      node->rpc_node.addr,
                                      node->rpc_node.addrlen,
                                      0);
                stamp_bucket(node);

                /* add the active contact to shortlist and to contacted */
                dist = distance(nid, node->rpc_node.nid);
                strn = btrie_add_node(shortlist, dist);

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

                ctrn = btrie_add_node(contacted, node->rpc_node.nid);
                ctrn->value = node;


                //CTRACE("data:");
                //mrkdata_datum_dump(rv);

                for (dat = array_first(&rv->data.fields, &it);
                     dat != NULL;
                     dat = array_next(&rv->data.fields, &it)) {

                    mrkdata_datum_t **fdat;
                    mrkdht_nid_t rnid;
                    char *addr;
                    int port;

                    if ((fdat = array_get(&(*dat)->data.fields,
                                         MRKDHT_NODE_FIELD_NID)) == NULL) {
                        CTRACE("invalid datum");
                        mrkdata_datum_dump(*dat);
                    }
                    rnid = (*fdat)->value.u64;
                    if ((fdat = array_get(&(*dat)->data.fields,
                                         MRKDHT_NODE_FIELD_ADDR)) == NULL) {
                        CTRACE("invalid datum");
                        mrkdata_datum_dump(*dat);
                    }
                    addr = (*fdat)->data.str;
                    if ((fdat = array_get(&(*dat)->data.fields,
                                         MRKDHT_NODE_FIELD_PORT)) == NULL) {
                        CTRACE("invalid datum");
                        mrkdata_datum_dump(*dat);
                    }
                    port = (*fdat)->value.u16;

                    /*
                     * first check out with shortlist indexed by dist to
                     * this nid
                     */
                    dist = distance(nid, rnid);
                    strn = btrie_add_node(shortlist, dist);

                    if (strn->value == NULL) {
                        mrkdht_node_t *rnode;

                        /*
                         * most likely not yet registered ...
                         */

                        rnode = NULL;
                        //CTRACE("shortlist: not found, "
                        //       "will try to register %016lx,%s,%d",
                        //       rnid, addr, port);

                        if (register_node_from_params(
                                rnid,
                                addr,
                                port,
                                &rnode,
                                MRKDHT_BUCKET_PUT_NODE_FPING |
                                MRKDHT_REGISTER_NODE_BUCKET_OPTIONAL) != 0) {
                            CTRACE("register_node_from_params failed ...");
                            //btrie_remove_node(&shortlist, strn) ?

                        } else {
                            //CTRACE("shortlist: registered OK");
                            strn->value = rnode;

                        }

                    } else {
                        /* registered, check it */
                        //CTRACE("shortlist: already registered %016lx,%s,%d",
                        //       rnid, addr, port);
                    }
                }

            } else {
                CTRACE("Find nodes call to %016lx failed (%s)", node->rpc_node.nid, mrkrpc_diag_str(res));
                /* remove this node from nodes and buckets */
                forget_node(node);
                goto bad;
            }

            mrkdata_datum_destroy(&rv);
        }

    } else {
        //CTRACE("node %016lx already contacted", node->rpc_node.nid);
    }

    /* let's "revive" it */
    node->flags.unresponsive = 0;

    return;


bad:
    dist = distance(nid, node->rpc_node.nid);
    if ((strn = btrie_find_exact(shortlist, dist)) != NULL) {
        btrie_remove_node(shortlist, strn);
    }
    if ((ctrn = btrie_find_exact(contacted, node->rpc_node.nid)) != NULL) {
        btrie_remove_node(contacted, ctrn);
    }
    node->flags.unresponsive = 1;
}


static int
select_alpha(mnbtrie_node_t *trn, void *udata)
{
    mrkdht_node_t *node;
    mnbtrie_node_t *ctrn;
    struct {
        mrkdht_node_t **alpha;
        int i;
        mnbtrie_t *contacted;
    } *params = udata;

    if (trn->value == NULL) {
        return 0;
    }

    if (params->i >= MRKDHT_ALPHA) {
        return 1;
    }

    node = trn->value;

    ctrn = btrie_find_exact(params->contacted, node->rpc_node.nid);
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
select_result(mnbtrie_node_t *trn, void *udata)
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
    mnbtrie_t shortlist;
    mnbtrie_t contacted;
    mnbtrie_node_t *ctrn;
    struct {
        mrkdht_node_t **nodes;
        size_t i;
        size_t sz;
    } rparams;

    closest_node = NULL;
    btrie_init(&shortlist);
    btrie_init(&contacted);

    /*
     * initial alpha
     */
    sz = MRKDHT_ALPHA;
    memset(alpha, '\0', sizeof(alpha));
    res = find_closest_nodes_array(nid, alpha, &sz, me.rpc_node.nid);

    while (!(mflags & MRKDHT_MFLAG_SHUTDOWN)) {
        mnbtrie_node_t *trn;
        struct {
            mrkdht_node_t **alpha;
            int i;
            mnbtrie_t *contacted;
        } params;

        for (i = 0; i < sz; ++i) {
            //CTRACE("trying contact from alpha: %016lx",
            //       alpha[i]->rpc_node.nid);
            fill_shortlist(alpha[i], &shortlist, &contacted, nid);
        }

        trn = BTRIE_MIN(&shortlist);
        if (trn != NULL) {
            if (trn->value != NULL && trn->value == closest_node) {
                //CTRACE("finished lookup at:");
                //node_dump(&closest_node, NULL);
                break;
            }
            closest_node = trn->value;
            //CTRACE("closest node:");
            //if (closest_node != NULL) {
            //    node_dump(&closest_node, NULL);
            //} else {
            //    CTRACE("NULL");
            //}
        } else {
            break;
        }

        if (closest_node == NULL) {
            CTRACE("Failed to find closest node ...");
            break;
        }

        /* select new alpha from the shortlist  */
        memset(alpha, '\0', sizeof(alpha));
        params.alpha = alpha;
        params.i = 0;
        params.contacted = &contacted;
        btrie_traverse(&shortlist, select_alpha, &params);
        sz = params.i;

        if (sz == 0) {
            break;
        }
    }

    //CTRACE("shortlist:");
    //btrie_traverse(&shortlist, node_dump_trie, NULL);

    /*
     * Final call, we fill shortlist for the last time from only those
     * nodes, if any, that were not contacted yet.
     */
    rparams.nodes = rnodes;
    rparams.i = 0;
    rparams.sz = *rsz;
    btrie_traverse(&shortlist, select_result, &rparams);

    for (i = 0; i < rparams.i; ++i) {
        ctrn = btrie_find_exact(&contacted, rnodes[i]->rpc_node.nid);

        if (ctrn == NULL) {
            //CTRACE("trying final contact: %016lx",
            //       rnodes[i]->rpc_node.nid);
            fill_shortlist(rnodes[i],
                           &shortlist,
                           &contacted,
                           nid);
        }
    }

    /* now select the result nodes */
    rparams.nodes = rnodes;
    rparams.i = 0;
    rparams.sz = *rsz;
    btrie_traverse(&shortlist, select_result, &rparams);
    *rsz = rparams.i;

    //CTRACE("contacted:");
    //btrie_traverse(&contacted, btrie_node_dump_cb, (void *)1);
    //mrkdht_buckets_dump();

    btrie_fini(&shortlist);
    btrie_fini(&contacted);

    return 0;
}


int
mrkdht_join(mrkdht_nid_t nid,
            const char *hostname,
            int port,
            unsigned flags)
{
    mrkdht_node_t *rnode = NULL;

    if ((register_node_from_params(nid, hostname, port, &rnode, MRKDHT_BUCKET_PUT_NODE_FPING)) != 0) {
        TRRET(MRKDHT_JOIN + 1);
    }

    if (!(flags & MRKDHT_FLAG_JOIN_NOPING)) {
        mrkdht_node_t *nodes[MRKDHT_BUCKET_MAX];
        size_t sz = MRKDHT_BUCKET_MAX;
        int res;
        UNUSED size_t i;

        /* let's think it's alive (public API only) */
        rnode->flags.unresponsive = 0;
        if ((res = ping_node(rnode)) != 0) {
            TRRET(res);
        }

        memset(nodes, '\0', sizeof(nodes));
        mrkdht_lookup_nodes(me.rpc_node.nid, nodes, &sz);
        //CTRACE("while joining looked up with res=%d sz=%ld", res, sz);
        //for (i = 0; i < sz; ++i) {
        //    mrkdht_dump_node(nodes[i]);
        //}

    }

    TRRET(0);
}

/* stats */
void
mrkdht_get_stats(unsigned idx, uint64_t *ntotal, uint64_t *nfailures)
{
    if (idx < countof(stats)) {
        *ntotal = stats[idx].ntotal;
        *nfailures = stats[idx].nfailures;
    }
}


/* module */

void
mrkdht_shutdown(void)
{
    mflags |= MRKDHT_MFLAG_SHUTDOWN;
    mrkthr_set_interrupt(monitor_thr);
    mrkthr_set_interrupt(refresher_thr);
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
    unsigned i;

    assert(!(mflags & MRKDHT_MFLAG_INITIALIZED));

    MEMDEBUG_REGISTER(mrkdht);

    mrkrpc_init();

    nid_spec = mrkdata_make_spec(MRKDATA_UINT64);

    node_spec = mrkdata_make_spec(MRKDATA_STRUCT);
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_UINT64));
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_STR8));
    mrkdata_spec_add_field(node_spec, mrkdata_make_spec(MRKDATA_UINT16));

    node_list_spec = mrkdata_make_spec(MRKDATA_SEQ);
    mrkdata_spec_add_field(node_list_spec, node_spec);

    value_spec = mrkdata_make_spec(MRKDATA_STR64);


    if (array_init(&buckets, sizeof(mrkdht_bucket_t), MRKDHT_IDLEN_BITS + 1,
                    (array_initializer_t)bucket_init,
                    (array_finalizer_t)bucket_fini) != 0) {
        FAIL("array_init");
    }

    id = 0;
    array_traverse(&buckets, (array_traverser_t)bucket_set_id, &id);

    btrie_init(&nodes);

    btrie_init(&host_infos);

    trefresh = 3600000;

    for (i = 0; i < countof(stats); ++i) {
        stats[i].ntotal = 0;
        stats[i].nfailures = 0;
    }

    mflags |= MRKDHT_MFLAG_INITIALIZED;
}

void
mrkdht_fini(void)
{
    assert(mflags & MRKDHT_MFLAG_INITIALIZED);

    btrie_traverse(&nodes, node_destroy, NULL);
    btrie_fini(&nodes);

    btrie_fini(&host_infos);

    array_fini(&buckets);

    node_fini(&me);

    mrkrpc_fini();

    mflags &= ~MRKDHT_MFLAG_INITIALIZED;
}

