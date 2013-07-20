#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <strings.h>

#include <mrkcommon/dumpm.h>
#include <mrkcommon/list.h>
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

static list_t nodes;
#define MRKDHT_IDLEN_BITS (sizeof(uint64_t) * 8)
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

/* util */

UNUSED static uint64_t
distance_to_bucket(uint64_t id)
{
    return flsll(id) - 1;
}


UNUSED static uint64_t
distance(uint64_t a, uint64_t b)
{
    return a ^ b;
}


static int
null_initializer(void **o)
{
    *o = NULL;
    return 0;
}

static int
monitor(UNUSED int argc, UNUSED void **argv)
{
    while (1) {
        size_t volume, length;

        mrkthr_sleep(2000);
        mrkthr_compact_sleepq(25000);
        volume = mrkthr_get_sleepq_volume();
        length = mrkthr_get_sleepq_length();
        CTRACE("sleepq: vol=%ld len=%ld", volume, length);

        mrkrpc_ctx_compact_pending(&rpc, 125000);
        volume = mrkrpc_ctx_get_pending_volume(&rpc);
        length = mrkrpc_ctx_get_pending_length(&rpc);
        CTRACE("pending: vol=%ld len=%ld", volume, length);
    }
    return 0;
}


/* node */

static int
node_init(mrkdht_node_t *node)
{
    mrkrpc_node_init(&node->rpc_node);
    node->distance = 0;
    node->last_seen = 0;
    return 0;
}


static int
node_fini(mrkdht_node_t *node)
{
    mrkrpc_node_fini(&node->rpc_node);
    node->distance = 0;
    node->last_seen = 0;
    return 0;
}


mrkdht_node_t *
mrkdht_make_node(uint64_t nid, const char *hostname, int port)
{
    mrkdht_node_t *node;

    if ((node = malloc(sizeof(mrkdht_node_t))) == NULL) {
        FAIL("malloc");
    }

    if (mrkrpc_node_init_from_params(&node->rpc_node, nid, hostname, port)) {
        free(node);
        node = NULL;
    }
    node->distance = distance(me.rpc_node.nid, nid);
    node->last_seen = mrkthr_get_now();

    return node;
}


int
mrkdht_node_destroy(mrkdht_node_t **node)
{
    if (*node != NULL) {
        node_fini(*node);
        free(*node);
        *node = NULL;
    }
    return 0;
}


/* bucket */

static int
bucket_init(mrkdht_bucket_t *bucket)
{
    bucket->last_accessed = 0;
    if (list_init(&bucket->nodes, sizeof(mrkdht_node_t *), 0,
                  (list_initializer_t)null_initializer,
                  (list_finalizer_t)mrkdht_node_destroy) != 0) {
        FAIL("list_init");
    }
    return 0;
}


static int
bucket_new(mrkdht_bucket_t **bucket)
{
    if ((*bucket = malloc(sizeof(mrkdht_bucket_t))) == NULL) {
        FAIL("malloc");
    }
    return bucket_init(*bucket);
}


static int
bucket_fini(mrkdht_bucket_t *bucket)
{
    list_fini(&bucket->nodes);
    bucket->last_accessed = 0;
    return 0;
}

static int
bucket_destroy(mrkdht_bucket_t **bucket)
{
    if (*bucket != NULL) {
        bucket_fini(*bucket);
        free(*bucket);
        *bucket = NULL;
    }
    return 0;
}


static int
node_dump(mrkdht_node_t **node, UNUSED void *udata)
{
    return mrkrpc_node_dump(&(*node)->rpc_node);
}


static int
bucket_dump(mrkdht_bucket_t **bucket, UNUSED void *udata)
{
    list_traverse(&(*bucket)->nodes,
                  (list_traverser_t)node_dump,
                  NULL);
    return 0;
}


UNUSED static void
nodes_dump(void)
{
    list_traverse(&nodes,
                 (list_traverser_t)bucket_dump,
                 NULL);
}


UNUSED static int
node_update(UNUSED mrkdht_node_t *node)
{
    return 0;
}


/* operation */

static int
rpc_server(UNUSED int argc, UNUSED void *argv[])
{
    mrkrpc_run(&rpc);
    mrkrpc_serve(&rpc);
    return 0;
}

void
mrkdht_set_me(uint64_t nid, const char *hostname, int port)
{
    mrkrpc_ctx_set_me(&rpc, nid, hostname, port);
    node_init(&me);
    mrkrpc_node_copy(&me.rpc_node, &rpc.me);
}


int
mrkdht_run(void)
{
    mrkthr_ctx_t *thr;

    if ((thr = mrkthr_new("rpc_server", rpc_server, 0)) == NULL) {
        FAIL("mrkthr_new");
    }
    mrkthr_run(thr);

    if ((thr = mrkthr_new("monitor", monitor, 0)) == NULL) {
        FAIL("mrkthr_new");
    }
    mrkthr_run(thr);

    return 0;
}

int
mrkdht_ping(UNUSED mrkdht_node_t *node)
{
    int res;
    mrkdata_datum_t *rv = NULL;

    res = mrkrpc_call(&rpc, &node->rpc_node, MRKDHT_MSG_PING, NULL, &rv);
    //TRACE("res=%d rv=%p", res, rv);
    if (rv != NULL) {
        mrkdata_datum_dump(rv);
    }
    mrkdata_datum_destroy(&rv);
    return res;
}


/* rpc ops */

static int
msg_ping_req_handler(UNUSED mrkrpc_ctx_t *ctx,
                     mrkrpc_queue_entry_t *qe)
{
    //TRACE("op=%02x -> %02x", qe->op, MRKDHT_MSG_PONG);
    if (qe->op != MRKDHT_MSG_PING) {
        return 123;
    }
    qe->op = MRKDHT_MSG_PONG;
    return 0;
}

static int
msg_ping_resp_handler(UNUSED mrkrpc_ctx_t *ctx,
                      mrkrpc_queue_entry_t *qe)
{
    //TRACE("op=%02x", qe->op);
    if (qe->op != MRKDHT_MSG_PONG) {
        return 234;
    }
    return 0;
}

static int
msg_pong_req_handler(UNUSED mrkrpc_ctx_t *ctx,
                     UNUSED mrkrpc_queue_entry_t *qe)
{
    /* error ever */
    //TRACE("...");
    return 345;
}

static int
msg_pong_resp_handler(UNUSED mrkrpc_ctx_t *ctx,
                      UNUSED mrkrpc_queue_entry_t *qe)
{
    /* error ever */
    //TRACE("...");
    return 0;
}

/* module */

void
mrkdht_shutdown(void)
{
    mflags |= MRKDHT_MFLAG_SHUTDOWN;
    mrkrpc_shutdown();
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

    if (list_init(&nodes, sizeof(mrkdht_bucket_t *), MRKDHT_IDLEN_BITS,
                  (list_initializer_t)bucket_new,
                  (list_finalizer_t)bucket_destroy) != 0) {
        FAIL("list_init");
    }

    mflags |= MRKDHT_MFLAG_INITIALIZED;
}

void
mrkdht_fini(void)
{
    if (!(mflags & MRKDHT_MFLAG_INITIALIZED)) {
        return;
    }

    list_fini(&nodes);

    mrkrpc_fini();

    node_fini(&me);

    mrkdata_fini();

    mflags &= ~MRKDHT_MFLAG_INITIALIZED;
}

