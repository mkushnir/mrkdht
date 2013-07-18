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
static unsigned mflags = 0;

/* ctx */

static list_t nodes;
#define MRKDHT_IDLEN_BITS (sizeof(uint64_t) * 8)
#define MRKDHT_BUCKET_MAX 4
#define MRKDHT_ALPHA 3

static mrkrpc_ctx_t rpc;

static mrkdata_spec_t *node_spec;
#define MRKDHT_NODE_FIELD_NID 0
#define MRKDHT_NODE_FIELD_ADDR 1
#define MRKDHT_NODE_FIELD_PORT 2

static mrkdata_spec_t *node_list_spec;

static mrkdata_spec_t *value_spec;


UNUSED static uint64_t
id_to_bucket(uint64_t id)
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

/* bucket */

static int
bucket_init(mrkdht_bucket_t *bucket)
{
    bucket->last_accessed = 0;
    if (list_init(&bucket->nodes, sizeof(mrkrpc_node_t *), 0,
                  (list_initializer_t)null_initializer,
                  (list_finalizer_t)mrkrpc_node_destroy) != 0) {
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
node_dump(mrkrpc_node_t **node, UNUSED void *udata)
{
    return mrkrpc_node_dump(*node);
}


static int
bucket_dump(mrkdht_bucket_t **bucket, UNUSED void *udata)
{
    CTRACE("bucket %d:", (*bucket)->idx);
    list_traverse(&(*bucket)->nodes,
                  (list_traverser_t)node_dump,
                  NULL);
    return 0;
}


UNUSED
static void
nodes_dump(void)
{
    list_traverse(&nodes,
                 (list_traverser_t)bucket_dump,
                 NULL);
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

    mrkdata_fini();

    mflags &= ~MRKDHT_MFLAG_INITIALIZED;
}

