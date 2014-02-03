#include <assert.h>
#include <err.h>
/* _POSIX_PATH_MAX */
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "unittest.h"
#include "diag.h"
#include <mrkcommon/dumpm.h>
#include <mrkcommon/util.h>

#include <mrkthr.h>
#include <mrkapp.h>
#include <mrkdht.h>

#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(testloadping)

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif

/* configuration */

static const char *myhost = NULL;
static unsigned long myport;
static mrkdht_nid_t mynid;

static const char *pinghost = NULL;
static unsigned long pingport;
static mrkdht_nid_t pingnid;

static int noping = 0;
static int nojoin = 0;
static int nodaemon = 0;

static uint64_t pingsleep = 1000;
static uint64_t printsleep = 1000;

#define REFRESH_INTERVAL_MIN (20000)
static uint64_t refresh_interval = 60000;


/* internal */
static int _shutdown = 0;
static mrkthr_ctx_t *backdoor_thr;
static mrkthr_ctx_t *pinger_thr;

static int
delayed_shutdown(UNUSED int argc, UNUSED void **argv)
{
    mrkthr_sleep(pingsleep);
    CTRACE("shutting down mrkdht module ...");
    mrkdht_shutdown();
    return 0;
}


static void
termhandler(UNUSED int sig)
{
    mrkthr_set_interrupt(backdoor_thr);
    if (pinger_thr != NULL) {
        mrkthr_set_interrupt(pinger_thr);
    }
    mrkthr_spawn("delayed_shutdown", delayed_shutdown, 0);
    _shutdown = 1;
}

static void
write_response(int fd, const char *resp)
{
    size_t sz;
    char buf[1024];

    sz = MIN(strlen(resp), countof(buf) - 1);
    strncpy(buf, resp, sz);
    buf[sz] = '\n';

    if (mrkthr_write_all(fd, buf, sz + 1) != 0) {
        /* pass through */
        ;
    }
}

static int
backdoor(UNUSED int argc, void **argv)
{
    int fd;
    void *udata;

    assert(argc == 2);
    fd = (int)(intptr_t)argv[0];
    udata = argv[1];

    while (!_shutdown) {
        char buf[1024], *pbuf;
        ssize_t nread;
        char cmd[32];

        memset(buf, '\0', sizeof(buf));

        if ((nread = mrkthr_read_allb(fd, buf, sizeof(buf))) <= 0) {
            break;
        }

        pbuf = buf;

        if (sscanf(pbuf, "%31s", cmd) <= 0) {
            break;
        }

        pbuf += strlen(cmd);

        //CTRACE("cmd=%s", cmd);

        if (strcmp(cmd, "help") == 0) {
            write_response(fd, "OK help kill tdump bdump ldump mdump fcn ln p stats quit");

        } else if (strcmp(cmd, "quit") == 0) {
            write_response(fd, "OK bye");
            termhandler(0);
            break;

        } else if (strcmp(cmd, "kill") == 0) {
            write_response(fd, "OK killing");
            mrkthr_shutdown();
            break;

        } else if (strcmp(cmd, "tdump") == 0) {
            write_response(fd, "OK dumping threads");
            mrkthr_dump_all_ctxes();
            mrkthr_dump_sleepq();

        } else if (strcmp(cmd, "bdump") == 0) {
            write_response(fd, "OK dumping buckets");
            mrkdht_buckets_dump();

        } else if (strcmp(cmd, "ldump") == 0) {
            write_response(fd, "OK dumping nodes");
            mrkdht_nodes_dump();

        } else if (strcmp(cmd, "mdump") == 0) {
            write_response(fd, "OK dumping memory");
            memdebug_print_stats();

        } else if (strcmp(cmd, "fcn") == 0) {
            mrkdht_nid_t nid = 0;
            char buf[64];

            sscanf(pbuf, "%lx", &nid);
            snprintf(buf, sizeof(buf), "OK fcn for %016lx", nid);
            write_response(fd, buf);

            mrkdht_test_find_closest_nodes(nid, MRKDHT_BUCKET_MAX);


        } else if (strcmp(cmd, "ln") == 0) {
            mrkdht_nid_t nid = 0;
            mrkdht_node_t *nodes[MRKDHT_BUCKET_MAX];
            size_t sz = MRKDHT_BUCKET_MAX;
            int res;
            size_t i;

            sscanf(pbuf, "%lx", &nid);
            CTRACE("nid=%016lx", nid);

            memset(nodes, '\0', sizeof(nodes));
            res = mrkdht_lookup_nodes(nid, nodes, &sz);
            CTRACE("res=%d sz=%ld", res, sz);
            for (i = 0; i < sz; ++i) {
                mrkdht_dump_node(nodes[i]);
            }

            write_response(fd, "OK");

        } else if (strcmp(cmd, "p") == 0) {
            int res;
            mrkdht_nid_t nid = 0;

            sscanf(pbuf, "%lx", &nid);
            CTRACE("pinging nid=%016lx", nid);

            res = mrkdht_ping(nid);
            CTRACE("res=%s", diag_str(res));

        } else if (strcmp(cmd, "stats") == 0) {
            CTRACE("pending %ld/%ld sendq %ld",
                   mrkdht_get_rpc_pending_volume(),
                   mrkdht_get_rpc_pending_length(),
                   mrkdht_get_rpc_sendq_length());

        } else {
            write_response(fd, "ERR not supported, bye");
            break;
        }
    }

    close(fd);

    return 0;
}

static int
pinger(UNUSED int argc, UNUSED void **argv)
{
    while (!_shutdown) {
        if (nojoin) {
            CTRACE("not joining");
            pinger_thr = NULL;
            return 0;
        }

        if (mrkdht_join(pingnid, pinghost, pingport, 0) == 0) {
            break;
        }

        if (mrkthr_sleep(pingsleep) != 0) {
            break;
        }

    }

    if (noping) {
        CTRACE("not pinging");
        pinger_thr = NULL;
        return 0;
    }

    while (!_shutdown) {
        int res;
        mrkdht_node_t *nodes[MRKDHT_BUCKET_MAX];
        size_t sz = MRKDHT_BUCKET_MAX;
        size_t i;


        if (mrkthr_sleep(pingsleep) != 0) {
            CTRACE("sleep broken ...");
            break;
        }

        res = mrkdht_ping(pingnid);

        if (res != 0) {
            ERROR(FRED("ping of %016lx failed: %s"),
                   pingnid,
                   mrkdht_diag_str(res));
        }

        sz = MRKDHT_BUCKET_MAX;
        memset(nodes, '\0', sizeof(nodes));
        res = mrkdht_find_closest_nodes(mynid, nodes, &sz);

        for (i = 0; i < sz; ++i) {

            res = mrkdht_ping(mrkdht_node_get_nid(nodes[i]));

            if (res != 0) {
                ERROR(FRED("ping of %016lx failed: %s"),
                       mrkdht_node_get_nid(nodes[i]),
                       mrkdht_diag_str(res));
            }
        }
    }

    /*
     * XXX Think of how to take care of the global reference to this
     * XXX thread.
     */
    pinger_thr = NULL;
    return 0;
}

static int
printer(UNUSED int argc, UNUSED void **argv)
{
    size_t pingtotal = 0, old_pingtotal = 0;
    size_t pingfailures = 0, old_pingfailures = 0;
    size_t pongtotal = 0, old_pongtotal = 0;
    size_t pongfailures = 0, old_pongfailures = 0;
    size_t pending = 0, old_pending = 0;
    size_t sendq = 0, old_sendq = 0;

    while (!_shutdown) {

        if (mrkthr_sleep(printsleep) != 0) {
            break;
        }

        mrkdht_get_stats(MRKDHT_STATS_PING, &pingtotal, &pingfailures);
        mrkdht_get_stats(MRKDHT_STATS_PONG, &pongtotal, &pongfailures);
        pending = mrkdht_get_rpc_pending_length();
        sendq = mrkdht_get_rpc_sendq_length();

        fprintf(stderr, "%ld %ld %ld %ld %ld %ld\n",
                pingtotal - old_pingtotal,
                pingfailures - old_pingfailures,
                pongtotal - old_pongtotal,
                pongfailures - old_pongfailures,
                pending,
                sendq);
        old_pingtotal = pingtotal;
        old_pingfailures = pingfailures;
        old_pongtotal = pongtotal;
        old_pongfailures = pongfailures;
        old_pending = pending;
        old_sendq = sendq;
    }
    return 0;
}

UNUSED static int
mprinter(UNUSED int argc, UNUSED void **argv)
{
    while (!_shutdown) {
        if (mrkthr_sleep(61000) != 0) {
            break;
        }
        //memdebug_print_stats();
        mrkdht_buckets_dump();
    }
    return 0;
}

static int
test1(UNUSED int argc, UNUSED void *argv[])
{
    mrkdht_set_local_node(mynid, myhost, myport);
    mrkdht_set_refresh(refresh_interval);
    mrkdht_run();

    pinger_thr = mrkthr_spawn("pinger", pinger, 0);
    mrkthr_spawn("printer", printer, 0);
    mrkthr_spawn("mprinter", mprinter, 0);

    return 0;
}


int
main(int argc, char **argv)
{
    struct sigaction sa;
    char ch;
    char bdpath[_POSIX_PATH_MAX];
    char pidfile[_POSIX_PATH_MAX];
    char outfile[_POSIX_PATH_MAX];

    MEMDEBUG_REGISTER(testloadping);
#ifndef NDEBUG
    MEMDEBUG_REGISTER(array);
    MEMDEBUG_REGISTER(list);
    MEMDEBUG_REGISTER(trie);
#endif

    while ((ch = getopt(argc, argv, "ch:H:jnp:P:r:s:S:")) != -1) {
        switch (ch) {
        case 'c':
            nodaemon = 1;
            break;

        case 'h':
            myhost = strdup(optarg);
            break;

        case 'H':
            pinghost = strdup(optarg);
            break;

        case 'j':
            nojoin = 1;
            noping = 1;
            break;

        case 'n':
            noping = 1;
            break;

        case 'p':
            myport = strtol(optarg, NULL, 10);
            mynid = 0xdadadadadada0000 | myport;
            break;

        case 'P':
            pingport = strtol(optarg, NULL, 10);
            pingnid = 0xdadadadadada0000 | pingport;
            break;

        case 'r':
            refresh_interval = strtol(optarg, NULL, 10);
            refresh_interval = MAX(REFRESH_INTERVAL_MIN, refresh_interval);
            break;

        case 's':
            pingsleep = strtol(optarg, NULL, 10);
            break;

        case 'S':
            printsleep = strtol(optarg, NULL, 10);
            break;

        default:
            break;

        }
    }

    if (myhost == NULL) {
        errx(1, "Please supply my host via -h option.");
    }

    if (myport < 1024 || myport > 65535) {
        errx(1, "Please supply my port between 1024 and 65535 via -p option.");
    }

    if (!noping) {
        if (pinghost == NULL) {
            errx(1, "Please supply ping host via -H option.");
        }

        if (pingport < 1024 || pingport > 65535) {
            errx(1, "Please supply ping port between 1024 and 65535 "
                 "via -P option.");
        }
    }

    if (myport == pingport) {
        errx(1, "My port and ping port may not be the same.");
    }

    sa.sa_handler = termhandler;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGINT, &sa, NULL) != 0) {
        FAIL("sigaction");
    }
    if (sigaction(SIGTERM, &sa, NULL) != 0) {
        FAIL("sigaction");
    }

    if (!nodaemon) {
        snprintf(pidfile, sizeof(pidfile), "pid.%ld", myport);
        snprintf(outfile, sizeof(outfile), "out.%ld", myport);
        daemonize(pidfile, outfile, outfile);
    }

    mrkthr_init();

    mrkdht_init();

    mrkthr_spawn("test1", test1, 0);

    snprintf(bdpath, sizeof(bdpath), "/tmp/testloadping.%ld.sock", myport);
    backdoor_thr = mrkthr_spawn("backdoor",
                                local_server,
                                4,
                                1,
                                bdpath,
                                backdoor,
                                NULL);

    mrkthr_loop();

    mrkdht_fini();

    mrkthr_fini();

    memdebug_print_stats();

    return 0;
}
