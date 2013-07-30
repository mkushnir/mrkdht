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
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(testjoin)

#include <mrkthr.h>
#include <mrkapp.h>
#include <mrkdht.h>

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif

/* configuration */

static const char *myhost = NULL;
static unsigned long myport;
static mrkdht_nid_t mynid;

static const char *joinhost = NULL;
static unsigned long joinport;
static mrkdht_nid_t joinnid;

static int nojoin = 0;
static int nowait = 0;
static int nodaemon = 0;


/* internal */
static int _shutdown = 0;
static mrkthr_ctx_t *backdoor_thr;

static void
termhandler(UNUSED int sig)
{
    mrkthr_set_interrupt(backdoor_thr);
    mrkdht_shutdown();
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
            write_response(fd, "OK help kill tdump bdump fcn ln quit");

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

        } else if (strcmp(cmd, "bdump") == 0) {
            write_response(fd, "OK dumping buckets");
            mrkdht_buckets_dump();

        } else if (strcmp(cmd, "fcn") == 0) {
            mrkdht_nid_t nid = 0;

            sscanf(pbuf, "%lx", &nid);
            CTRACE("nid=%016lx", nid);

            mrkdht_test_find_closest_nodes(nid, 3);

            write_response(fd, "OK");

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

        } else {
            write_response(fd, "ERR not supported, bye");
            break;
        }
    }

    close(fd);

    return 0;
}

static int
test1(UNUSED int argc, UNUSED void *argv[])
{
    int res;

    mrkdht_set_me(mynid, myhost, myport);
    mrkdht_run();

    if (!nojoin) {
        while (!_shutdown) {
            if ((res = mrkdht_join(joinnid, joinhost, joinport, 0)) != 0) {
                CTRACE("Failed to join, retrying ...");
                mrkthr_sleep(2000);
            } else {
                break;
            }
        }
    }

    if (nowait) {
        killpg(0, SIGTERM);
    }
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

    MEMDEBUG_REGISTER(testjoin);

    while ((ch = getopt(argc, argv, "ch:H:nNp:P:")) != -1) {
        switch (ch) {
        case 'c':
            nodaemon = 1;
            break;

        case 'h':
            myhost = strdup(optarg);
            break;

        case 'H':
            joinhost = strdup(optarg);
            break;

        case 'n':
            nojoin = 1;
            break;

        case 'N':
            nowait = 1;
            break;

        case 'p':
            myport = strtol(optarg, NULL, 10);
            mynid = 0xdadadadadada0000 | myport;
            break;

        case 'P':
            joinport = strtol(optarg, NULL, 10);
            joinnid = 0xdadadadadada0000 | joinport;
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

    if (!nojoin) {
        if (joinhost == NULL) {
            errx(1, "Please supply join host via -H option.");
        }

        if (joinport < 1024 || joinport > 65535) {
            errx(1, "Please supply join port between 1024 and 65535 "
                 "via -P option.");
        }
    }

    if (myport == joinport) {
        errx(1, "My port and join port may not be the same.");
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

    snprintf(bdpath, sizeof(bdpath), "/tmp/testjoin.%ld.sock", myport);
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
