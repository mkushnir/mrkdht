#include <assert.h>
#include <err.h>
#include <signal.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "diag.h"
#include <mrkcommon/dumpm.h>
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(testfoo)
#include <mrkthr.h>
#include <mrkdht.h>

#ifndef NDEBUG
const char *malloc_options = "AJ";
#endif

/* configuration */

static const char *myhost = NULL;
static unsigned long myport;
static uint64_t mynid;

static const char *pinghost = NULL;
static unsigned long pingport;
static uint64_t pingnid;
static mrkdht_node_t *pingnode;

static int noping = 0;

static size_t npings = 0;

static void
termhandler(UNUSED int sig)
{
    mrkdht_shutdown();
    mrkthr_shutdown();
}


static int
pinger(UNUSED int argc, UNUSED void **argv)
{
    int res;

    while (1) {
        mrkthr_sleep(10);
        res = mrkdht_ping(pingnode);
        if (res != 0) {
            CTRACE("res=%d", res);
        }
        ++npings;
    }
    return 0;
}

static int
test1(UNUSED int argc, UNUSED void **argv)
{
    int i;
    mrkthr_ctx_t *thr;

    mrkdht_set_me(mynid, myhost, myport);
    mrkdht_run();

    mrkthr_sleep(5000);

    if (noping == 0) {
        CTRACE("Now testing ping ...");

        if ((pingnode = mrkdht_make_node(pingnid,
                                         pinghost,
                                         pingport)) == NULL) {
            FAIL("mrkdht_make_node");
        }

        for (i = 0; i < 50; ++i) {
            if ((thr = mrkthr_new(NULL, pinger, 0)) == NULL) {
                FAIL("mrkthr_new");
            }
            mrkthr_run(thr);
        }


        //mrkdht_node_destroy(&pingnode);
    } else {
        CTRACE("Not pinging ...");
    }

    while (1) {
        size_t old_npings;

        old_npings = npings;
        mrkthr_sleep(5000);
        CTRACE("pinged: %ld", npings - old_npings);
    }


    return 0;
}


int
main(int argc, char **argv)
{
    struct sigaction sa;
    char ch;
    mrkthr_ctx_t *thr;


    MEMDEBUG_REGISTER(testfoo);
#ifndef NDEBUG
    MEMDEBUG_REGISTER(array);
    MEMDEBUG_REGISTER(list);
    MEMDEBUG_REGISTER(trie);
#endif

    while ((ch = getopt(argc, argv, "h:H:i:np:")) != -1) {
        switch (ch) {
        case 'i':
            myport = strtol(optarg, NULL, 10);
            mynid = 0xdadadada00000000 | myport;
            break;

        case 'h':
            myhost = strdup(optarg);
            break;

        case 'H':
            pinghost = strdup(optarg);
            break;

        case 'n':
            noping = 1;
            break;

        case 'p':
            pingport = strtol(optarg, NULL, 10);
            pingnid = 0xdadadada00000000 | pingport;
            break;

        default:
            break;

        }
    }

    if (myhost == NULL) {
        errx(1, "Please supply my host via -h option.");
    }

    if (myport < 1024 || myport > 65535) {
        errx(1, "Please supply my port between 1024 and 65535 via -i option.");
    }

    if (pinghost == NULL) {
        errx(1, "Please supply ping host via -H option.");
    }

    if (pingport < 1024 || pingport > 65535) {
        errx(1, "Please supply ping port between 1024 and 65535 via -p option.");
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

    mrkthr_init();

    mrkdht_init();

    if ((thr = mrkthr_new("test1", test1, 0)) == NULL) {
        FAIL("mrkthr_new");
    }
    mrkthr_run(thr);

    mrkthr_loop();

    mrkdht_fini();

    mrkthr_fini();

    memdebug_print_stats();

    return 0;
}
