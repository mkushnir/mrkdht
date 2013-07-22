#include <assert.h>
#include <err.h>
/* _POSIX_PATH_MAX */
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "diag.h"
#include <mrkcommon/dumpm.h>
#include <mrkcommon/profile.h>
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(testfoo)

#include <mrkthr.h>
#include <mrkapp.h>
#include <mrkdht.h>

#ifndef NDEBUG
const char *malloc_options = "AJ";
#endif

const profile_t *p_ping;

/* configuration */

static const char *myhost = NULL;
static unsigned long myport;
static mrkdht_nid_t mynid;

static const char *pinghost = NULL;
static unsigned long pingport;
static mrkdht_nid_t pingnid;

static int noping = 0;

static size_t npings = 0;

static int _shutdown = 0;
static mrkthr_ctx_t *backdoor_thr;

static void
termhandler(UNUSED int sig)
{
    mrkthr_set_interrupt(backdoor_thr);
    mrkdht_shutdown();
    _shutdown = 1;
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
        char buf[1024];
        ssize_t nread;

        memset(buf, '\0', sizeof(buf));

        if ((nread = mrkthr_read_allb(fd, buf, sizeof(buf))) <= 0) {
            break;
        }
        buf[nread - 2] = '\0';
        //D8(buf, nread);

        if (strcmp(buf, "help") == 0) {
            const char *help = "help: quit\n";
            if (mrkthr_write_all(fd, help, strlen(help)) != 0) {
                break;
            }

        } else if (strcmp(buf, "quit") == 0) {
            const char *error = "bye\n";

            if (mrkthr_write_all(fd, error, strlen(error)) != 0) {
                /* pass through */
                ;
            }
            termhandler(0);
            break;

        } else {
            const char *error = "not supported, bye\n";
            if (mrkthr_write_all(fd, error, strlen(error)) != 0) {
                break;
            }
            break;
        }
    }

    close(fd);

    return 0;
}


static int
pinger(UNUSED int argc, UNUSED void **argv)
{
    int res;
    uint64_t elapsed;

    while (!_shutdown) {
        mrkthr_sleep(100);
        profile_start(p_ping);
        res = mrkdht_ping(pingnid);
        elapsed = profile_stop(p_ping);
        //printf("%ld\n", elapsed);
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

        if (mrkdht_make_node_from_params(pingnid, pinghost, pingport) != 0) {
            FAIL("mrkdht_make_node_from_params");
        }

        for (i = 0; i < 1; ++i) {
            if ((thr = mrkthr_new(NULL, pinger, 0)) == NULL) {
                FAIL("mrkthr_new");
            }
            mrkthr_run(thr);
        }


    } else {
        CTRACE("Not pinging ...");
    }

    while (!_shutdown) {
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
    char bdpath[_POSIX_PATH_MAX];


    MEMDEBUG_REGISTER(testfoo);
#ifndef NDEBUG
    MEMDEBUG_REGISTER(array);
    MEMDEBUG_REGISTER(list);
    MEMDEBUG_REGISTER(trie);
#endif

    while ((ch = getopt(argc, argv, "h:H:np:P:")) != -1) {
        switch (ch) {
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
            myport = strtol(optarg, NULL, 10);
            mynid = 0xdadadada00000000 | myport;
            break;

        case 'P':
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
        errx(1, "Please supply my port between 1024 and 65535 via -p option.");
    }

    if (pinghost == NULL) {
        errx(1, "Please supply ping host via -H option.");
    }

    if (pingport < 1024 || pingport > 65535) {
        errx(1, "Please supply ping port between 1024 and 65535 via -P option.");
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

    profile_init_module();

    p_ping = profile_register("ping");

    mrkthr_init();

    mrkdht_init();

    if ((thr = mrkthr_new("test1", test1, 0)) == NULL) {
        FAIL("mrkthr_new");
    }
    mrkthr_run(thr);

    snprintf(bdpath, sizeof(bdpath), "/tmp/testping.%ld.sock", myport);
    if ((backdoor_thr = mrkthr_new("backdoor",
                          mrk_local_server,
                          4,
                          1,
                          bdpath,
                          backdoor,
                          NULL)) == NULL) {
        FAIL("mrkthr_new");
    }
    mrkthr_run(backdoor_thr);

    mrkthr_loop();

    mrkdht_fini();

    mrkthr_fini();

    profile_report();
    profile_fini_module();

    memdebug_print_stats();

    return 0;
}
