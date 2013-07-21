#include <assert.h>
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
MEMDEBUG_DECLARE(testfoo)

#include <mrkthr.h>
#include <mrkdht.h>
#include <mrkapp.h>

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif

static int _shutdown = 0;
static mrkthr_ctx_t *backdoor_thr;

/* utilities */


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
test1(UNUSED int argc, UNUSED void *argv[])
{
    int res = 0;
    mrkdht_set_me(1234, "localhost", 0x1234);
    mrkdht_run();
    CTRACE("res=%d", res);
    return 0;
}


int
main(void)
{
    UNUSED mrkthr_ctx_t *thr;
    struct sigaction sa;

    MEMDEBUG_REGISTER(testfoo);

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

    if ((backdoor_thr = mrkthr_new("backdoor",
                          mrk_local_server,
                          3,
                          "/tmp/test.sock",
                          backdoor,
                          NULL)) == NULL) {
        FAIL("mrkthr_new");
    }
    mrkthr_run(backdoor_thr);

    mrkthr_loop();

    mrkdht_fini();

    mrkthr_fini();

    memdebug_print_stats();

    return 0;
}
