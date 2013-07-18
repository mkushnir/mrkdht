#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <time.h>

#include "unittest.h"
#include "diag.h"
#include <mrkcommon/dumpm.h>
#include <mrkcommon/memdebug.h>
MEMDEBUG_DECLARE(testfoo)
#include <mrkthr.h>
#include <mrkdht.h>

#ifndef NDEBUG
const char *_malloc_options = "AJ";
#endif

static void
termhandler(UNUSED int sig)
{
    mrkthr_fini();
}


UNUSED static void
test0(void)
{
    struct {
        long rnd;
        int in;
        int expected;
    } data[] = {
        {0, 0, 0},
    };
    UNITTEST_PROLOG_RAND;

    FOREACHDATA {
        TRACE("in=%d expected=%d", CDATA.in, CDATA.expected);
        assert(CDATA.in == CDATA.expected);
    }
}


UNUSED static int
test1(UNUSED int argc, UNUSED void *argv[])
{
    int res;
    res = mrkthr_sleep(2000);
    CTRACE("res=%d", res);
    return 0;
}


int
main(void)
{
    mrkthr_ctx_t *thr;
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

    if ((thr = mrkthr_new(NULL, test1, 0)) == NULL) {
        FAIL("mrkthr_new");
    }
    mrkthr_run(thr);

    mrkthr_loop();

    mrkdht_fini();

    mrkthr_fini();

    memdebug_print_stats();

    return 0;
}
