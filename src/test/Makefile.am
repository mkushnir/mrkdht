AUTOMAKE_OPTIONS = foreign
CLEANFILES = *.core
#CLEANFILES += *.in

noinst_PROGRAMS=testfoo testping

distdir = ../../$(PACKAGE)-$(VERSION)/src/test
dist_HEADERS = unittest.h

BUILT_SOURCES = ../diag.c ../diag.h

noinst_HEADERS = ../mrkdht.h

if DEBUG
DEBUG_FLAGS = -g -O0 -ftrapv -fcatch-undefined-behavior
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

nodist_testfoo_SOURCES = ../diag.c

testfoo_SOURCES = testfoo.c
testfoo_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I.. -I$(includedir)
testfoo_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkdht -lmrkrpc -lmrkdata -lmrkthr

testping_SOURCES = testping.c
testping_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I.. -I$(includedir)
testping_LDFLAGS = -L$(libdir) -lmrkcommon -lmrkdht -lmrkrpc -lmrkdata -lmrkthr

../diag.c ../diag.h: ../diag.txt
	sh ../gen-diag

testrun: all
	for i in $(noinst_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;