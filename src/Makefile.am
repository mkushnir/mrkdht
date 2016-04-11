AUTOMAKE_OPTIONS = foreign

distdir = ../$(PACKAGE)-$(VERSION)/src
dist_HEADERS = mrkdht_private.h

BUILT_SOURCES = diag.c diag.h
EXTRA_DIST = diag.txt gen-diag
CLEANFILES = $(BUILT_SOURCES) *.core
#CLEANFILES += *.in

lib_LTLIBRARIES = libmrkdht.la

nobase_include_HEADERS = mrkdht.h

noinst_HEADERS =

libmrkdht_la_SOURCES = mrkdht.c
nodist_libmrkdht_la_SOURCES = diag.c

if DEBUG
DEBUG_FLAGS = -g -O0 @CLANG_DEBUG@
else
DEBUG_FLAGS = -DNDEBUG -O3
endif

libmrkdht_la_CFLAGS = $(DEBUG_FLAGS) -Wall -Wextra -Werror -std=c99 -I$(includedir)

libmrkdht_la_LDFLAGS = -version-info 0:0:0 -L$(libdir) -lmrkcommon -lmrkthr -lmrkdata -lmrkrpc

SUBDIRS = . test

diag.c diag.h: diag.txt
	$(AM_V_GEN) sh ./gen-diag mrkdht

run: all
	for i in $(bin_PROGRAMS); do if test -x ./$$i; then LD_LIBRARY_PATH=$(libdir) ./$$i; fi; done;

testrun:
	for i in $(SUBDIRS); do if test "$$i" != "."; then cd $$i && $(MAKE) testrun && cd ..; fi; done;
