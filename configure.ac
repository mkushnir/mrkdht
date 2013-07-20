AC_INIT(mrkdht, 0.1, markiyan.kushnir@gmail.com)
if test -z "$USE_GCC"
then
    CC="clang -Qunused-arguments -fcolor-diagnostics"
    CXX="clang++ -Qunused-arguments -fcolor-diagnostics"
    CPP="clang -E"
    AC_SUBST(CLANG_DEBUG, ['-ftrapv -fcatch-undefined-behavior'])
fi

AC_CONFIG_MACRO_DIR([m4])

#AC_LANG(C)

AC_PROG_CC
AC_PROG_CXX
AM_PROG_AR
AC_PROG_RANLIB
AC_PROG_INSTALL

LT_INIT
AM_INIT_AUTOMAKE

dnl AM_MAINTAINER_MODE

AC_CONFIG_FILES([Makefile src/Makefile src/test/Makefile man/Makefile])
AC_CONFIG_HEADERS(config.h)

AC_CHECK_HEADERS([syslog.h])

AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T

AC_FUNC_MALLOC
AC_FUNC_REALLOC

AC_PRESERVE_HELP_ORDER

AC_ARG_ENABLE(debug,
              AC_HELP_STRING([--enable-debug],
                             [Enable debug build (default=no)]),
              [AM_CONDITIONAL([DEBUG], [enable_debug=yes])],
              [AM_CONDITIONAL([DEBUG], [test "$enable_debug" = "yes"])])

# pass LDFLAGS=-L${LOCALBASE}/lib in the environment
AC_CHECK_LIB(mrkcommon, _fini, [], [AC_MSG_FAILURE(libmrkcommon.so is required. Please find it at git@github.com:mkushnir/mrkcommon.git)]) 
AC_CHECK_LIB(mrkthr, _fini, [], [AC_MSG_FAILURE(libmrkthr.so is required. Please find it at git@github.com:mkushnir/mrkthr.git)]) 
AC_CHECK_LIB(mrkdata, _fini, [], [AC_MSG_FAILURE(libmrkdata.so is required. Please find it at git@github.com:mkushnir/mrkdata.git)]) 
AC_CHECK_LIB(mrkrpc, _fini, [], [AC_MSG_FAILURE(libmrkrpc.so is required. Please find it at git@github.com:mkushnir/mrkrpc.git)]) 

AC_OUTPUT
