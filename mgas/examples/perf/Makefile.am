noinst_PROGRAMS = perf
perf_SOURCES    = perf.c
perf_CFLAGS     = -I$(top_srcdir)/mgas/include
perf_LDFLAGS    = -lstdc++
perf_LDADD      = $(top_builddir)/comm/src/libmcomm.la \
                  $(top_builddir)/mgas/src/libmgas.la
