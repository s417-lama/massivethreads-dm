noinst_PROGRAMS = stream
stream_SOURCES  = stream.c
stream_CFLAGS   = -I$(top_srcdir)/mgas/include
stream_LDFLAGS  = -lstdc++
stream_LDADD    = $(top_builddir)/comm/src/libmcomm.la \
                  $(top_builddir)/mgas/src/libmgas.la
