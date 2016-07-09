noinst_PROGRAMS  = stream
stream_SOURCES   = stream.cc
stream_CXXFLAGS  = -I$(top_srcdir)/comm/include \
                   -I$(top_srcdir)/uth/include \
                   -I$(top_srcdir)/mgas/include \
                   -I$(top_srcdir)/madm/include
stream_LDFLAGS   =
stream_LDADD     = $(top_builddir)/madm/src/libmadm.la \
                   $(top_builddir)/uth/src/libuth.la \
                   $(top_builddir)/mgas/src/libmgas.la
