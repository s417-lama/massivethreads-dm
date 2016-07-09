noinst_PROGRAMS = mm
mm_SOURCES  = mm.cc
mm_CXXFLAGS = -I$(top_srcdir)/comm/include \
              -I$(top_srcdir)/uth/include \
              -I$(top_srcdir)/mgas/include \
              -I$(top_srcdir)/madm/include
mm_LDFLAGS  =
mm_LDADD    = $(top_builddir)/madm/src/libmadm.la \
              $(top_builddir)/uth/src/libuth.la \
              $(top_builddir)/mgas/src/libmgas.la
              
