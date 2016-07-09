noinst_PROGRAMS = mgas_mm
mgas_mm_SOURCES = mm.c
mgas_mm_CFLAGS  = -I$(top_srcdir)/mgas/include
mgas_mm_LDFLAGS = -lstdc++
mgas_mm_LDADD   = $(top_builddir)/comm/src/libmcomm.la \
                  $(top_builddir)/mgas/src/libmgas.la
