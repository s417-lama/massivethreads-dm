noinst_PROGRAMS = bin
bin_SOURCES     = bin.cc
bin_CXXFLAGS    = -I$(top_srcdir)/comm/include \
		  -I$(top_srcdir)/uth/include \
		  -I$(top_srcdir)/mgas/include \
                  -I$(top_srcdir)/madm/include
bin_LDADD       = $(top_builddir)/comm/src/libmcomm.la \
		  $(top_builddir)/uth/src/libuth.la \
		  $(top_builddir)/mgas/src/libmgas.la \
		  $(top_builddir)/madm/src/libmadm.la
