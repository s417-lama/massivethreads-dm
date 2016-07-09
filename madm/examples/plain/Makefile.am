noinst_PROGRAMS = plain
plain_SOURCES   = plain.cc
plain_CXXFLAGS  = -I$(top_srcdir)/comm/include \
                  -I$(top_srcdir)/uth/include \
                  -I$(top_srcdir)/mgas/include \
                  -I$(top_srcdir)/madm/include
plain_LDFLAGS   = 
plain_LDADD     = $(top_builddir)/madm/src/libmadm.la \
                  $(top_builddir)/uth/src/libuth.la \
                  $(top_builddir)/mgas/src/libmgas.la
