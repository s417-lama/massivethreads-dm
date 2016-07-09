
defines=

lib_LTLIBRARIES = libmgas.la
libmgas_la_SOURCES = \
    sys.c \
    globals.c \
    threadsafe.c \
    myth_alloc.c \
    comm.c \
    ga_alloc.c \
    gmt_entry.c \
    gmt.c \
    mgas_am.c \
    mgas_syncvar.c \
    localize_v.c \
    memory_v.c \
    memory.c \
    mgas_alloc.c \
    mgas.c

libmgas_la_CPPFLAGS  = -I$(top_srcdir)/mgas/include \
                        -I$(top_srcdir)/mgas/src \
                        -I$(top_builddir)/mgas/include \
                        -Wundef \
			$(defines)
libmgas_la_CXXFLAGS  =
libmgas_la_CCASFLAGS =
libmgas_la_LDFLAGS   =

