
output      := bitonic_sort
cxxsources  := $(output).cpp
build_dir   := .build
mgas_prefix := ../..

mthreads_prefix := $(HOME)/local/mthreads

-include $(mgas_prefix)/include/mgas-config.mk

CFLAGS += -I$(mthreads_prefix)/include

include $(mgas_prefix)/include/mgas-build.mk

