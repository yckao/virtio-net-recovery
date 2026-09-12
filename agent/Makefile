CLANG ?= clang
CC ?= cc
MULTIARCH := $(shell $(CC) -dumpmachine)
CFLAGS := -O2 -g -Wall -Wextra

all: watch.bpf.o

watch.bpf.o: watch.bpf.c wire.h
	$(CLANG) -g -O2 -target bpf -D__TARGET_ARCH_x86 -I/usr/include/$(MULTIARCH) -c $< -o $@

libwatch.so: native.c wire.h
	$(CC) $(CFLAGS) -shared -fPIC $< -o $@ -lbpf -lelf -lz

clean:
	rm -f watch.bpf.o libwatch.so

.PHONY: all clean
