CLANG ?= clang
CFLAGS ?= -O2 -g -target bpf -D__TARGET_ARCH_x86
MULTIARCH ?= $(shell $(CC) -dumpmachine)
BPF_SOURCE = internal/kernel/assets/vhost.bpf.c

.PHONY: all generate test clean
all: build/vhost-observe.bpf.o build/vhost-trace.bpf.o
build:
	mkdir -p $@
build/vhost-observe.bpf.o: $(BPF_SOURCE) internal/kernel/assets/wire.h | build
	$(CLANG) $(CFLAGS) -I/usr/include/$(MULTIARCH) -DVHOST_TRACE=0 -c $< -o $@
build/vhost-trace.bpf.o: $(BPF_SOURCE) internal/kernel/assets/wire.h | build
	$(CLANG) $(CFLAGS) -I/usr/include/$(MULTIARCH) -DVHOST_TRACE=1 -c $< -o $@
generate:
	python3 internal/kernel/assets/generate_wire.py
	gofmt -w internal/kernel/wire_generated.go
test:
	GOWORK=off go test ./...
clean:
	rm -rf build
