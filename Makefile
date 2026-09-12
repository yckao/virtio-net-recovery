GO ?= go

.PHONY: all bpf build test clean
all: build

bpf:
	$(MAKE) -C agent watch.bpf.o

build: bpf
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o bin/vhost-watch ./cmd/vhost-watch
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o bin/vhost-fault ./cmd/vhost-fault

test:
	$(GO) test -race ./...

clean:
	rm -rf bin
	$(MAKE) -C agent clean
