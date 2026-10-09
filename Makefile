GO ?= go

.PHONY: all build linux bpf test check clean
all: build
build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/vhost-agent ./cmd/vhost-agent
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/vhost-faultlab ./cmd/vhost-faultlab
linux:
	$(MAKE) build GOOS=linux GOARCH=amd64
bpf:
	$(MAKE) -C vhost

test:
	$(GO) test -race ./...
check:
	$(GO) vet ./...
clean:
	rm -rf bin
	$(MAKE) -C vhost clean
