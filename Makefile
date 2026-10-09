GO ?= go
MODULES := modules/recovery-core modules/recovery-evidence modules/qemu-discovery modules/vhost-linux apps/vhost-agent apps/vhost-faultlab

.PHONY: all build linux bpf test check independent clean
all: build
build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/vhost-agent ./apps/vhost-agent/cmd/vhost-agent
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/vhost-faultlab ./apps/vhost-faultlab/cmd/vhost-faultlab
linux:
	$(MAKE) build GOOS=linux GOARCH=amd64
bpf:
	$(MAKE) -C modules/vhost-linux

test:
	$(GO) test -race ./tools/...
	$(GO) vet ./tools/...
	@set -eu; for module in $(MODULES); do (cd $$module && $(GO) test -race ./... && $(GO) vet ./...); done
check:
	$(GO) run ./tools/check-boundaries
independent:
	python3 tools/test-independent.py
clean:
	rm -rf bin
	$(MAKE) -C modules/vhost-linux clean
