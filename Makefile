GO ?= go
WINDOWS_ARCH ?= amd64

.PHONY: check fmt fmt-check test vet build build-windows test-windows

check: fmt-check vet test build build-windows

fmt:
	gofmt -w client cmd internal e2e-bench

fmt-check:
	@files=$$(gofmt -l client cmd internal e2e-bench) || exit $$?; \
	if [ -n "$$files" ]; then \
		printf 'Run make fmt for:\n%s\n' "$$files"; \
		exit 1; \
	fi

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

build:
	$(GO) build ./...

# Compile all Windows packages and the foreground executable, without CGO.
build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=$(WINDOWS_ARCH) $(GO) build ./...
	CGO_ENABLED=0 GOOS=windows GOARCH=$(WINDOWS_ARCH) $(GO) build -o bin/wsl-proxy-server-$(WINDOWS_ARCH).exe ./cmd/wsl-proxy-server

# WSL only: env launches the cross-compiled test binaries through Windows interop.
# WINDOWS_ARCH must match the Windows host (or an architecture it can execute).
test-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=$(WINDOWS_ARCH) $(GO) test -count=1 -exec env ./...
