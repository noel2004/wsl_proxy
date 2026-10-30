GO ?= go
WINDOWS_ARCH ?= amd64

.PHONY: check fmt fmt-check test vet build build-windows

check: fmt-check vet test build build-windows

fmt:
	$(GO) fmt ./...

fmt-check:
	@files=$$(gofmt -l client internal) || exit $$?; \
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

# Compile existing packages for Windows; no server executable exists yet.
build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=$(WINDOWS_ARCH) $(GO) build ./...
