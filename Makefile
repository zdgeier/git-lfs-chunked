VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test vet fmt install clean

build:
	go build -ldflags "-X main.version=$(VERSION)" -o bin/git-lfs-chunked .

install:
	go install -ldflags "-X main.version=$(VERSION)" .

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

clean:
	rm -rf bin dist
