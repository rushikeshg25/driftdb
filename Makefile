.PHONY: build test vet race fmt clean demo

BIN := bin/driftdb

build:
	go build -o $(BIN) ./cmd/driftdb

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './bin/*')

clean:
	rm -rf bin

demo: build
	./examples/demo.sh
