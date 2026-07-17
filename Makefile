build:
	@ go build -o bin/

run: build
	@ ./bin/miniKafka

test:
	@ rm -rf .logs && go test -race ./...

build-mkt:
	@ go build -o bin/ mkt/mkt.go 