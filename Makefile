build:
	@ go build -o bin/

run: build
	@ ./bin/miniKafka

test:
	@ rm -rf .logs && go test ./...