build:
	@ go build -o bin/

run: build
	@ ./bin/miniKafka

test:
	@ rm -rf .logs && go test -race ./...

build-mkt:
	@ go build -o bin/ mkt/mkt.go 

truncate-all:
	@ ./miniKafka --broker_id=1 --truncate_only=true
	@ ./miniKafka --broker_id=2 --truncate_only=true
	@ ./miniKafka --broker_id=3 --truncate_only=true