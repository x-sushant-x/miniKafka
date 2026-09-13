run:
	@ go build -o raft
	@ ./raft


pb:
	@ protoc \
		--go_out=. --go_opt=module=github.com/x-sushant-x/raft \
		--go-grpc_out=. --go-grpc_opt=module=github.com/x-sushant-x/raft \
		proto/raft.proto