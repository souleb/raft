GO_TEST_ARGS ?= GO111MODULE=on

.PHONY: proto
proto: ## Compile the proto file.
	protoc --go_out=. --go_opt=paths=source_relative \
	--go-grpc_out=. --go-grpc_opt=paths=source_relative \
	api/raft.proto

	.PHONY: unit-tests
unit-tests: ## Run unit tests.
	go test -v -cover $(GO_TEST_ARGS) ./...
	