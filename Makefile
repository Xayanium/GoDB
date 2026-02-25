# Makefile
.PHONY: proto raft shardctrler shardkv

proto: raft shardctrler shardkv

raft:
	protoc --go_out=./rpc --go-grpc_out=./rpc proto/raft.proto
shardctrler:
	protoc --go_out=./rpc --go-grpc_out=./rpc proto/shardctrler.proto
shardkv:
	protoc --go_out=./rpc --go-grpc_out=./rpc proto/shardkv.proto