PROTOBUF_PATH=../api

mkdir -p $PROTOBUF_PATH/router
protoc --proto_path=$PROTOBUF_PATH --go_out=$PROTOBUF_PATH/router --go_opt=paths=source_relative --go-grpc_out=$PROTOBUF_PATH/router --go-grpc_opt=paths=source_relative router.proto

mkdir -p $PROTOBUF_PATH/shardnode
protoc --proto_path=$PROTOBUF_PATH --go_out=$PROTOBUF_PATH/shardnode --go_opt=paths=source_relative --go-grpc_out=$PROTOBUF_PATH/shardnode --go-grpc_opt=paths=source_relative shardnode.proto

mkdir -p $PROTOBUF_PATH/oramnode
protoc --proto_path=$PROTOBUF_PATH --go_out=$PROTOBUF_PATH/oramnode --go_opt=paths=source_relative --go-grpc_out=$PROTOBUF_PATH/oramnode --go-grpc_opt=paths=source_relative oramnode.proto
