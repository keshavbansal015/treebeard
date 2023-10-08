# oblishard
A scalable and fault-tolerant Oblivious RAM (ORAM) data store

## Table of Contents

1. [Protocol Buffers](#protocol-buffers)
2. [Running the Project](#running-the-project)
   1. [Running the Router](#running-the-router)
   2. [Running the Shardnode](#running-the-shardnode)
   3. [Running the ORAM Node](#running-the-oram-node)
   4. [Running the Jaeger Backend](#running-the-jaeger-backend)
   5. [Running Redis](#running-redis)
   6. [Sending Requests](#sending-requests)
3. [Example Execution](#example-execution)
4. [Configurations](#configurations)

---
## Protocol Buffers

The following parts are generated, but if you want to generate them again yourself, you can run the provided script:

```bash
./scripts/generate_protos.sh
```

## Running the Project
### Running the Router
To run the router, use the following command:
```bash
go run -race . -routerid <routerid> -port <rpc port>
```
### Running the Shardnode
To run a shardnode, use the following command:
```bash
go run -race . -shardnodeid <id> -rpcport <rpcport> -replicaid <replicaid> -raftport <raftport>
```
You can provide a `-joinaddr` for the followers to join the leader.
### Running the ORAM Node
To run an ORAM node, use the following command:
```bash
go run -race . -oramnodeid <id> -rpcport <rpcport> -replicaid <replicaid> -raftport <raftport>
```
You can provide a `-joinaddr` for the followers to join the leader.
### Running the Jaeger Backend
To run the Jaeger backend, use Docker Compose:
```bash
docker compose up
```
### Running Redis
Ensure that Redis is up and running on the default port.
### Sending Requests
To send requests, run the following command in the `cmd/client` directory:
```bash
go run -race .
```
It will send the requests that are in the `traces/simple.trace` file.
## Example Execution
Run each of the commands in a new terminal:
```bash
go run -race . -routerid 0 -port 8745 #cmd/router directory
go run -race . -shardnodeid 0 -rpcport 8748 -replicaid 0 -raftport 3124 #cmd/shardnode directory
go run -race . -shardnodeid 0 -rpcport 8749 -replicaid 1 -raftport 3125 -joinaddr=127.0.0.1:8748 #cmd/shardnode directory
go run -race . -shardnodeid 0 -rpcport 8750 -replicaid 2 -raftport 3126 -joinaddr=127.0.0.1:8748 #cmd/shardnode directory
go run -race . -oramnodeid 0 -rpcport 8751 -replicaid 0 -raftport 1415 #cmd/oramnode directory
go run -race . -oramnodeid 0 -rpcport 8752 -replicaid 1 -raftport 1416 -joinaddr=127.0.0.1:8751 #cmd/oramnode directory
go run -race . -oramnodeid 0 -rpcport 8753 -replicaid 2 -raftport 1417 -joinaddr=127.0.0.1:8751 #cmd/oramnode directory
docker compose up #jaeger directory
go run -race . #client directory
```
## Configurations
The configurations can be found in the configs directory.  
There are files for endpoint configs, as well as one file for the parameters.
