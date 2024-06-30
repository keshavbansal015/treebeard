# Treebeard [![Github Actions](https://github.com/dsg-uwaterloo/treebeard/actions/workflows/go.yml/badge.svg)](https://github.com/dsg-uwaterloo/treebeard/actions/workflows/go.yml)
Welcome to **Treebeard**, a fault-tolerant and highly scalable Oblivious RAM (ORAM) data store designed to provide strong security guarantees by hiding access patterns from the storage server and randomizing client access.
## Key Features:
* **Scalability**: Treebeard is designed with horizontal scalability in mind, enabling scaling the system without leaking security information.
* **Fault Tolerance**: Using the Raft consensus algorithm, Treebeard ensures the resilience of your data store even in the face of node crashes or failures, enhancing the overall reliability of your system.
* **High Throughput**: Treebeard achieves high-throughput performance, ensuring that your applications can handle large volumes of data seamlessly and efficiently.
* **Configurable**: Treebeard recognizes the diverse needs of different projects and offers a high level of configurability to adapt to your specific requirements.
* **Easy to Deploy**: We've designed the setup of Treebeard to be straightforward, ensuring that you can integrate our secure and high-performance data store effortlessly into your projects.
* **Easy to Extend**: Treebeard's modular architecture makes it easy to extend functionality. You can add new storage layers to Treebeard to support different use cases.

## Installation
### Protocol Buffers

The following parts are generated, but if you want to generate them again yourself, you can run the provided script:

```bash
./scripts/generate_protos.sh
```
### Dependencies
You will need Go 1.20 to run the project.

## Running the Experiments
Each of the directories in the `experiments` directory contains several experiments. For example, dist_experiments has four subdirectories (uniform, zipf0.2, zipf0.6, zipf0.8, zipf0.99). The `run_scripts.sh` file in the `experiments` directory runs all of the experiments.  
**To run all the experiments:**
```bash
./experiments/run_experiments.sh
```
This script uses `ansible/deploy.yaml` and `ansible/experiment.yaml` to deploy the experiment and run the experiment. It then gathers the results on the local folder.
Each experiment should have a trace.txt file, which is the generated YCSB trace file. These are not provided since they are very large. You can generate them using the [YCSB]([url](https://github.com/brianfrankcooper/YCSB)) client.

### Experiment Details
Every experiment has its own configuration files. The following are the current configurations:
* jaeger_endpoints.yaml: configures the jaeger backend for collecting the distributed tracing telemetries.
* oramnode_endpoints.yaml: endpoints for the ORAM services.
* shardnode_endpoints.yaml: endpoints for the Shard node services.
* router_endpoints.yaml: endpoints for the router services.
* redis_endpoints.yaml: endpoints for the redis services.
* **parameters.yaml**: configurable parameters for each experiment. The comments explain what each configurable variable does.

Feel free to change the files to add a new experiment.
