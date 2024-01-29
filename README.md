# Oblishard [![Github Actions](https://github.com/dsg-uwaterloo/oblishard/actions/workflows/go.yml/badge.svg)](https://github.com/dsg-uwaterloo/oblishard/actions/workflows/go.yml)
Welcome to **Oblishard**, a fault-tolerant and highly scalable Oblivious RAM (ORAM) data store designed to provide strong security guarantees by hiding access patterns from the storage server and randomizing client access.
## Key Features:
* **Scalability**: Oblishard is designed with horizontal scalability in mind, enabling scaling the system without leaking security information.
* **Fault Tolerance**: Using the Raft consensus algorithm, Oblishard ensures the resilience of your data store even in the face of node crashes or failures, enhancing the overall reliability of your system.
* **High Throughput**: Oblishard achieves high-throughput performance, ensuring that your applications can handle large volumes of data seamlessly and efficiently.
* **Configurable**: Oblishard recognizes the diverse needs of different projects and offers a high level of configurability to adapt to your specific requirements.
* **Easy to Deploy**: We've designed the setup of Oblishard to be straightforward, ensuring that you can integrate our secure and high-performance data store effortlessly into your projects.
* **Easy to Extend**: Oblishard's modular architecture makes it easy to extend functionality. You can add new storage layers to Oblishard to support different use cases.

## Installation
### Protocol Buffers

The following parts are generated, but if you want to generate them again yourself, you can run the provided script:

```bash
./scripts/generate_protos.sh
```

### Configurations
The configurations can be found in the configs directory.  
There are files for endpoint configs, as well as one file for the parameters.
* `*_endpoints.yaml`: These files include the endpoints of the different components in your system. You can specify the endpoints here and Ansible will use these files to deploy the components to their correct hosts and will configure them accordingly.
* `parameters.yaml`: This file holds all the configuration parameters. You can change them as you want to fit your needs.

### Running Oblishard
1. Ensure that you have Ansible installed, and you have ssh access to your deployment hosts.
2. Update the hosts file to include your deployment hosts.
3. Run `ansible-playbook -i /path/to/hosts tasks.yaml` in the deploy directory.
