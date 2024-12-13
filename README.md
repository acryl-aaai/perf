# PeRF: Preemption-enabled RDMA Framework
This project is accepted in USENIX ATC'24 ([PeRF: Preemption-enabled RDMA Framework](https://www.usenix.org/conference/atc24/presentation/lee)).

## Overview
 
PeRF is a software-based framework designed to enhance performance isolation and maintain bare-metal RDMA performance in multi-tenant cloud environments. Utilizing a novel preemption mechanism at the RNIC level, PeRF dynamically manages RDMA resource allocation without the need for accurate estimation of network resources, ensuring optimal utilization and preventing performance degradation.
 
## Tested Environment
 
- **OS**: Ubuntu 20.04 (5.4.0-100-generic)
- **Hardware**: RDMA-capable network interface cards (RNICs: NVIDIA Mellanox ConnectX-4/5/6)
- **Driver**: Mellanox Driver (MLNX_OFED_LINUX-4.9-4.1.7.0-ubuntu20.04-x86_64)
Note that we have made modifications to **libmlx5** which supports NVIDIA ConnectX-4 onwards adapter cards.
 
## Getting Started
 
### Installation
 
Clone the repository and compile the PeRF framework:
 
```bash
git clone https://github.com/acryl-aaai/perf.git
cd perf
./build_perf.sh
```
You can get back to the original driver mode by executing ```./build_origin.sh```.
 
### Configuration 
 
Every node needs to run **perf_main** in advance. 
For a set of terminal window of **perf_main**:
```
export PERF_NIC_LINK_BW=100000 PERF_NIC_MSG_RATE=12400  PERF_NIC_QPS_CAPA=9
```
| Environment Variable | Description | Recommended Value |
| :---: | :--- | ---: |
| PERF_NIC_LINK_BW | Maximum bandwidth (in Mega Bytes) of the NIC. | 40000 (40G)<br>100000 (100G)<br>... |
| PERF_NIC_MSG_RATE | Maximum message processing capacity per single QP (in Kilo Messages). The value cited in the paper corresponds to the performance achieved when sending 16 KB messages in batches of 32 on a connectX-6 adapter. | hardware-specific |
| PERF_NIC_QPS_CAPA<br>(*QNum<sub>capa*) | Minimum number of QPs required to fully saturate the maximum message rate of the NIC. The cited value in the paper was obtained by incrementally increasing the number of QPs sending 16 KB messages in batches of 32 on a connectX-6 adapter until reaching the maximum message rate. | hardware-specific |
 
 
Next, you must carefully set the PeRF parameters for each tenant (in respective terminal window).
 
On server node:
```
export PERF_ENABLE=1 PERF_IS_SERVER=1 PERF_GIDX={gid_idx} PERF_REMOTE_IP={remote_ip} PERF_CHUNK_SIZE=16384 PERF_DUMMY_FACTOR=1024 PERF_READ_PORT=2111 PERF_IB_PORT=1 TB_TARGET_RATE=0
```
 
On client node:
 
```
export PERF_ENABLE=1 PERF_IS_SERVER=0 PERF_GIDX={gid_idx} PERF_REMOTE_IP={remote_ip} PERF_CHUNK_SIZE=16384 PERF_DUMMY_FACTOR=1024 PERF_READ_PORT=2111 PERF_IB_PORT=1 TB_TARGET_RATE=0
```
 
Make sure to fill in the correct values for the ```gid_idx``` and ```remote_ip``` fields. (```remote_ip``` is used for creating a channel for RPC-based READ operations.)
| Environment Variable | Description | Recommended Value |
| :---: | :--- | ---: |
| PERF_ENABLE |  Enables (1) or disables (0) the operational algorithm of the PeRF | 0 or 1 |
| PERF_IS_SERVER | Set to 1 for servers listening RDMA connections; set to 0 for clients requesting RDMA connections. | 0 or 1 |
| PERF_GIDX | Index of the GID to be used. (Find the appropriate GID from the list provided by the 'show_gids' command.) | - |
| PERF_CHUNK_SIZE<br>(*SUB_MSG_SIZE*) | The unit size (in Bytes) for splitting large messages to achieve message-level isolation. A smaller chunk size results in higher CPU overhead and reduced throughput. Instead, it improves the performance of applications sending small messages. For more detailed information, refer to Appendix A in our paper. | 16384 |
| PERF_DUMMY_FACTOR<br>(0_*WAIT_UNIT*) | A unit size (in Bytes) used to calculate the number of additional 0_WAIT WRs to be inserted when sending a single chunk. For example, if this value is 1KB, then when sending a 16KB chunk, 16KB / 1KB = 16, meaning that for each chunk sent, 16 0_WAIT WRs are generated and preempted. For more detailed information, refer to Appendix A in our paper. | 1024 |
| PERF_REMOTE_IP | IP address of the remote node. Used for READ operations. | - |
| PERF_READ_PORT | The UDP port used for RPC when performing RDMA READ operations. (User-defined port number) | - |
| PERF_IB_PORT | The NIC's port number to be used. | - |
| TB_TARGET_RATE | To use a token bucket, set this value greater than 0 (bytes per second). To disable the token bucket, set this value to 0. | - |
 
### Command Examples
Here are some commands for executing [Perftest](https://github.com/linux-rdma/perftest) applications.
 
To start applications using **single QP**,
* B_App<sub>single</sub>: ```ib_write_bw -F -q 1 -s 1048576 --run_infinitely```
* M_App<sub>single</sub>: ```ib_write_bw -F -q 1 -s 16 -l 32 --run_infinitely``` 
* D_App<sub>single</sub>: ```ib_write_lat -F -s 16 -n 5000000``` 
 
Note that the only difference in commands between server and client is that *client application must specify the server's IP address* in the command.
 
You can run applications that use multiple QPs by simply increasing the value of  ```-q``` option. However, applications can utilize only a single thread to take turns on QPs to post WRs in this case. Hence, we added modified version of Perftest to support multi-threading.
 
To start applications that support multi-threading, you need to build the modified Perftest as follows:
 
```
cd perftest-4.5.0-multhrd
./build.sh
```
 
After it is successfully built, use the following commands:
* B_App<sub>multi</sub>: ```./ib_write_bw -F -q 5 -s 1048576 --run_infinitely --thrd_per_qp```
* M_App<sub>multi</sub>: ```./ib_write_bw -F -q 10 -s 16 -l 32 --run_infinitely --thrd_per_qp``` 
* D_App<sub>multi</sub>: Not allowed (The Perftest does not support support multiple QPs in latency tests.)
 
Note that this version only supports ```--run_infinitely``` in *ib_write_bw*.
 
## Example Scenario
Here we simulate B_App<sub>single</sub> vs. D_App<sub>single</sub> between Node A (10.0.101.2) and Node B (10.0.102.2). Node A writes to Node B in this example.
 
1. Run **perf_main** on each node.
 
    In terminal #1 of Node A and B, respectively,
    
    ```
    ./perf_main
    ```
    
2. Run server applications (Node A):
 
    In terminal #2 of Node A,
    ```
    # B_App
    ib_write_bw -F -q 1 -s 1048576 --run_infinitely --report_gbits -p 9000
    ```
    
    In terminal #3 of Node A,
    
    ```
    # D_App
    ib_write_lat -F -s 16 -n 5000000 -p 9001
    ```
    
3. Run client applications (Node B):
 
    In terminal #2 of Node B,
    
    ```
    # B_App
    ib_write_bw -F -q 1 -s 1048576 --run_infinitely --report_gbits -p 9000 10.0.102.2
    ```
    
    In terminal #3 of Node B,
    
    ```
    # D_App
    ib_write_lat -F -s 16 -n 5000000 -p 9001 10.0.102.2
    ```
 
## Contributing
Contributions to PeRF are welcome. Please ensure to follow coding standards and submit pull requests for review.
 
## Support
For questions or support, please open an issue in the GitHub repository.
