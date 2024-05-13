
# PeRF

Preemption-enabled RDMA Framework

## Pre-requisite

PeRF is tested on Ubuntu 20.04 (5.4.0-100-generic) and Mellanox Driver (MLNX_OFED_LINUX-4.9-4.1.7.0-ubuntu20.04-x86_64).

## How to Build PeRF
```
git clone https://github.com/acryl-aaai/perf.git
cd perf
./build_perf.sh
```
You can get back to the original driver mode by executing ```./build_origin.sh```.


## How to Run PeRF

### Configuration 

Every node needs to run **perf_main** in advance. 
For a set of terminal window of **perf_main**:
```
export PERF_NIC_LINK_BW=100000 PERF_NIC_MSG_RATE=12400  PERF_NIC_QPS_CAPA=9 PERF_MSEN_QP_LIMIT=1 PERF_MAX_SIM_BTENANT_NUM=1
```
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
* D_App<sub>multi</sub>: Not allowing multiple QPs in latency tests.

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
