export PERF_NIC_LINK_BW=100000 PERF_NIC_MSG_RATE=12400  PERF_NIC_QPS_CAPA=9 PERF_MSEN_QP_LIMIT=1 PERF_MAX_SIM_BTENANT_NUM=1

sudo rm /dev/shm/perf-shm
gcc perf_main.c -o perf_main -lpthread -lrt -lm
taskset -c 0 ./perf_main
