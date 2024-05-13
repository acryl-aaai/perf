#ifndef PERF_H
#define PERF_H

#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>

#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <time.h>
#include <stdbool.h>
#include <sys/time.h>

#define LOG_LEVEL 0

#if ((LOG_LEVEL) > 0)
  #define LOG_DEBUG(s, a...)  printf((s), ##a)
#else
  #define LOG_DEBUG(s, a...)
#endif
#define LOG_ERROR(s, a...)  printf((s), ##a)

#define MAX_TENANT_NUM 3000 //same as in perf_main.c
#define MAX_SGE_LEN 16

#define PERF_LARGE_FLOW 1024
#define PERF_BACKGROUND -1
#define PERF_BYPASS 0

#define MASTER_QP_DEPTH 1024

#define TENANT_SQ_CHECK_INTERVAL 5000 //us
#define TENANT_SQ_CHECK_WINDOW   1000000 //us
#define DELAY_SEN_NUM_TH         5 
#define DELAY_SEN_BYTES_TH    1024 //byte
#define TENANT_ACTIVE_CHECK_INTERVAL 10000 //us     
#define TENANT_INACTIVE_CHECK_INTERVAL 1000000 //us     

#define USE_MULTI_MQP 0

#define ALLOWED_QP_TIME_TH 10000 //us, 10ms
#define MAX_ALLOWED_QP_NUM 2 

//GLOBAL FUNC
void update_perf_state(struct ibv_qp *qp, uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t origin_max_send_wr, uint32_t origin_max_recv_wr, int sig_all);
void perf_set_dest_info(struct ibv_qp *qp, union ibv_gid dgid, int sgid_idx); 
int perf_process(struct ibv_qp *qp, struct ibv_send_wr *wr);
int perf_recv_process(struct ibv_qp *qp, struct ibv_recv_wr *wr);

void perf_bg_post(struct ibv_qp *qp, struct ibv_send_wr *wr);
void perf_bg_recv_post(struct ibv_qp *qp, struct ibv_recv_wr *wr);
void perf_poll_post(struct ibv_qp *qp, struct ibv_send_wr *wr, uint32_t size);
int perf_poll_cq(struct ibv_cq *cq, uint32_t ne, struct ibv_wc *wc, int cqe_ver);
void perf_preemption_process(struct ibv_qp* qp, uint32_t nreq, bool is_read);

void perf_destroy_qp();

//LOCAL FUNC

void perf_update_active_state(uint32_t q_idx);
void perf_init_pause_state();
bool perf_check_paused(uint32_t q_idx);
void perf_allowed_release(uint32_t q_idx);
void perf_wr_queue_manage();
void perf_recv_wr_queue_manage();
bool perf_large_process(uint32_t q_idx, struct ibv_send_wr *wr, uint64_t size);
bool perf_small_process(uint32_t q_idx, struct ibv_send_wr *wr, uint32_t nreq);
bool perf_large_recv_process(uint32_t q_idx, struct ibv_recv_wr *wr, uint64_t size);

void load_perf_config();
void update_qp_ctx(struct ibv_qp *qp, uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t origin_max_send_wr, uint32_t origin_max_recv_wr, int sig_all);
void update_cq_ctx(struct ibv_qp *qp, uint32_t max_send_wr);
void update_mqp_ctx();
void update_tenant_ctx();
void perf_create_read_qp();
void perf_early_poll_cq();
void perf_create_master_qp();
void perf_mqp_process();
void perf_update_tenant_state();

void enqueue_wr(uint32_t q_idx, struct ibv_send_wr *wr);
struct ibv_send_wr* get_queued_wr(uint32_t q_idx, uint32_t pwr_idx);
void dequeue_wr(uint32_t q_idx, uint32_t num);

void enqueue_recv_wr(uint32_t q_idx, struct ibv_recv_wr *wr);
struct ibv_recv_wr* get_queued_recv_wr(uint32_t q_idx, uint32_t pwr_idx);
void dequeue_recv_wr(uint32_t q_idx, uint32_t num);

void send_read_request(uint32_t q_idx, struct ibv_send_wr *wr);
void* get_next_read_header();
void process_read_request(void* req_ptr, struct ibv_send_wr* wr, struct ibv_send_wr* comp_wr);
void post_cmd_recv_wrs(uint32_t polled);
int perf_poll_read_cmd();
void perf_resp_read_cmd();
void perf_send_read_report(uint32_t q_idx);

void perf_thread_end();
void* perf_thread(void *para);

void* perf_exchange_read_qpn();
void* perf_exchange_read_qpn2();


//EXP is not supported yet.
int perf_exp_process(struct ibv_qp *qp, struct ibv_exp_send_wr *wr);
void perf_exp_small_process(uint32_t q_idx, struct ibv_exp_send_wr *wr, uint32_t nreq);
void perf_exp_large_process(uint32_t q_idx, struct ibv_exp_send_wr *wr, uint64_t size);
void perf_exp_bg_post(struct ibv_qp *qp, struct ibv_exp_send_wr *wr);
void perf_exp_poll_post(struct ibv_qp *qp, struct ibv_exp_send_wr *wr, uint32_t size);
int perf_exp_recv_process(struct ibv_qp *qp, struct ibv_recv_wr *wr);
void perf_exp_bg_recv_post(struct ibv_qp *qp, struct ibv_recv_wr *wr);
int perf_exp_poll_cq(struct ibv_cq *cq, uint32_t ne, struct ibv_exp_wc *wc, uint32_t wc_size, int cqe_ver);


/*
void perf_sent(struct ibv_qp *qp);
void perf_comp(uint32_t wr_id);
*/

//Structures 

struct perf_tenant_context {
  uint32_t sq_history_len;
  uint32_t* sq_history;
  uint32_t sq_ins_idx;
  uint32_t sq_max_idx;
  
  struct timeval last_sq_check_time;

  bool delay_sensitive;
  bool small_msg_sending;
  double avg_msg_size;
  uint64_t max_msg_size;
  uint64_t max_recv_msg_size;

  struct timeval last_active_check_time;
  bool is_active;
  uint32_t active_qps_num;
  pthread_mutex_t active_lock;

  bool is_first_wait;
  uint32_t *waiting_qps;
  uint32_t waiting_head;
  uint32_t waiting_tail;

  struct timeval* enabled_time;
  uint32_t* enabled_qps;
  uint32_t enabled_head;
  uint32_t enabled_tail;

  uint32_t additional_enable_num;

  uint32_t wait_num;
  uint32_t enable_num;

  uint32_t* paused_qps;
  uint32_t  paused_qps_head;
  uint32_t  paused_qps_tail;

  uint32_t* allowed_qp;
  uint32_t allowed_qps_num;

  pthread_mutex_t wait_lock;
  pthread_mutex_t allow_lock;

  bool resp_read;
  bool passive_reading;
  bool passive_delay_sensitive;
  bool passive_msg_sensitive;
  struct ibv_qp *rrqp;

  bool is_bw_read;

  pthread_mutex_t poll_lock;
  pthread_cond_t poll_cond;
};

struct perf_master_qp_context {
  uint8_t port_num;
  uint32_t gidx;

  struct ibv_qp *qp;
  struct ibv_cq *send_cq;
  struct ibv_cq *recv_cq;
  
  uint32_t manage_qnum;
  
  //posting to other QPs
  struct ibv_exp_send_wr wait_cqe_wr;
  struct ibv_exp_send_wr send_enable_wr;
  
  //posting to itself
  struct ibv_exp_send_wr self_wait_cqe_wr;
};

struct perf_qp_context {
  struct ibv_qp* qp;
  struct ibv_cq* zero_wait_cq;

  int sig_all;
  uint32_t max_wr;
  uint32_t max_recv_wr;

  struct ibv_exp_send_wr* dummy;

  uint32_t cq_num;
  
  bool is_active;

  struct ibv_send_wr* wr_queue;
  uint32_t wr_queue_head;
  uint32_t wr_queue_tail;
  atomic_int wr_queue_len;
  uint32_t wr_queue_size;
  
  struct ibv_exp_send_wr* exp_wr_queue;
  uint32_t exp_wr_queue_head;
  uint32_t exp_wr_queue_tail;
  atomic_int exp_wr_queue_len;

  struct ibv_recv_wr* recv_wr_queue;
  uint32_t recv_wr_queue_head;
  uint32_t recv_wr_queue_tail;
  atomic_int recv_wr_queue_len;
  uint32_t recv_wr_queue_size;

  
  bool is_paused;
  bool is_allowed;
  struct timeval last_allowed_time;
  uint32_t paused_idx;
  
  uint32_t post_num;

  uint64_t chunk_sent_bytes;
  
  bool is_first_wait;

  bool is_reading;
};

struct perf_cq_context {
  struct ibv_cq* cq;
  uint32_t max_cqe;
  atomic_int early_poll_num;
  
  void* wc_list;
  uint32_t wc_head;
  uint32_t wc_tail;

  pthread_mutex_t lock;
};

struct perf_read_qp_context {
  struct ibv_context *context;
  struct ibv_pd *pd;
  struct ibv_mr *mr;
  struct ibv_qp *qp;
  struct ibv_cq *cq;
  struct ibv_ah *ah;
  union ibv_gid dgid;
 
  uint32_t max_wr; 

  uint32_t remote_qpn;
  uint32_t port_num;

  //for crail
  struct ibv_ah *ah_list[2];
  uint32_t rqpn_list[2];

  void* read_queue; //Q_idx + Batch_num + read_request
  uint32_t read_queue_head;
  uint32_t read_queue_tail;
  uint32_t read_queue_len;
  uint32_t read_queue_size;

  void* next_read_header;

  struct ibv_send_wr cmd_wr;
  struct ibv_recv_wr cmd_recv_wr;

  struct ibv_recv_wr recv_wr;
  struct ibv_sge     recv_sge;

  struct ibv_send_wr* send_wrs;
  struct ibv_sge*     send_sge;
  uint32_t read_comp; 

  pthread_mutex_t read_queue_lock;
  pthread_mutex_t read_qp_con_lock;

};

struct perf_read_header { 
    uint32_t q_idx;
    uint32_t batch_num;
    uint64_t next_offset;
    uint8_t read_report;
    uint8_t read_delay_sensitive;
    uint8_t read_msg_sensitive;
};

struct perf_read_payload {
    uint64_t addr;
    uint32_t len;
    uint32_t lkey;

    uint64_t remote_addr;
    uint32_t rkey;
};

union perf_read_request {
  struct perf_read_header header;
  struct perf_read_payload payload;
};

struct perf_shm_context {
  uint32_t next_tenant_id;
  uint32_t tenant_num;
  uint32_t active_tenant_num;
  uint64_t active_qps_num;
  uint32_t active_stenant_num;
  uint32_t active_dtenant_num;
  uint32_t active_mtenant_num;
  uint32_t active_rrtenant_num;
  uint32_t max_qps_limit;

  uint32_t active_qps_per_tenant[MAX_TENANT_NUM];
  uint32_t additional_qps_num[MAX_TENANT_NUM];
  bool     msg_sensitive[MAX_TENANT_NUM];
  bool     delay_sensitive[MAX_TENANT_NUM];
  bool     resp_read[MAX_TENANT_NUM];
  uint64_t avg_msg_size[MAX_TENANT_NUM];
  bool     btenant_can_post[MAX_TENANT_NUM];


  pthread_mutex_t perf_thread_lock[MAX_TENANT_NUM];
  pthread_cond_t perf_thread_cond[MAX_TENANT_NUM];
  pthread_mutex_t lock;
};

#endif
