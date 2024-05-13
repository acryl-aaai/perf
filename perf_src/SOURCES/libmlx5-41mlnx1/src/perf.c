#include "perf.h"
#include "khash.h"
#include <math.h>
#include <arpa/inet.h>
#include "mlx5.h"

uint64_t MAX_RATE = 12500000000;
uint64_t TENANT_TARGET_RATE = 12500000000;
uint64_t BURST_SIZE = 4000000;

void* token_bucket;

uint32_t CHUNK_SIZE = 8192; //BYTES
uint32_t DUMMY_FACTOR = 4096; // the number of dummy factor : 1 dummy per x bytes
uint32_t DUMMY_FACTOR_1 = 0;
uint32_t DUMMY_FACTOR_2 = 0;

uint32_t READ_PORT = 9999;   //For PeRF Read
uint32_t POST_STOP_NUM_TH = 128;   
uint32_t POST_STOP_BYTES_TH = 1024000; //BYTE
uint32_t POST_STOP_TIME_TH = 1000; //us

uint32_t MAX_READ_QP_NUM = 64;
uint32_t MAX_READ_BATCH_NUM = 1; //Batching Large Read is not supported yet!

KHASH_MAP_INIT_INT(qph, uint32_t);
KHASH_MAP_INIT_INT(cqh, uint32_t);

khash_t(qph)* qp_hash;
khash_t(cqh)* cq_hash;

int use_perf = -1;

static uint32_t tenant_id = -1;
static uint32_t global_qnum = 0;
static uint32_t global_cqnum = 0;
static uint32_t global_mqnum = 0;

cpu_set_t th_cpu;
pthread_attr_t th_attr;

pthread_t daemon_thread;
static uint32_t new_qp_create = 0;

bool read_qp_connected = 0;
bool rc_used = 0;
char remote_addr[16];  

//for crail
#define CRAIL_NAMENODE 0
#define CRAIL_DATANODE 1
#define CRAIL_CLIENT 2

bool crail = 0;
uint32_t crail_type = 0;

bool manage_stop;
bool bw_send = false;

//uint64_t* sorted_min_left;
struct perf_tenant_context tenant_ctx;
struct perf_master_qp_context* mqp_ctx = NULL;
struct perf_qp_context* qp_ctx = NULL;
struct perf_cq_context* cq_ctx = NULL;
struct perf_shm_context* shm_ctx = NULL;
struct perf_read_qp_context read_qp_ctx;

extern void* tb_create(uint64_t max_rate, uint64_t target_rate, uint64_t burstSize);
extern bool tb_consume(void*p, uint64_t tokens);

void wr_copy(struct ibv_send_wr* dest_wr, struct ibv_send_wr* src_wr)
{
  struct ibv_sge* origin_sg_list = dest_wr->sg_list;
  
  memcpy(dest_wr, src_wr, sizeof(struct ibv_send_wr));

  dest_wr->sg_list = origin_sg_list;
  memcpy(dest_wr->sg_list, src_wr->sg_list, sizeof(struct ibv_sge) * src_wr->num_sge);
}

void enqueue_wr(uint32_t q_idx, struct ibv_send_wr* wr)
{
  if(qp_ctx[q_idx].wr_queue_len == qp_ctx[q_idx].wr_queue_size)
  {
    LOG_ERROR("Error! Waiting SGE Queue is full, cannot enqueue: %d %d %d\n", q_idx, qp_ctx[q_idx].wr_queue_len, qp_ctx[q_idx].wr_queue_size);
    exit(1);
  }

  wr_copy(&(qp_ctx[q_idx].wr_queue[qp_ctx[q_idx].wr_queue_tail]), wr);
  //LOG_ERROR("Enqueue: %d %d\n", qp_ctx[q_idx].wr_queue[qp_ctx[q_idx].wr_queue_tail].sg_list[0].length, wr->sg_list[0].length);

  uint32_t suc_idx = qp_ctx[q_idx].wr_queue_tail - 1;
  if(qp_ctx[q_idx].wr_queue_tail == 0)
    suc_idx =  qp_ctx[q_idx].wr_queue_size - 1;

  if(qp_ctx[q_idx].wr_queue_len > 0 && qp_ctx[q_idx].wr_queue[suc_idx].next != NULL)
    qp_ctx[q_idx].wr_queue[suc_idx].next = &(qp_ctx[q_idx].wr_queue[qp_ctx[q_idx].wr_queue_tail]);

  qp_ctx[q_idx].wr_queue_tail = (qp_ctx[q_idx].wr_queue_tail + 1) % qp_ctx[q_idx].wr_queue_size;
  qp_ctx[q_idx].wr_queue_len++;

  //LOG_ERROR("%d Enqueue WR: %d\n", q_idx, qp_ctx[q_idx].wr_queue_len);
}

struct ibv_send_wr* get_queued_wr(uint32_t q_idx, uint32_t pwr_idx)
{
  if(qp_ctx[q_idx].wr_queue_len <= pwr_idx)
  {
    return NULL;
    /*
    LOG_ERROR("Error! Waiting SGE Queue is empty, cannot get_queued_wr\n");
    exit(1);
    */
  }

  //LOG_ERROR("%d GET WR: %d, %p\n",q_idx, (qp_ctx[q_idx].wr_queue_head + pwr_idx) % qp_ctx[q_idx].wr_queue_size, &(qp_ctx[q_idx].wr_queue[(qp_ctx[q_idx].wr_queue_head + pwr_idx) % qp_ctx[q_idx].wr_queue_size]));
  return &(qp_ctx[q_idx].wr_queue[(qp_ctx[q_idx].wr_queue_head + pwr_idx) % qp_ctx[q_idx].wr_queue_size]);
}

void dequeue_wr(uint32_t q_idx, uint32_t num)
{
  if(qp_ctx[q_idx].wr_queue_len < num)
  {
    LOG_ERROR("Error! Waiting SGE Queue is empty, cannot dequeue\n");
    exit(1);
  }

  qp_ctx[q_idx].wr_queue_head = (qp_ctx[q_idx].wr_queue_head + num) % qp_ctx[q_idx].wr_queue_size;
  qp_ctx[q_idx].wr_queue_len -= num;
  
  //LOG_ERROR("%d Dequeue WR: %d\n", q_idx, qp_ctx[q_idx].wr_queue_len);
}

void recv_wr_copy(struct ibv_recv_wr* dest_wr, struct ibv_recv_wr* src_wr)
{
  struct ibv_sge* origin_sg_list = dest_wr->sg_list;
  
  memcpy(dest_wr, src_wr, sizeof(struct ibv_recv_wr));

  dest_wr->sg_list = origin_sg_list;
  memcpy(dest_wr->sg_list, src_wr->sg_list, sizeof(struct ibv_sge) * src_wr->num_sge);
}

void enqueue_recv_wr(uint32_t q_idx, struct ibv_recv_wr* wr)
{
  
  if(qp_ctx[q_idx].recv_wr_queue_len == qp_ctx[q_idx].recv_wr_queue_size)
  {
    LOG_ERROR("Error! Waiting SGE Queue is full, cannot enqueue\n");
    exit(1);
  }

  recv_wr_copy(&(qp_ctx[q_idx].recv_wr_queue[qp_ctx[q_idx].recv_wr_queue_tail]), wr);
  LOG_DEBUG("Enqueue: %d %d\n", qp_ctx[q_idx].recv_wr_queue[qp_ctx[q_idx].recv_wr_queue_tail].sg_list[0].length, wr->sg_list[0].length);

  uint32_t suc_idx = qp_ctx[q_idx].recv_wr_queue_tail - 1;
  if(qp_ctx[q_idx].recv_wr_queue_tail == 0)
    suc_idx =  qp_ctx[q_idx].recv_wr_queue_size - 1;

  if(qp_ctx[q_idx].recv_wr_queue_len > 0 && qp_ctx[q_idx].recv_wr_queue[suc_idx].next != NULL)
    qp_ctx[q_idx].recv_wr_queue[suc_idx].next = &(qp_ctx[q_idx].recv_wr_queue[qp_ctx[q_idx].recv_wr_queue_tail]);

  qp_ctx[q_idx].recv_wr_queue_tail = (qp_ctx[q_idx].recv_wr_queue_tail + 1) % qp_ctx[q_idx].recv_wr_queue_size;
  qp_ctx[q_idx].recv_wr_queue_len++;

  //LOG_ERROR("Enqueue WR: %d\n", qp_ctx[q_idx].recv_wr_queue_len);
}

struct ibv_recv_wr* get_queued_recv_wr(uint32_t q_idx, uint32_t pwr_idx)
{
  if(qp_ctx[q_idx].recv_wr_queue_len <= pwr_idx)
  {
    LOG_ERROR("Error! Waiting SGE Queue is empty, cannot get_queued_wr\n");
    return NULL;
  }

  //LOG_ERROR("GET WR: %d, %p\n", (qp_ctx[q_idx].recv_wr_queue_head + pwr_idx) % qp_ctx[q_idx].recv_wr_queue_size, &(qp_ctx[q_idx].recv_wr_queue[(qp_ctx[q_idx].recv_wr_queue_head + pwr_idx) % qp_ctx[q_idx].recv_wr_queue_size]));
  return &(qp_ctx[q_idx].recv_wr_queue[(qp_ctx[q_idx].recv_wr_queue_head + pwr_idx) % qp_ctx[q_idx].recv_wr_queue_size]);
}

void dequeue_recv_wr(uint32_t q_idx, uint32_t num)
{
  if(qp_ctx[q_idx].recv_wr_queue_len < num)
  {
    LOG_ERROR("Error! Waiting SGE Queue is empty, cannot dequeue\n");
    exit(1);
  }

  qp_ctx[q_idx].recv_wr_queue_head = (qp_ctx[q_idx].recv_wr_queue_head + num) % qp_ctx[q_idx].recv_wr_queue_size;
  qp_ctx[q_idx].recv_wr_queue_len -= num;
  
  //LOG_ERROR("Dequeue WR: %d\n", qp_ctx[q_idx].recv_wr_queue_len);
}

void perf_thread_end()
{
  sleep(1);
  
  if(!use_perf)
    return;
 
  tenant_ctx.is_active = false;
  pthread_mutex_lock(&shm_ctx->lock);
  shm_ctx->active_qps_per_tenant[tenant_id] = 0;
  shm_ctx->delay_sensitive[tenant_id] = false;
  shm_ctx->msg_sensitive[tenant_id] = false;
  shm_ctx->resp_read[tenant_id] = false;
  shm_ctx->btenant_can_post[tenant_id] = true;
  pthread_mutex_unlock(&(shm_ctx->perf_thread_lock[tenant_id]));
  pthread_mutex_unlock(&shm_ctx->lock);

  use_perf = false;

  exit(1);
}

void* perf_thread(void *para)
{
  if(!use_perf)
    return NULL;
  
  signal(SIGKILL, perf_thread_end); // MUST be disabled when using CRAIL
  signal(SIGINT, perf_thread_end);

  while(1)
  {
    if(new_qp_create)
      pthread_exit(NULL);

    if(!rc_used || read_qp_connected)
    {
      perf_update_tenant_state();

      if(global_qnum > 1 && mqp_ctx && (global_qnum == mqp_ctx[0].manage_qnum || global_qnum == global_mqnum))
        perf_mqp_process();

      perf_recv_wr_queue_manage();

      while(perf_poll_read_cmd() || read_qp_ctx.max_wr - mlx5_get_rq_num(read_qp_ctx.qp))
      {
        perf_resp_read_cmd();
        tenant_ctx.resp_read = true;
      }

      if(!shm_ctx->btenant_can_post[tenant_id])
      {
        //printf("disable: %d\n", tenant_id);
        pthread_mutex_lock(&(shm_ctx->perf_thread_lock[tenant_id]));
        while(!shm_ctx->btenant_can_post[tenant_id])
          pthread_cond_wait(&(shm_ctx->perf_thread_cond[tenant_id]), &(shm_ctx->perf_thread_lock[tenant_id]));  
        pthread_mutex_unlock(&(shm_ctx->perf_thread_lock[tenant_id]));

        pthread_mutex_lock(&(tenant_ctx.poll_lock));
        pthread_cond_broadcast(&(tenant_ctx.poll_cond));
        pthread_mutex_unlock(&(tenant_ctx.poll_lock)); 
        //printf("enble: %d\n", tenant_id);
      }

      perf_wr_queue_manage();

      if(!shm_ctx->delay_sensitive[tenant_id] && !shm_ctx->msg_sensitive[tenant_id])
        perf_early_poll_cq();
    }
    usleep(0);
  }
  return NULL;
}

void load_perf_config()
{
  char* env;  
 
  env = getenv("TB_MAX_RATE");
  if(env)
    MAX_RATE = atol(env);

  env = getenv("TB_TARGET_RATE");
  if(env)
    TENANT_TARGET_RATE = atol(env);

  env = getenv("TB_BURST_SIZE");
  if(env)
    BURST_SIZE = atol(env);

  if(TENANT_TARGET_RATE != 0)
    token_bucket = tb_create(MAX_RATE, TENANT_TARGET_RATE, BURST_SIZE);

  env = getenv("PERF_ENABLE");

  LOG_DEBUG("PERF_ENABLE: %s\n", env);
  if(!env || atoi(env) == 0)
    use_perf = 0;
  else
  {
    use_perf = 1;
    atexit(perf_destroy_qp);

    qp_hash = kh_init(qph);
    cq_hash = kh_init(cqh);

    env = getenv("PERF_CHUNK_SIZE");
    if(env)
      CHUNK_SIZE = atoi(env);

    env = getenv("PERF_DUMMY_FACTOR");
    if(env)
      DUMMY_FACTOR_1 = atoi(env);
    
    DUMMY_FACTOR = DUMMY_FACTOR_1;

    env = getenv("PERF_DUMMY_FACTOR_2");
    if(env)
      DUMMY_FACTOR_2 = atoi(env);

    env = getenv("PERF_READ_PORT");
    if(env)
      READ_PORT = atoi(env);

    LOG_ERROR("CHUNK_SIZE: %d, DUMMY_FACTOR: %d, DUMMY_FACTOR_2: %d (MUST LARGER THAN DUMMY_FACTOR)\n", CHUNK_SIZE, DUMMY_FACTOR_1, DUMMY_FACTOR_2);

    int shm_fd = shm_open("/perf-shm", O_RDWR, 0);
    if(shm_fd == -1)
    {
      LOG_ERROR("Cannot load perf_shm\n");
      exit(1);
    }

    shm_ctx = (struct perf_shm_context*) mmap(NULL, sizeof(struct perf_shm_context), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

    if (shm_ctx == MAP_FAILED){
      LOG_ERROR("Error mapping shared memory perf_shm");
      exit(1);
    }

    pthread_mutex_lock(&shm_ctx->lock);
    tenant_id = shm_ctx->next_tenant_id;
    tenant_id = shm_ctx->next_tenant_id++;
    shm_ctx->tenant_num++;
    shm_ctx->active_qps_per_tenant[tenant_id] = 0;
    shm_ctx->additional_qps_num[tenant_id] = 0;
    LOG_ERROR("Set Tenant ID: %d\n", tenant_id);
    pthread_mutex_unlock(&shm_ctx->lock);

    CPU_ZERO(&th_cpu);
    CPU_SET(0, &th_cpu);
    //CPU_SET(1, &th_cpu);
    pthread_attr_init(&th_attr);
    pthread_attr_setaffinity_np(&th_attr, sizeof(th_cpu), &th_cpu);

    sigset_t tSigSetMask;
    sigaddset(&tSigSetMask, SIGALRM);
    pthread_sigmask(SIG_SETMASK, &tSigSetMask, NULL);
  }
}

void update_qp_ctx(struct ibv_qp *qp, uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t origin_max_send_wr, uint32_t origin_max_recv_wr, int sig_all)
{
  if(global_qnum >= 2 && mqp_ctx && ((USE_MULTI_MQP == true && global_qnum != global_mqnum) || (USE_MULTI_MQP == false && mqp_ctx[0].qp == NULL))) //triggered by master_qp
    return;

  if(global_qnum == 1 && !read_qp_ctx.qp)
    return;

  new_qp_create = 1;
  pthread_join(daemon_thread, NULL);
  new_qp_create = 0;

  int k = kh_get(qph, qp_hash, qp->qp_num);
  if(k != kh_end(qp_hash))
  {
    LOG_ERROR("Error! Duplicated QP is created\n");
    exit(1);
  }

  int ret;
  k = kh_put(qph, qp_hash, qp->qp_num, &ret);
  kh_value(qp_hash, k) = global_qnum;

  global_qnum++;
  uint32_t q_idx = global_qnum - 1;    

  qp_ctx = (struct perf_qp_context*)realloc(qp_ctx, global_qnum*sizeof(struct perf_qp_context));

  qp_ctx[q_idx].qp = qp;
  qp_ctx[q_idx].sig_all = sig_all;
 
  qp_ctx[q_idx].max_wr = max_send_wr;
  
  if(global_qnum == 1)
    read_qp_ctx.max_wr = origin_max_send_wr * MAX_READ_QP_NUM;

  qp_ctx[q_idx].max_recv_wr = max_recv_wr;

  qp_ctx[q_idx].wr_queue_size = origin_max_send_wr * 2 + 10;
  qp_ctx[q_idx].recv_wr_queue_size = origin_max_recv_wr * 2 + 10;

  qp_ctx[q_idx].wr_queue = (struct ibv_send_wr*)malloc(sizeof(struct ibv_send_wr) * qp_ctx[q_idx].wr_queue_size);

  qp_ctx[q_idx].wr_queue_head = 0;
  qp_ctx[q_idx].wr_queue_tail = 0;
  qp_ctx[q_idx].wr_queue_len = 0;
  
  qp_ctx[q_idx].recv_wr_queue = (struct ibv_recv_wr*)malloc(sizeof(struct ibv_recv_wr) * qp_ctx[q_idx].recv_wr_queue_size);
  qp_ctx[q_idx].recv_wr_queue_head = 0;
  qp_ctx[q_idx].recv_wr_queue_tail = 0;
  qp_ctx[q_idx].recv_wr_queue_len = 0;

  for(uint32_t i=0; i<qp_ctx[q_idx].wr_queue_size; i++)
    qp_ctx[q_idx].wr_queue[i].sg_list = (struct ibv_sge*)malloc(sizeof(struct ibv_sge) * MAX_SGE_LEN);


  for(uint32_t i=0; i<qp_ctx[q_idx].recv_wr_queue_size; i++)
    qp_ctx[q_idx].recv_wr_queue[i].sg_list = (struct ibv_sge*)malloc(sizeof(struct ibv_sge) * MAX_SGE_LEN);

  qp_ctx[q_idx].is_paused = true;
  qp_ctx[q_idx].paused_idx = -1;
  qp_ctx[q_idx].is_allowed = false;

  qp_ctx[q_idx].is_active = false;
  qp_ctx[q_idx].post_num = 0;
  qp_ctx[q_idx].is_first_wait = true;
 
  qp_ctx[q_idx].is_reading = false;
 
  if(q_idx == 0)
  {
    qp_ctx[q_idx].zero_wait_cq = ibv_create_cq(qp->context, 1, NULL, NULL, 0);
    struct ibv_exp_cq_attr cq_mod_attr = {
      .comp_mask       = IBV_EXP_CQ_ATTR_CQ_CAP_FLAGS,
      .cq_cap_flags    = IBV_EXP_CQ_IGNORE_OVERRUN
    };

    if (ibv_exp_modify_cq(qp_ctx[q_idx].zero_wait_cq, &cq_mod_attr, IBV_EXP_CQ_CAP_FLAGS)) {
      LOG_ERROR("Failed to modify send CQ\n");
      exit(1);
    }
  }
  else
    qp_ctx[q_idx].zero_wait_cq = qp_ctx[0].zero_wait_cq;

  qp_ctx[q_idx].dummy = (struct ibv_exp_send_wr*)malloc(sizeof(struct ibv_exp_send_wr) * (uint32_t)(CHUNK_SIZE/DUMMY_FACTOR_1));

  qp_ctx[q_idx].chunk_sent_bytes = 0;
  for(uint32_t i=0; i< (uint32_t)(CHUNK_SIZE/DUMMY_FACTOR_1); i++)
  {
    qp_ctx[q_idx].dummy[i].wr_id = -1;
    qp_ctx[q_idx].dummy[i].sg_list = NULL;
    qp_ctx[q_idx].dummy[i].num_sge = 0;
    qp_ctx[q_idx].dummy[i].exp_opcode = IBV_EXP_WR_CQE_WAIT;
    //qp_ctx[q_idx].dummy[i].exp_opcode = IBV_EXP_WR_NOP;
    qp_ctx[q_idx].dummy[i].exp_send_flags = 0;
    qp_ctx[q_idx].dummy[i].task.cqe_wait.cq = qp_ctx[0].zero_wait_cq;
    qp_ctx[q_idx].dummy[i].task.cqe_wait.cq_count = 0;

    if((crail && i % 8) || i % 16 == 0 || qp_ctx[q_idx].max_wr < 16)
      qp_ctx[q_idx].dummy[i].exp_send_flags = IBV_EXP_SEND_SIGNALED;
    else
      qp_ctx[q_idx].dummy[i].exp_send_flags = 0;

    if(i == (uint32_t)(CHUNK_SIZE/DUMMY_FACTOR_1) - 1)
    {
      qp_ctx[q_idx].dummy[i].exp_send_flags = IBV_EXP_SEND_SIGNALED;
      qp_ctx[q_idx].dummy[i].next = NULL;
    }
    else
      qp_ctx[q_idx].dummy[i].next = &(qp_ctx[q_idx].dummy[i+1]);
  }

  update_cq_ctx(qp, origin_max_send_wr);
  
  if(global_qnum == 1)
    update_tenant_ctx();
  else
  {
    tenant_ctx.waiting_qps = (uint32_t*)realloc(tenant_ctx.waiting_qps, sizeof(uint32_t) * global_qnum);
    tenant_ctx.paused_qps = (uint32_t*)realloc(tenant_ctx.paused_qps, sizeof(uint32_t) * global_qnum);
    tenant_ctx.enabled_qps = (uint32_t*)realloc(tenant_ctx.enabled_qps, sizeof(uint32_t) * global_qnum);
    tenant_ctx.enabled_time = (struct timeval*)realloc(tenant_ctx.enabled_time, sizeof(struct timeval) * global_qnum);
    for(uint32_t i=0; i<global_qnum; i++)
    {
      tenant_ctx.waiting_qps[i] = -1;
      tenant_ctx.enabled_qps[i] = -1;
      tenant_ctx.paused_qps[i] = -1;
    }
  }

  if(global_qnum >= 2)
    update_mqp_ctx();
  
  pthread_t read_qp_con_thread;
  pthread_t read_qp_con_thread2;
  pthread_create(&daemon_thread, &th_attr, perf_thread, NULL);
  
  if(qp->qp_type == IBV_QPT_RC)
    rc_used = 1;


  char* env;  
  env = getenv("PERF_USE_CRAIL");
  if(env)
    crail = atoi(env);

  if(qp->qp_type == IBV_QPT_RC && !read_qp_connected && pthread_mutex_trylock(&read_qp_ctx.read_qp_con_lock) == 0)
    pthread_create(&read_qp_con_thread, NULL, perf_exchange_read_qpn, NULL);
  else if(crail && (qp->qp_type == IBV_QPT_RC && pthread_mutex_trylock(&read_qp_ctx.read_qp_con_lock) == 0)) //for crail
  {
    read_qp_connected = 0;
    pthread_create(&read_qp_con_thread2, NULL, perf_exchange_read_qpn2, NULL);
  }
}

void update_cq_ctx(struct ibv_qp *qp, uint32_t max_send_wr)
{
  //LOG_ERROR("update_perf_cq_state()\n");
  //updqte cq_ctx  
  uint32_t cq_num = 0;
  uint32_t q_idx = global_qnum - 1;
  for(uint32_t i=0; i<global_qnum- 1; i++)
  {
    if(qp->send_cq != qp_ctx[i].qp->send_cq)
      cq_num++;
    else
    {
      cq_num = qp_ctx[i].cq_num;
      cq_ctx[cq_num].max_cqe += max_send_wr * 4;
      cq_ctx[cq_num].wc_list = (void*)realloc(cq_ctx[cq_num].wc_list, sizeof(struct ibv_exp_wc) * (cq_ctx[cq_num].max_cqe + 10));
      break;
    }
  }

  if(global_cqnum == cq_num)
  {
    cq_ctx = (struct perf_cq_context*)realloc(cq_ctx, (global_cqnum + 1)*sizeof(struct perf_cq_context));
    cq_ctx[cq_num].max_cqe = max_send_wr * 4;
    cq_ctx[cq_num].wc_list = (void*)malloc(sizeof(struct ibv_exp_wc) * (cq_ctx[cq_num].max_cqe + 10));
    cq_ctx[cq_num].cq = qp->send_cq;
    cq_ctx[cq_num].wc_head = 0;
    cq_ctx[cq_num].wc_tail = 0;
    cq_ctx[cq_num].early_poll_num = 0;
    pthread_mutex_init(&(cq_ctx[cq_num].lock), NULL);

    qp_ctx[q_idx].cq_num = cq_num;

    int ret;
    uint32_t k = kh_put(cqh, cq_hash, qp->send_cq->handle, &ret);
    kh_value(cq_hash, k) = cq_num;

    global_cqnum++;
  }
}

void update_tenant_ctx()
{
  //LOG_ERROR("update_perf_tenant_state()\n");

  tenant_ctx.sq_history_len = TENANT_SQ_CHECK_WINDOW / TENANT_SQ_CHECK_INTERVAL;
  tenant_ctx.sq_history = (uint32_t*)malloc(tenant_ctx.sq_history_len * sizeof(uint32_t));
  memset(tenant_ctx.sq_history, 0, sizeof(uint32_t) * tenant_ctx.sq_history_len);
  tenant_ctx.sq_ins_idx = 0;
  tenant_ctx.sq_max_idx = 0;

  gettimeofday(&(tenant_ctx.last_sq_check_time), NULL);

  tenant_ctx.delay_sensitive = true;
  tenant_ctx.avg_msg_size = 0;
  tenant_ctx.max_msg_size = 0;
  tenant_ctx.max_recv_msg_size = 0;

  tenant_ctx.is_active = false;
  tenant_ctx.active_qps_num = 0;

  gettimeofday(&(tenant_ctx.last_active_check_time), NULL);
  
  tenant_ctx.is_first_wait = true;
  tenant_ctx.wait_num = 0;
  tenant_ctx.enable_num = 0;
  tenant_ctx.waiting_qps = (uint32_t*)malloc(sizeof(uint32_t));
  tenant_ctx.waiting_head = 0;
  tenant_ctx.waiting_tail = 0;

  tenant_ctx.enabled_time = (struct timeval*)malloc(sizeof(struct timeval));
  tenant_ctx.enabled_qps = (uint32_t*)malloc(sizeof(uint32_t));
  tenant_ctx.enabled_head = 0;
  tenant_ctx.enabled_tail = 0;

  tenant_ctx.additional_enable_num = 0;
 
  tenant_ctx.paused_qps = (uint32_t*)malloc(sizeof(uint32_t));
  *(tenant_ctx.paused_qps) = -1;
  tenant_ctx.paused_qps_head = 0; 
  tenant_ctx.paused_qps_tail = 0; 
  tenant_ctx.allowed_qps_num = 0;

  tenant_ctx.resp_read = false;
  tenant_ctx.passive_reading = false;
  tenant_ctx.passive_delay_sensitive = true;
  tenant_ctx.passive_msg_sensitive = false;
  tenant_ctx.is_bw_read = false;

  pthread_mutex_init(&(tenant_ctx.wait_lock), NULL);
  pthread_mutex_init(&(tenant_ctx.allow_lock), NULL);
  
  pthread_mutex_init(&(tenant_ctx.poll_lock), NULL);
  pthread_cond_init(&(tenant_ctx.poll_cond), NULL);
  
  perf_create_read_qp();
}

void update_mqp_ctx()
{
  //LOG_ERROR("update_perf_mqp_state()\n");
  //make Master UD QP
  if((global_qnum >= 2 && USE_MULTI_MQP) || (global_qnum >= 2 && !USE_MULTI_MQP && mqp_ctx == NULL))
  {
    mqp_ctx = (struct perf_master_qp_context*)realloc(mqp_ctx, sizeof(struct perf_master_qp_context) * global_qnum);

    while(global_qnum != global_mqnum)
    {
      uint32_t mqp_idx = global_mqnum;
      perf_create_master_qp(mqp_idx);

      //create cqe_wait_wr of mqp
      mqp_ctx[mqp_idx].wait_cqe_wr.wr_id = -1;
      mqp_ctx[mqp_idx].wait_cqe_wr.sg_list = NULL;
      mqp_ctx[mqp_idx].wait_cqe_wr.num_sge = 0;
      mqp_ctx[mqp_idx].wait_cqe_wr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
      //mqp_ctx[mqp_idx].wait_cqe_wr.exp_send_flags = IBV_EXP_SEND_SIGNALED | IBV_EXP_SEND_WAIT_EN_LAST;
      mqp_ctx[mqp_idx].wait_cqe_wr.exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;

      mqp_ctx[mqp_idx].wait_cqe_wr.task.cqe_wait.cq = mqp_ctx[mqp_idx].qp->send_cq;
      mqp_ctx[mqp_idx].wait_cqe_wr.task.cqe_wait.cq_count = 1;
      mqp_ctx[mqp_idx].wait_cqe_wr.next = NULL;

      //create send_enable_wr of mqp
      mqp_ctx[mqp_idx].send_enable_wr.wr_id = -1;
      mqp_ctx[mqp_idx].send_enable_wr.sg_list = NULL;
      mqp_ctx[mqp_idx].send_enable_wr.num_sge = 0;
      mqp_ctx[mqp_idx].send_enable_wr.exp_opcode = IBV_EXP_WR_SEND_ENABLE;
      mqp_ctx[mqp_idx].send_enable_wr.exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
      //mqp_ctx[mqp_idx].send_enable_wr.exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST | IBV_EXP_SEND_SIGNALED;

      mqp_ctx[mqp_idx].send_enable_wr.task.wqe_enable.qp = mqp_ctx[mqp_idx].qp;
      mqp_ctx[mqp_idx].send_enable_wr.task.wqe_enable.wqe_count = 1;
      mqp_ctx[mqp_idx].send_enable_wr.next = NULL;


      //create self_wait_cqe_wr of mqp
      mqp_ctx[mqp_idx].self_wait_cqe_wr.wr_id = 7000 + global_mqnum;
      mqp_ctx[mqp_idx].self_wait_cqe_wr.sg_list = NULL;
      mqp_ctx[mqp_idx].self_wait_cqe_wr.num_sge = 0;
      mqp_ctx[mqp_idx].self_wait_cqe_wr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
      mqp_ctx[mqp_idx].self_wait_cqe_wr.exp_send_flags = IBV_EXP_SEND_SIGNALED;
      //mqp_ctx[mqp_idx].self_wait_cqe_wr.exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST | IBV_EXP_SEND_SIGNALED;
      mqp_ctx[mqp_idx].self_wait_cqe_wr.task.cqe_wait.cq = mqp_ctx[mqp_idx].qp->recv_cq;
      mqp_ctx[mqp_idx].self_wait_cqe_wr.task.cqe_wait.cq_count = 0;
      mqp_ctx[mqp_idx].self_wait_cqe_wr.next = NULL;

      mqp_ctx[mqp_idx].manage_qnum = USE_MULTI_MQP ? 1 : 2;
      global_mqnum++;

      if(!USE_MULTI_MQP)
        break;
    }
  }
  else if(!USE_MULTI_MQP && mqp_ctx[0].qp)
  {
    mqp_ctx[0].manage_qnum++;
  }
  else
  {
    LOG_ERROR("Error when start mqp update\n");
    exit(1);
  }
}

void perf_create_master_qp(uint32_t mqp_idx)
{
  struct ibv_context *context;
  struct ibv_pd *pd;

  struct ibv_cq *send_cq;
  struct ibv_cq *recv_cq;
  
  context = qp_ctx[0].qp->pd->context;
  pd = qp_ctx[0].qp->pd;
  
  char* env;  
  env = getenv("PERF_IB_PORT");
  if(env)
    mqp_ctx[mqp_idx].port_num = atoi(env);
  else
    mqp_ctx[mqp_idx].port_num = 1;

  env = getenv("PERF_GIDX");
  if(env)
    mqp_ctx[mqp_idx].gidx = atoi(env);
  else
    mqp_ctx[mqp_idx].gidx = 5;

  /*
  struct ibv_exp_cq_init_attr cq_init_attr = {
    .comp_mask = IBV_EXP_CQ_INIT_ATTR_FLAGS,
    .flags = IBV_EXP_CQ_CREATE_CROSS_CHANNEL
  };

  send_cq = ibv_exp_create_cq(context, MASTER_QP_DEPTH, NULL, NULL, 0, &cq_init_attr);
  recv_cq = ibv_exp_create_cq(context, MASTER_QP_DEPTH, NULL, NULL, 0, &cq_init_attr);
  */
 
  if(mqp_idx == 0)
  {
    mqp_ctx[mqp_idx].send_cq = ibv_create_cq(context, MASTER_QP_DEPTH, NULL, NULL, 0);
    mqp_ctx[mqp_idx].recv_cq = ibv_create_cq(context, MASTER_QP_DEPTH, NULL, NULL, 0);
  }
  else
  {
    mqp_ctx[mqp_idx].send_cq = mqp_ctx[0].send_cq;
    mqp_ctx[mqp_idx].recv_cq = mqp_ctx[0].recv_cq;
  }

  send_cq = mqp_ctx[mqp_idx].send_cq;
  recv_cq = mqp_ctx[mqp_idx].recv_cq;

  if (!send_cq || !recv_cq) {
    LOG_ERROR("Couldn't create CQ in creating master QP\n");
    exit(1);
  }

   struct ibv_exp_cq_attr cq_mod_attr = {
     .comp_mask       = IBV_EXP_CQ_ATTR_CQ_CAP_FLAGS,
     .cq_cap_flags    = IBV_EXP_CQ_IGNORE_OVERRUN
   };

   if (ibv_exp_modify_cq(send_cq, &cq_mod_attr, IBV_EXP_CQ_CAP_FLAGS)) {
     LOG_ERROR("Failed to modify send CQ\n");
     exit(1);
   }
   if (ibv_exp_modify_cq(recv_cq, &cq_mod_attr, IBV_EXP_CQ_CAP_FLAGS)) {
     LOG_ERROR("Failed to modify recv CQ\n");
     exit(1);
   }

  {
    struct ibv_exp_qp_init_attr attr = {
      .send_cq = send_cq,
      .recv_cq = recv_cq,
      .cap     = {
        .max_send_wr  = MASTER_QP_DEPTH,
        .max_recv_wr  = MASTER_QP_DEPTH,
        .max_send_sge = 1,
        .max_recv_sge = 1
      },
      .qp_type = IBV_QPT_RC,
      .pd = pd
    };

    attr.comp_mask |= IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
    attr.exp_create_flags = IBV_EXP_QP_CREATE_CROSS_CHANNEL | IBV_EXP_QP_CREATE_MANAGED_SEND; 
    mqp_ctx[mqp_idx].qp = NULL;
    mqp_ctx[mqp_idx].qp = ibv_exp_create_qp(context, &attr);

    if (!mqp_ctx[mqp_idx].qp)  
    {
      LOG_ERROR("Couldn't create QP in creating master QP\n");
      exit(1);
    }
  }

  {
    struct ibv_exp_qp_attr attr = {
      .qp_state		= IBV_QPS_INIT,
      .pkey_index		= 0,
      .port_num		= mqp_ctx[mqp_idx].port_num,
      .qp_access_flags          = 0  
    };

    if(ibv_exp_modify_qp(mqp_ctx[mqp_idx].qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX	| IBV_QP_PORT	| IBV_QP_ACCESS_FLAGS))
    {
      LOG_ERROR("Failed to exp_modify QP to INIT in creating master QP\n");
      exit(1);
    }

    memset(&attr, 0, sizeof(attr));

    union ibv_gid gid;

    if(ibv_query_gid(context, mqp_ctx[mqp_idx].port_num , mqp_ctx[mqp_idx].gidx, &gid))
    {
      LOG_ERROR("get_gid error in creatin master QP\n");
      exit(1);
    }

    LOG_DEBUG("SUBNET: %02d:%02d:%02d:%02d:%02d:%02d:%02d:%02d\nGUID  : %02d:%02d:%02d:%02d:%02d:%02d:%02d:%02d\n", gid.raw[0], gid.raw[1],  gid.raw[2], gid.raw[3], gid.raw[4], gid.raw[5], gid.raw[6], gid.raw[7], gid.raw[8], gid.raw[9], gid.raw[10],gid.raw[11],gid.raw[12],gid.raw[13], gid.raw[14],gid.raw[15]);

    attr.qp_state   = IBV_QPS_RTR;
    attr.path_mtu   = IBV_MTU_4096;
    attr.dest_qp_num  = mqp_ctx[mqp_idx].qp->qp_num;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer  = 12;
    attr.rq_psn   = 0;
    attr.ah_attr.dlid = 0;
    attr.ah_attr.is_global     = 1;
    attr.ah_attr.grh.dgid = gid;
    attr.ah_attr.grh.sgid_index = mqp_ctx[mqp_idx].gidx;
    attr.ah_attr.grh.hop_limit = 10;
    attr.ah_attr.sl      = 3;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num    = mqp_ctx[mqp_idx].port_num;

    if (ibv_exp_modify_qp(mqp_ctx[mqp_idx].qp, &attr,
          IBV_QP_STATE    |
          IBV_QP_AV       |
          IBV_QP_PATH_MTU |
          IBV_QP_DEST_QPN |
          IBV_QP_RQ_PSN   |
          IBV_QP_MAX_DEST_RD_ATOMIC |
          IBV_QP_MIN_RNR_TIMER)) {
      LOG_ERROR("Failed to exp_modify QP to RTR in creating master QP\n");
      exit(1);
    }

    memset(&attr, 0, sizeof(attr));

    attr.qp_state     = IBV_QPS_RTS;
    attr.sq_psn     = 0;
    attr.timeout      = 14;
    attr.retry_cnt      = 7;
    attr.rnr_retry      = 7; /* infinite */
    attr.max_rd_atomic  = 1;

    if (ibv_exp_modify_qp(mqp_ctx[mqp_idx].qp, &attr, 
          IBV_QP_STATE              |
          IBV_QP_TIMEOUT            |
          IBV_QP_RETRY_CNT          |
          IBV_QP_RNR_RETRY          |
          IBV_QP_SQ_PSN             |
          IBV_QP_MAX_QP_RD_ATOMIC))
    {
      LOG_ERROR("Failed to exp_modify QP to RTS in creating master QP\n");
      exit(1);
    }
  }
}


void update_perf_state(struct ibv_qp *qp, uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t origin_max_send_wr, uint32_t origin_max_recv_wr, int sig_all)
{
  if(use_perf == -1)
    load_perf_config();

  if(!use_perf)
    return;

  update_qp_ctx(qp, max_send_wr, max_recv_wr, origin_max_send_wr, origin_max_recv_wr, sig_all);
}

void perf_destroy_qp()
{
  if(!use_perf)
    return;
  tenant_ctx.is_active = false;
  pthread_mutex_lock(&shm_ctx->lock);
  shm_ctx->active_qps_per_tenant[tenant_id] = 0;
  shm_ctx->delay_sensitive[tenant_id] = false;
  shm_ctx->msg_sensitive[tenant_id] = false;
  shm_ctx->resp_read[tenant_id] = false;
  shm_ctx->btenant_can_post[tenant_id] = true;
  pthread_mutex_unlock(&(shm_ctx->perf_thread_lock[tenant_id]));
  pthread_mutex_unlock(&shm_ctx->lock);
  
  pthread_cancel(daemon_thread);

  use_perf = false; 
}

void perf_early_poll_cq()
{
  //LOG_ERROR("perf_early_poll_cq()\n");
  uint32_t cq_poll_num;
  int polled;
  for(uint32_t i=0; i<global_cqnum; i++)
  {
    if(cq_ctx[i].cq != NULL)
    {
      //pthread_mutex_lock(&(cq_ctx[i].lock));
      //LOG_ERROR("before i: %d %d\n",  mlx5_get_sq_num(qp_ctx[0].qp), mlx5_get_sq_num(qp_ctx[1].qp)); 
      while(1)
      {
        cq_poll_num = cq_ctx[i].max_cqe - cq_ctx[i].wc_tail;
        polled = mlx5_poll_cq_ex2(cq_ctx[i].cq, cq_poll_num, (struct ibv_exp_wc*)(cq_ctx[i].wc_list) + cq_ctx[i].wc_tail, sizeof(struct ibv_exp_wc), 1, 1);

        if(polled < 0)
        {
          LOG_ERROR("Error in early poll: %d\n", polled);
          exit(1);
        }

        if(!polled)
          break;

        /*
        for(uint32_t j=0; j<polled; j++)
        {
          LOG_ERROR("Comp WR ID: %lu %d\n", ((struct ibv_exp_wc*)cq_ctx[i].wc_list)[cq_ctx[i].wc_tail+j].wr_id, ((struct ibv_exp_wc*)cq_ctx[i].wc_list)[cq_ctx[i].wc_tail+j].status);
        }
        */

        cq_ctx[i].early_poll_num += polled;
        cq_ctx[i].wc_tail = (cq_ctx[i].wc_tail + polled) % cq_ctx[i].max_cqe;

        //LOG_ERROR("Early Poll CQ: %d, early poll num: %d, wc_tail: %d\n",polled, cq_ctx[i].early_poll_num, cq_ctx[i].wc_tail);
      }
      //LOG_ERROR("after i: %d %d\n",  mlx5_get_sq_num(qp_ctx[0].qp), mlx5_get_sq_num(qp_ctx[1].qp)); 
      //pthread_mutex_unlock(&(cq_ctx[i].lock));
    }
  }
}

void perf_update_tenant_state()
{
  struct timeval now;
  gettimeofday(&now, NULL);

  uint64_t t =  (now.tv_usec - tenant_ctx.last_sq_check_time.tv_usec) + 1000000 * (now.tv_sec - tenant_ctx.last_sq_check_time.tv_sec);

  uint32_t max = 0;

  if(t >= TENANT_SQ_CHECK_INTERVAL)
  {
    for(uint32_t i=0; i<global_qnum; i++)
    {   
      if(max < mlx5_get_sq_num(qp_ctx[i].qp) + qp_ctx[i].wr_queue_len)
        max = mlx5_get_sq_num(qp_ctx[i].qp) + qp_ctx[i].wr_queue_len;
    }
      
    tenant_ctx.sq_history[tenant_ctx.sq_ins_idx] = max;
    if(tenant_ctx.sq_ins_idx == tenant_ctx.sq_max_idx)
    {
      for(uint32_t i=0; i<tenant_ctx.sq_history_len; i++)
      {
        if(tenant_ctx.sq_history[i] > tenant_ctx.sq_history[tenant_ctx.sq_max_idx])
        {
          tenant_ctx.sq_max_idx = i;
        }
      }
      if(tenant_ctx.is_bw_read)
        tenant_ctx.delay_sensitive = true;
      else if(tenant_ctx.sq_history[tenant_ctx.sq_max_idx] >= DELAY_SEN_NUM_TH ||
          tenant_ctx.sq_history[tenant_ctx.sq_max_idx] * tenant_ctx.avg_msg_size >= DELAY_SEN_BYTES_TH || tenant_ctx.max_msg_size >= PERF_LARGE_FLOW || !tenant_ctx.passive_delay_sensitive)
        tenant_ctx.delay_sensitive = false;
      else
        tenant_ctx.delay_sensitive = true;
    }
    else
    {
      if(max >= tenant_ctx.sq_history[tenant_ctx.sq_max_idx])
      {
         tenant_ctx.sq_max_idx = tenant_ctx.sq_ins_idx;
      
         if(tenant_ctx.is_bw_read)
           tenant_ctx.delay_sensitive = true;
         else if(tenant_ctx.sq_history[tenant_ctx.sq_max_idx] >= DELAY_SEN_NUM_TH ||
             tenant_ctx.sq_history[tenant_ctx.sq_max_idx] * tenant_ctx.avg_msg_size >= DELAY_SEN_BYTES_TH || tenant_ctx.max_msg_size >= PERF_LARGE_FLOW || !tenant_ctx.passive_delay_sensitive)
           tenant_ctx.delay_sensitive = false;
         else
           tenant_ctx.delay_sensitive = true;
      }
    }
   
    if(tenant_ctx.max_msg_size < PERF_LARGE_FLOW || (tenant_ctx.passive_reading && (tenant_ctx.passive_delay_sensitive || tenant_ctx.passive_msg_sensitive)) || tenant_ctx.is_bw_read)
      tenant_ctx.small_msg_sending = true;
    else
      tenant_ctx.small_msg_sending = false;

    if(bw_send)
    {
      tenant_ctx.delay_sensitive = false;
      tenant_ctx.small_msg_sending = false;
    }


    tenant_ctx.sq_ins_idx = (tenant_ctx.sq_ins_idx + 1) % tenant_ctx.sq_history_len;
    gettimeofday(&(tenant_ctx.last_sq_check_time), NULL);
    //if(tenant_ctx.delay_sensitive)
    //  LOG_ERROR("MAX SQ NUM : %d\n", tenant_ctx.sq_history[tenant_ctx.sq_max_idx]);
  }

  t =  (now.tv_usec - tenant_ctx.last_active_check_time.tv_usec) + 1000000 * (now.tv_sec - tenant_ctx.last_active_check_time.tv_sec);

  if((!shm_ctx->delay_sensitive[tenant_id] && t >= TENANT_ACTIVE_CHECK_INTERVAL) || (shm_ctx->delay_sensitive[tenant_id] && t >= TENANT_INACTIVE_CHECK_INTERVAL))
  {
    uint32_t cur_active_num = 0;
    
    for(uint32_t i=0; i<global_qnum; i++)
    {
      //if(qp_ctx[i].is_active || mlx5_get_sq_num(qp_ctx[i].qp) || qp_ctx[i].wr_queue_len)
      if(qp_ctx[i].is_active || tenant_ctx.sq_history[tenant_ctx.sq_max_idx] || qp_ctx[i].wr_queue_len || mlx5_get_rq_num(qp_ctx[i].qp))
      {
        cur_active_num++;
        if(qp_ctx[i].is_reading)
        {
          if(rc_used && read_qp_connected)
          {
            perf_send_read_report(i);
            qp_ctx[i].is_reading = false;
          }
        }
      }

      qp_ctx[i].is_active = false;
      //LOG_ERROR("QIDX: %d, UNCOMP: %d\n", i, mlx5_get_sq_num(qp_ctx[i].qp));
    }

    tenant_ctx.active_qps_num = cur_active_num;

    //LOG_ERROR("Active QP Num: %d %d\n", tenant_ctx.active_qps_num, tenant_ctx.is_active);

    //pthread_mutex_lock(&shm_ctx->lock);
    if(!tenant_ctx.active_qps_num)
    {
      tenant_ctx.is_active = false;
      shm_ctx->active_qps_per_tenant[tenant_id] = 0;
      shm_ctx->delay_sensitive[tenant_id] = true;
      shm_ctx->msg_sensitive[tenant_id] = false;
    }
    else
    {
      tenant_ctx.is_active = true;
      if(tenant_ctx.small_msg_sending && !tenant_ctx.is_bw_read)
      {
        if(tenant_ctx.delay_sensitive)
        {
          shm_ctx->delay_sensitive[tenant_id] = true;
          shm_ctx->msg_sensitive[tenant_id] = false;
        }
        else
        {
          shm_ctx->delay_sensitive[tenant_id] = false;
          shm_ctx->msg_sensitive[tenant_id] = true;
        }
      }
      else
      {
        shm_ctx->delay_sensitive[tenant_id] = false;
        shm_ctx->msg_sensitive[tenant_id] = false;
      }

      shm_ctx->resp_read[tenant_id] = tenant_ctx.resp_read;

      shm_ctx->active_qps_per_tenant[tenant_id] = tenant_ctx.is_bw_read ? 1 : tenant_ctx.active_qps_num;
      shm_ctx->avg_msg_size[tenant_id] = tenant_ctx.is_bw_read ? 16 : tenant_ctx.avg_msg_size; 
    }
    //pthread_mutex_unlock(&shm_ctx->lock);

    tenant_ctx.resp_read = false;
    tenant_ctx.passive_reading = false;

    gettimeofday(&(tenant_ctx.last_active_check_time), NULL);

    tenant_ctx.is_bw_read = false;
  }
}

void perf_mqp_process()
{
  if(!use_perf)
    return;

  for(uint32_t i=0; i<global_mqnum; i++)
  {
    struct ibv_exp_wc wc[MASTER_QP_DEPTH];
    int ret =  mlx5_poll_cq_ex2(mqp_ctx[i].qp->send_cq, MASTER_QP_DEPTH, wc, sizeof(struct ibv_exp_wc), 1, 1);

    if(ret < 0)
    {
      LOG_ERROR("MQP COMPLETION Error\n");
      exit(1);
    }
  }

  //if(!tenant_ctx.delay_sensitive && shm_ctx->active_tenant_num > 1 && (shm_ctx->active_qps_num > shm_ctx->max_qps_limit || shm_ctx->active_tenant_num != shm_ctx->active_stenant_num) && tenant_ctx.wait_num >= tenant_ctx.enable_num && (tenant_ctx.enable_num || tenant_ctx.wait_num))
  if(!tenant_ctx.delay_sensitive && shm_ctx->active_tenant_num > 1 && shm_ctx->active_qps_num > shm_ctx->max_qps_limit && tenant_ctx.wait_num >= tenant_ctx.enable_num && (tenant_ctx.enable_num || tenant_ctx.wait_num))
  {
    struct timeval now;
    gettimeofday(&now, NULL);

    if(!tenant_ctx.is_first_wait && tenant_ctx.wait_num >= tenant_ctx.enable_num)
    {
      pthread_mutex_lock(&tenant_ctx.wait_lock);
      uint32_t le_qidx = tenant_ctx.enabled_qps[tenant_ctx.enabled_head];
      uint32_t mqp_idx = USE_MULTI_MQP ? le_qidx : 0; 
      
      if(le_qidx != -1 && tenant_ctx.wait_num > tenant_ctx.enable_num && (qp_ctx[le_qidx].post_num >= POST_STOP_NUM_TH || (qp_ctx[le_qidx].post_num * tenant_ctx.avg_msg_size) >= POST_STOP_BYTES_TH))
      {
        //LOG_ERROR("Wait process: %d %d %d\n", le_qidx, tenant_ctx.wait_num - tenant_ctx.enable_num, mlx5_get_sq_num(mqp_ctx[mqp_idx].qp));
        struct ibv_exp_send_wr *exp_bad_wr;

        if(mlx5_exp_post_send2(mqp_ctx[mqp_idx].qp, &(mqp_ctx[mqp_idx].self_wait_cqe_wr), &exp_bad_wr, 1) != 0)
        {
          LOG_ERROR("%d Send self wait error: %d\n", mqp_idx,  mlx5_get_sq_num(mqp_ctx[mqp_idx].qp));
          exit(1);
        }

        if(mlx5_exp_post_send2(qp_ctx[le_qidx].qp, &(mqp_ctx[mqp_idx].send_enable_wr), &exp_bad_wr, 1) != 0)
        {
          LOG_ERROR("Send enable error\n");
          exit(1);
        }
        if(mlx5_exp_post_send2(qp_ctx[le_qidx].qp, &(mqp_ctx[mqp_idx].wait_cqe_wr), &exp_bad_wr, 1) != 0)
        {
          LOG_ERROR("Send wait error\n");
          exit(1);
        }

        tenant_ctx.wait_num++;
        tenant_ctx.enable_num++;
        qp_ctx[le_qidx].post_num = 0;

        tenant_ctx.waiting_qps[tenant_ctx.waiting_tail] = le_qidx;
        tenant_ctx.waiting_tail = (tenant_ctx.waiting_tail + 1) % global_qnum;

        tenant_ctx.enabled_time[tenant_ctx.enabled_tail] = now;
        tenant_ctx.enabled_qps[tenant_ctx.enabled_tail] = tenant_ctx.waiting_qps[tenant_ctx.waiting_head];
        tenant_ctx.enabled_tail = (tenant_ctx.enabled_tail + 1) % global_qnum;

        tenant_ctx.waiting_qps[tenant_ctx.waiting_head] = -1;
        tenant_ctx.waiting_head = (tenant_ctx.waiting_head + 1) % global_qnum;

        tenant_ctx.enabled_qps[tenant_ctx.enabled_head] = -1;
        tenant_ctx.enabled_head = (tenant_ctx.enabled_head + 1) % global_qnum;
        le_qidx = tenant_ctx.enabled_qps[tenant_ctx.enabled_head];
        qp_ctx[le_qidx].post_num = 0;

        mqp_idx = USE_MULTI_MQP ? le_qidx : 0; 
      }
      else if(le_qidx != -1 && tenant_ctx.wait_num > tenant_ctx.enable_num)
      {
        uint64_t enabled_time = (now.tv_sec - tenant_ctx.enabled_time[tenant_ctx.enabled_head] .tv_sec) * 1000000 + (now.tv_usec - tenant_ctx.enabled_time[tenant_ctx.enabled_head].tv_usec);
        //if(tenant_ctx.wait_num - tenant_ctx.enable_num && (perf_check_paused(le_qidx) || enabled_time > POST_STOP_TIME_TH))
        if(perf_check_paused(le_qidx) || enabled_time > POST_STOP_TIME_TH)
        {
          struct ibv_exp_send_wr *exp_bad_wr;

          //LOG_ERROR("Enable process: %d %d\n", le_qidx, tenant_ctx.wait_num - tenant_ctx.enable_num);

          if(mlx5_exp_post_send2(mqp_ctx[mqp_idx].qp, &(mqp_ctx[mqp_idx].self_wait_cqe_wr), &exp_bad_wr, 1) != 0)
          {
            LOG_ERROR("%d Send self wait error: %d\n", mqp_idx,  mlx5_get_sq_num(mqp_ctx[mqp_idx].qp));
            exit(1);
          }

          if(mlx5_exp_post_send2(qp_ctx[le_qidx].qp, &(mqp_ctx[mqp_idx].send_enable_wr), &exp_bad_wr, 1) != 0)
          {
            LOG_ERROR("Send enable error\n");
            exit(1);
          }

          tenant_ctx.enable_num++;
          qp_ctx[le_qidx].post_num = 0;
          qp_ctx[le_qidx].is_first_wait = true;

          tenant_ctx.enabled_time[tenant_ctx.enabled_tail] = now;
          tenant_ctx.enabled_qps[tenant_ctx.enabled_tail] = tenant_ctx.waiting_qps[tenant_ctx.waiting_head];
          tenant_ctx.enabled_tail = (tenant_ctx.enabled_tail + 1) % global_qnum;
          
          tenant_ctx.waiting_qps[tenant_ctx.waiting_head] = -1;
          tenant_ctx.waiting_head = (tenant_ctx.waiting_head + 1) % global_qnum;

          if(!qp_ctx[le_qidx].is_paused)
            perf_allowed_release(le_qidx);
        
          tenant_ctx.enabled_qps[tenant_ctx.enabled_head] = -1;
          tenant_ctx.enabled_head = (tenant_ctx.enabled_head + 1) % global_qnum;
          le_qidx = tenant_ctx.enabled_qps[tenant_ctx.enabled_head];
          qp_ctx[le_qidx].post_num = 0;
        
          mqp_idx = USE_MULTI_MQP ? le_qidx : 0; 
        }
      }

      if(le_qidx != -1 && tenant_ctx.additional_enable_num < shm_ctx->additional_qps_num[tenant_id] && tenant_ctx.wait_num > tenant_ctx.enable_num)
      {
        struct ibv_exp_send_wr *exp_bad_wr;

        //LOG_ERROR("Additional Enable process: %d %d %d %d\n", le_qidx, tenant_ctx.allowed_qps_num, tenant_ctx.wait_num - tenant_ctx.enable_num, shm_ctx->additional_qps_num[tenant_id]);

        if(mlx5_exp_post_send2(mqp_ctx[mqp_idx].qp, &(mqp_ctx[mqp_idx].self_wait_cqe_wr), &exp_bad_wr, 1) != 0)
        {
          LOG_ERROR("%d Send self wait error: %d\n", mqp_idx,  mlx5_get_sq_num(mqp_ctx[mqp_idx].qp));
          exit(1);
        }

        if(mlx5_exp_post_send2(qp_ctx[le_qidx].qp, &(mqp_ctx[mqp_idx].send_enable_wr), &exp_bad_wr, 1) != 0)
        {
          LOG_ERROR("Send enable error\n");
          exit(1);
        }

        tenant_ctx.enable_num++;
        tenant_ctx.additional_enable_num++;
        qp_ctx[le_qidx].post_num = 0;

        tenant_ctx.enabled_time[tenant_ctx.enabled_tail] = now;
        tenant_ctx.enabled_qps[tenant_ctx.enabled_tail] = tenant_ctx.waiting_qps[tenant_ctx.waiting_head];
        tenant_ctx.enabled_tail = (tenant_ctx.enabled_tail + 1) % global_qnum;

        tenant_ctx.waiting_qps[tenant_ctx.waiting_head] = -1;
        tenant_ctx.waiting_head = (tenant_ctx.waiting_head + 1) % global_qnum;
      }

      else if(le_qidx != -1 && tenant_ctx.additional_enable_num > shm_ctx->additional_qps_num[tenant_id])
      {
        //LOG_ERROR("Additional Wait process: %d %d %d %d\n", le_qidx, tenant_ctx.allowed_qps_num, tenant_ctx.wait_num - tenant_ctx.enable_num, shm_ctx->additional_qps_num[tenant_id]);
        
        qp_ctx[le_qidx].post_num = 0;
        qp_ctx[le_qidx].is_first_wait = true;
        tenant_ctx.additional_enable_num--;

        if(!qp_ctx[le_qidx].is_paused)
          perf_allowed_release(le_qidx);

        tenant_ctx.enabled_qps[tenant_ctx.enabled_head] = -1;
        tenant_ctx.enabled_head = (tenant_ctx.enabled_head + 1) % global_qnum;
        le_qidx = tenant_ctx.enabled_qps[tenant_ctx.enabled_head];
        qp_ctx[le_qidx].post_num = 0;
        mqp_idx = USE_MULTI_MQP ? le_qidx : 0; 
      }

      if(le_qidx == -1)
      {
        LOG_ERROR("enable_qps is empty\n");
        for(uint32_t i=0; i<global_qnum; i++)
          LOG_ERROR("%d %d\n", tenant_ctx.enabled_qps[i], tenant_ctx.waiting_qps[i]);
        
        exit(1);
      }
      pthread_mutex_unlock(&tenant_ctx.wait_lock);
    }
  }
  else if(tenant_ctx.wait_num && !(!tenant_ctx.delay_sensitive  && shm_ctx->active_tenant_num > 1 && (shm_ctx->active_qps_num > shm_ctx->max_qps_limit || shm_ctx->active_tenant_num != shm_ctx->active_stenant_num)))
  {
    pthread_mutex_lock(&tenant_ctx.wait_lock);
    uint32_t le_qidx = tenant_ctx.enabled_qps[tenant_ctx.enabled_head];
    uint32_t mqp_idx = USE_MULTI_MQP ? le_qidx : 0; 
    struct ibv_exp_send_wr *exp_bad_wr;

    //LOG_ERROR("Start Init process: %d\n", le_qidx);
    for(uint32_t i=0; i<tenant_ctx.wait_num - tenant_ctx.enable_num; i++)
    {
      //LOG_ERROR("Init process: %d\n", le_qidx);
      if(mlx5_exp_post_send2(mqp_ctx[mqp_idx].qp, &(mqp_ctx[mqp_idx].self_wait_cqe_wr), &exp_bad_wr, 1) != 0)
      {
        LOG_ERROR("%d Send self wait error: %d\n", mqp_idx,  mlx5_get_sq_num(mqp_ctx[mqp_idx].qp));
        exit(1);
      }

      if(mlx5_exp_post_send2(qp_ctx[le_qidx].qp, &(mqp_ctx[mqp_idx].send_enable_wr), &exp_bad_wr, 1) != 0)
      {
        LOG_ERROR("Send enable error\n");
        exit(1);
      }
    }

    tenant_ctx.is_first_wait = true;
    for(uint32_t i=0; i<global_qnum; i++)
    {
      qp_ctx[i].is_first_wait = true;
      tenant_ctx.enabled_qps[i] = -1;
      tenant_ctx.waiting_qps[i] = -1;
    }

    tenant_ctx.wait_num = 0;
    tenant_ctx.enable_num = 0;
    tenant_ctx.waiting_head = 0;
    tenant_ctx.waiting_tail = 0;
    tenant_ctx.enabled_head = 0;
    tenant_ctx.enabled_tail = 0;
    tenant_ctx.additional_enable_num = 0;

    perf_init_pause_state();
    pthread_mutex_unlock(&tenant_ctx.wait_lock);
  }
}

int perf_process(struct ibv_qp *qp, struct ibv_send_wr *wr)
{
  uint64_t size = 0;
  for(uint32_t i=0; i<wr->num_sge; i++)
    size += wr->sg_list[i].length;

  while(TENANT_TARGET_RATE != 0 && wr->opcode != IBV_WR_RDMA_READ && !tb_consume(token_bucket, size)) {
    usleep(1);
  }
 
  if(!use_perf)
  {
    return 0;
  }

  while(rc_used && !read_qp_connected)
    sleep(1);

  tenant_ctx.avg_msg_size = tenant_ctx.avg_msg_size == 0 ? size : 0.5 * tenant_ctx.avg_msg_size + 0.5 * size; 
  tenant_ctx.max_msg_size = tenant_ctx.max_msg_size < size ? size : tenant_ctx.max_msg_size; 

  if(size < PERF_LARGE_FLOW)
  {
    if(tenant_ctx.delay_sensitive || global_qnum == 1)
      return PERF_BYPASS;
    
    uint32_t q_idx = kh_value(qp_hash, kh_get(qph, qp_hash, qp->qp_num));
    
    if (qp_ctx[q_idx].wr_queue_len)
    {
      //LOG_ERROR("Already Paused QP: %d %d %lu\n", q_idx, qp_ctx[q_idx].wr_queue_len, size);
      return PERF_BACKGROUND;
    }
    
    //if(!(shm_ctx->active_tenant_num > 1 &&  (shm_ctx->active_qps_num > shm_ctx->max_qps_limit || shm_ctx->active_tenant_num != shm_ctx->active_stenant_num)))
    if(!(shm_ctx->active_tenant_num > 1 && shm_ctx->active_qps_num > shm_ctx->max_qps_limit))
      return PERF_BYPASS;

    if(tenant_ctx.allowed_qps_num == MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id] && qp_ctx[q_idx].is_paused)
    {
      pthread_mutex_lock(&(tenant_ctx.allow_lock));
      if(tenant_ctx.allowed_qps_num == MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id] && qp_ctx[q_idx].is_paused)
      {
        if(qp_ctx[q_idx].paused_idx == -1 && !qp_ctx[q_idx].is_allowed)
        {
          if(tenant_ctx.paused_qps[tenant_ctx.paused_qps_tail] != -1)
          {
            LOG_ERROR("Pause QP insert error\n");
            exit(1);
          }
          tenant_ctx.paused_qps[tenant_ctx.paused_qps_tail] = q_idx;
          qp_ctx[q_idx].paused_idx = tenant_ctx.paused_qps_tail;
          //LOG_ERROR("Paused QP Insert: %d %d\n", q_idx,  tenant_ctx.paused_qps_tail);
          tenant_ctx.paused_qps_tail = (tenant_ctx.paused_qps_tail + 1) % global_qnum;
        }
        pthread_mutex_unlock(&(tenant_ctx.allow_lock));
        return PERF_BACKGROUND;
      }
      pthread_mutex_unlock(&(tenant_ctx.allow_lock));
    }
    //else
    //{
      //LOG_ERROR("BYPASS QP: %d\n", q_idx);
      return PERF_BYPASS;
    //}
  }
  else
  {
    if(wr->opcode == IBV_WR_RDMA_READ)
    {
      tenant_ctx.is_bw_read = true;
      return PERF_BACKGROUND;
    }
  
    bw_send = true;

    uint32_t q_idx = kh_value(qp_hash, kh_get(qph, qp_hash, qp->qp_num));

    if(tenant_ctx.allowed_qps_num == MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id] && qp_ctx[q_idx].is_paused)
    {
      pthread_mutex_lock(&(tenant_ctx.allow_lock));
      if(tenant_ctx.allowed_qps_num == MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id] && qp_ctx[q_idx].is_paused)
      {
        if(qp_ctx[q_idx].paused_idx == -1 && !qp_ctx[q_idx].is_allowed)
        {
          if(tenant_ctx.paused_qps[tenant_ctx.paused_qps_tail] != -1)
          {
            LOG_ERROR("Pause QP insert error\n");
            exit(1);
          }
          tenant_ctx.paused_qps[tenant_ctx.paused_qps_tail] = q_idx;
          qp_ctx[q_idx].paused_idx = tenant_ctx.paused_qps_tail;
          //LOG_ERROR("Paused QP Insert: %d %d\n", q_idx,  tenant_ctx.paused_qps_tail);
          tenant_ctx.paused_qps_tail = (tenant_ctx.paused_qps_tail + 1) % global_qnum;
        }
        pthread_mutex_unlock(&(tenant_ctx.allow_lock));
        return PERF_BACKGROUND;
      }
      pthread_mutex_unlock(&(tenant_ctx.allow_lock));
    }
 
    if(size <= CHUNK_SIZE) //for poll post mode
      return size;
  }

  return PERF_BACKGROUND;
}

void perf_poll_post(struct ibv_qp *qp, struct ibv_send_wr *wr, uint32_t size)
{
  uint32_t q_idx = kh_value(qp_hash, kh_get(qph, qp_hash, qp->qp_num));

  //LOG_ERROR("perf_poll_post start: %d %ld\n", q_idx, size);
  while(wr != NULL)
  {
    //for poll post mode
    if(size > CHUNK_SIZE || qp_ctx[q_idx].wr_queue_len || !shm_ctx->btenant_can_post[tenant_id]) 
    {
      perf_bg_post(qp, wr);
      break;
    }
    else
    {
      struct ibv_send_wr* next_wr;
      next_wr = wr->next;
      wr->next = NULL;
      perf_large_process(q_idx, wr, size);
      wr->next = next_wr;
    }

    wr = wr->next;
    if(wr != NULL)
    {
      size = 0;
      for(uint32_t i=0; i<wr->num_sge; i++)
        size += wr->sg_list[i].length;
    }
  }
  //LOG_ERROR("POST Comp\n");
}


void perf_bg_post(struct ibv_qp *qp, struct ibv_send_wr *wr)
{
  //LOG_ERROR("Perf bg post\n");
  uint32_t q_idx = kh_value(qp_hash, kh_get(qph, qp_hash, qp->qp_num));

  if(wr->opcode == IBV_WR_RDMA_READ)
  {
    send_read_request(q_idx, wr);
    return;
  }

  //LOG_ERROR("Enqueue start: %d\n", q_idx);
  enqueue_wr(q_idx, wr);

  while(wr->next != NULL)
  {
    wr = wr->next;
    enqueue_wr(q_idx, wr);
  }
}
int perf_recv_process(struct ibv_qp *qp, struct ibv_recv_wr *wr)
{
  if(!use_perf)
    return 0;

  uint32_t size = 0;
  for(uint32_t i=0; i<wr->num_sge; i++)
    size += wr->sg_list[i].length;
  
  tenant_ctx.max_recv_msg_size = tenant_ctx.max_recv_msg_size < size ? size : tenant_ctx.max_recv_msg_size; 
  //LOG_DEBUG("perf_recv_process: %u %lu\n", size, tenant_ctx.max_recv_msg_size); 

  if(tenant_ctx.max_recv_msg_size > CHUNK_SIZE)
    return PERF_BACKGROUND;

  return PERF_BYPASS;
}


void perf_bg_recv_post(struct ibv_qp *qp, struct ibv_recv_wr *wr)
{
  uint32_t q_idx = kh_value(qp_hash, kh_get(qph, qp_hash, qp->qp_num));

  //LOG_ERROR("Recv Enqueue start: %d\n", q_idx);
  enqueue_recv_wr(q_idx, wr);

  while(wr->next != NULL)
  {
    wr = wr->next;
    enqueue_recv_wr(q_idx, wr);
  }
}

void perf_init_pause_state()
{
  pthread_mutex_lock(&(tenant_ctx.allow_lock));
  for(uint32_t i=0; i<global_qnum; i++)
  {
    qp_ctx[i].is_paused = true;
    qp_ctx[i].paused_idx = -1;
    qp_ctx[i].is_allowed = false;
    tenant_ctx.paused_qps[i] = -1;
  }
  tenant_ctx.paused_qps_head = 0; 
  tenant_ctx.paused_qps_tail = 0; 
  tenant_ctx.allowed_qps_num = 0;
  pthread_mutex_unlock(&(tenant_ctx.allow_lock));
}

bool perf_check_paused(uint32_t q_idx)
{
  if(!(!tenant_ctx.delay_sensitive && shm_ctx->active_tenant_num > 1 && shm_ctx->active_qps_num > shm_ctx->max_qps_limit && global_qnum > 1))
  {
    //LOG_ERROR("CHECK Paused pass\n");
    return false;
  }
  
  bool ret = false;
  pthread_mutex_lock(&(tenant_ctx.allow_lock));
  if(tenant_ctx.allowed_qps_num < MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id] && qp_ctx[q_idx].is_paused)
  {
    qp_ctx[q_idx].is_paused = false;
    qp_ctx[q_idx].paused_idx = -1;
    qp_ctx[q_idx].is_allowed = true;
    gettimeofday(&(qp_ctx[q_idx].last_allowed_time), NULL);
    tenant_ctx.allowed_qps_num++;
    //LOG_ERROR("ALLOW INIT: %d %d\n", q_idx,  tenant_ctx.allowed_qps_num);
  }
  else if(qp_ctx[q_idx].is_paused)
  {
    ret = true;
  }
  else if(tenant_ctx.allowed_qps_num > MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id])
  {
    qp_ctx[q_idx].is_allowed = false;
    qp_ctx[q_idx].is_paused = true;
    qp_ctx[q_idx].paused_idx = -1;
    tenant_ctx.allowed_qps_num--;
    ret = true;
    //LOG_ERROR("Allow Released: %d %d\n", q_idx,  tenant_ctx.allowed_qps_num);
  }
  else
  {
    struct timeval now;
    gettimeofday(&now, NULL);

    uint64_t t = (now.tv_sec - qp_ctx[q_idx].last_allowed_time.tv_sec) * 1000000 + (now.tv_usec - qp_ctx[q_idx].last_allowed_time.tv_usec);

    if(t >= ALLOWED_QP_TIME_TH && tenant_ctx.paused_qps[tenant_ctx.paused_qps_head] != -1)
    {
      //LOG_ERROR("Paused QP Delete: %d %d %d\n", q_idx,  tenant_ctx.paused_qps[tenant_ctx.paused_qps_head],  tenant_ctx.paused_qps_head);
      
      uint32_t allow_idx = tenant_ctx.paused_qps[tenant_ctx.paused_qps_head];

      while(allow_idx != -1 && !qp_ctx[allow_idx].is_paused)
      {
        tenant_ctx.paused_qps[tenant_ctx.paused_qps_head] = -1;
        tenant_ctx.paused_qps_head = (tenant_ctx.paused_qps_head + 1) % global_qnum;
        allow_idx = tenant_ctx.paused_qps[tenant_ctx.paused_qps_head];
      }

      if(allow_idx != -1)
      {
        qp_ctx[allow_idx].is_allowed = true;

        qp_ctx[q_idx].is_allowed = false;

        qp_ctx[allow_idx].paused_idx = -1;
        qp_ctx[allow_idx].last_allowed_time = now;

        tenant_ctx.paused_qps[tenant_ctx.paused_qps_head] = -1;
        tenant_ctx.paused_qps_head = (tenant_ctx.paused_qps_head + 1) % global_qnum;

        qp_ctx[q_idx].is_paused = true;
        qp_ctx[allow_idx].is_paused = false;

        //LOG_ERROR("ALLOWED: %d %d\n", q_idx, allow_idx);
        ret = true;
      }
      else
      {
        qp_ctx[q_idx].is_allowed = false;
        qp_ctx[q_idx].is_paused = true;
        qp_ctx[q_idx].paused_idx = -1;
        tenant_ctx.allowed_qps_num--;
        //LOG_ERROR("Allow Released: %d %d\n", q_idx,  tenant_ctx.allowed_qps_num);
      }
    }
  }

  pthread_mutex_unlock(&(tenant_ctx.allow_lock));
  return ret;
}

void perf_allowed_release(uint32_t q_idx)
{
  pthread_mutex_lock(&(tenant_ctx.allow_lock));
  uint32_t allow_idx = -1;
 
  if(tenant_ctx.allowed_qps_num <= MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id] && tenant_ctx.paused_qps[tenant_ctx.paused_qps_head] != -1)
  {
    //LOG_ERROR("Allow released & Paused QP Delete: %d %d %d\n", q_idx,  tenant_ctx.paused_qps[tenant_ctx.paused_qps_head],  tenant_ctx.paused_qps_head);
    struct timeval now;
    gettimeofday(&now, NULL);

    allow_idx = tenant_ctx.paused_qps[tenant_ctx.paused_qps_head];
 
    while(allow_idx != -1 && !qp_ctx[allow_idx].is_paused)
    {
      tenant_ctx.paused_qps[tenant_ctx.paused_qps_head] = -1;
      tenant_ctx.paused_qps_head = (tenant_ctx.paused_qps_head + 1) % global_qnum;
      allow_idx = tenant_ctx.paused_qps[tenant_ctx.paused_qps_head];
    }

    if(allow_idx != -1)
    {
      qp_ctx[allow_idx].is_allowed = true;

      qp_ctx[q_idx].is_allowed = false;

      qp_ctx[allow_idx].paused_idx = -1;
      qp_ctx[allow_idx].last_allowed_time = now;

      tenant_ctx.paused_qps[tenant_ctx.paused_qps_head] = -1;
      tenant_ctx.paused_qps_head = (tenant_ctx.paused_qps_head + 1) % global_qnum;

      qp_ctx[q_idx].is_paused = true;
      qp_ctx[allow_idx].is_paused = false;

      //LOG_ERROR("Allow Released & NEW ALLOWED: %d %d\n", q_idx, allow_idx);
    }
  }

  if(allow_idx == -1)
  {
    qp_ctx[q_idx].is_allowed = false;
    qp_ctx[q_idx].is_paused = true;
    qp_ctx[q_idx].paused_idx = -1;
    tenant_ctx.allowed_qps_num--;
    //LOG_ERROR("Allow Released: %d %d\n", q_idx,  tenant_ctx.allowed_qps_num);
  }
  pthread_mutex_unlock(&(tenant_ctx.allow_lock));
}

void perf_wr_queue_manage()
{
  while(1)
  {
    manage_stop = true;
    for(uint32_t i=0; i<global_qnum; i++)
    {
      //LOG_ERROR("start perf_wr_queue_manage: %d %d %d\n", i, perf_check_paused(i), qp_ctx[i].wr_queue_len);
      if(perf_check_paused(i) || !qp_ctx[i].wr_queue_len)
        continue;

      uint32_t p_num = 0;
      while(qp_ctx[i].wr_queue_len - p_num > 0 && shm_ctx->btenant_can_post[tenant_id] && !(tenant_ctx.allowed_qps_num == MAX_ALLOWED_QP_NUM + shm_ctx->additional_qps_num[tenant_id] && qp_ctx[i].is_paused))
      { 
        //LOG_ERROR("%d %d %d\n", mlx5_get_sq_num(qp_ctx[i].qp), qp_ctx[i].wr_queue_len, p_num);
        struct ibv_send_wr* wr = get_queued_wr(i, p_num);

        if(wr == NULL)
          break;
        /*
           uint64_t size = 0;
           for(uint64_t i=0; i<wr->num_sge; i++)
           size += wr->sg_list[i].length;
           */

        if(tenant_ctx.max_msg_size < PERF_LARGE_FLOW)
        {
          uint32_t nreq = 1;
          struct ibv_send_wr* tmp = wr;
          bool get_pass = true;

          while(tmp->next != NULL)
          {
            if(qp_ctx[i].wr_queue_len == 0)
            {
              LOG_ERROR("POST BG SEND Error not including singal_wr\n");
              exit(1);
            }
            tmp = get_queued_wr(i, p_num + nreq);

            if(tmp == NULL)
            {
              get_pass = false;
              break;
            }

            nreq++;
          }

          if(get_pass && perf_small_process(i, wr, nreq))
            p_num += nreq;
          else
            break;

          if(p_num >= qp_ctx[i].wr_queue_size / 2)
          {
            dequeue_wr(i, p_num);
            p_num = 0;
          }

        }
        else
        {
          uint64_t size = 0;
          for(uint64_t i=0; i<wr->num_sge; i++)
            size += wr->sg_list[i].length;

          wr->next = NULL;

          if(perf_large_process(i, wr, size))
          {
            p_num++;
            if(p_num > 8)
            {
              dequeue_wr(i, p_num);
              p_num = 0;
              break;
            }
          }
          else
            break;
        }

        //LOG_ERROR("Pause WRs Processed: %d %d %d\n", i,  qp_ctx[i].wr_queue_len, p_num);
      }

      if(p_num)
      {
        dequeue_wr(i, p_num);
      }
    }

    if(manage_stop)
      break;
  }

}

void perf_recv_wr_queue_manage()
{
  for(uint32_t i=0; i<global_qnum; i++)
  {
    if(!qp_ctx[i].recv_wr_queue_len)
      continue;

    //LOG_ERROR("Rcev Queue Len: %d\n", qp_ctx[i].recv_wr_queue_len);
    bool p_comp = false;
    while(qp_ctx[i].recv_wr_queue_len > 0)
    { 
      //LOG_ERROR("%d %d \n", mlx5_get_rq_num(qp_ctx[i].qp), qp_ctx[i].recv_wr_queue_len);
      struct ibv_recv_wr* wr = get_queued_recv_wr(i, 0);

      if(wr == NULL)
        break;

      uint64_t size = 0;
      for(uint64_t i=0; i<wr->num_sge; i++)
        size += wr->sg_list[i].length;

      wr->next = NULL;
      if(perf_large_recv_process(i, wr, size))
        p_comp = true;
      else
        break;

      if(p_comp)
        dequeue_recv_wr(i, 1);
      //LOG_ERROR("Pause WRs Processed: %d %d %d\n", i,  qp_ctx[i].wr_queue_len, p_num);
    }
  }
}

bool perf_small_process(uint32_t q_idx, struct ibv_send_wr *wr, uint32_t nreq)
{
  if(qp_ctx[q_idx].max_wr - mlx5_get_sq_num(qp_ctx[q_idx].qp) < nreq)
  {
    perf_early_poll_cq();
    if(qp_ctx[q_idx].max_wr - mlx5_get_sq_num(qp_ctx[q_idx].qp) < nreq)
    {
      return false;
    }
  }
        
  struct ibv_send_wr *bad_wr;

  //perf_preemption_process(qp_ctx[q_idx].qp, 0); 

  if(mlx5_post_send2(qp_ctx[q_idx].qp, wr, &bad_wr, 1))
  {
    LOG_ERROR("Bacground posting error in small process\n");
    exit(1);
  }

  return true;
}

bool perf_large_process(uint32_t q_idx, struct ibv_send_wr *wr, uint64_t size)
{
  uint64_t chunk_num = ceil(size / (double)CHUNK_SIZE);
  struct ibv_send_wr* bad_wr;

  uint64_t origin_length = wr->sg_list[0].length;
  uint64_t origin_addr = wr->sg_list[0].addr;
  uint32_t origin_flag = wr->send_flags;
  uint64_t origin_wrid = wr->wr_id;
  struct timeval poll_start, poll_end;
  uint64_t poll_time = 5; //us
  bool poll_break = false;

  uint32_t i;
  //LOG_ERROR("perf_large_process start: %d %ld %ld %u %lu %lu\n", q_idx, size, chunk_num, origin_flag, origin_wrid, origin_length);
            
  for(i=0; i<chunk_num; i++)
  {
    while(qp_ctx[q_idx].chunk_sent_bytes >= CHUNK_SIZE)
    {
      if(shm_ctx->active_stenant_num)
      {
        if(DUMMY_FACTOR_2 != 0)
        {
          if(shm_ctx->active_mtenant_num == 0)
            DUMMY_FACTOR = DUMMY_FACTOR_2; // for large throughput
          else
            DUMMY_FACTOR = DUMMY_FACTOR_1; // for high message rate
        }

        uint32_t dummy_num = CHUNK_SIZE / DUMMY_FACTOR;
        struct ibv_exp_send_wr* dummy = qp_ctx[q_idx].dummy;
        struct ibv_exp_send_wr* exp_bad_wr;

        //LOG_ERROR("dummy send: %d %ld\n", dummy_num, qp_ctx[q_idx].chunk_sent_bytes);
        while(dummy_num)
        { 
          gettimeofday(&poll_start, NULL);
          while(qp_ctx[q_idx].max_wr - mlx5_get_sq_num(qp_ctx[q_idx].qp) < 11)
          {
            perf_early_poll_cq();
            gettimeofday(&poll_end, NULL);
            uint64_t t = (poll_end.tv_sec - poll_start.tv_sec) * 1000000 + (poll_end.tv_usec - poll_start.tv_usec);
            if(t > poll_time)
            {
              poll_break = true;
              break;
            }

          }

          if(poll_break)
            break;

          uint32_t can_post_num = qp_ctx[q_idx].max_wr - 10 - mlx5_get_sq_num(qp_ctx[q_idx].qp);
          can_post_num = can_post_num < dummy_num ? can_post_num : dummy_num;

          if(mlx5_exp_post_send2(qp_ctx[q_idx].qp, &(dummy[(uint32_t)(CHUNK_SIZE / DUMMY_FACTOR_1) - can_post_num]), &exp_bad_wr, 1) != 0)
          {
            LOG_ERROR("Send dummy error: %d %d\n", qp_ctx[q_idx].max_wr, mlx5_get_sq_num(qp_ctx[q_idx].qp));
            exit(1);
          }

          dummy_num -= can_post_num;
          qp_ctx[q_idx].chunk_sent_bytes -= (DUMMY_FACTOR * can_post_num);
          manage_stop = false;
        }

        if(poll_break)
          break;
        
        //qp_ctx[q_idx].chunk_sent_bytes -= (DUMMY_FACTOR * (uint32_t)(CHUNK_SIZE / DUMMY_FACTOR)) ;
        //LOG_ERROR("after dummy send: %d %ld %d\n", dummy_num, qp_ctx[q_idx].chunk_sent_bytes, mlx5_get_sq_num(qp_ctx[q_idx].qp));
        //LOG_ERROR("dummy send comp\n");
      }
      else
        qp_ctx[q_idx].chunk_sent_bytes = 0;
    }

    if(poll_break)
      break;

    gettimeofday(&poll_start, NULL);
    while(qp_ctx[q_idx].max_wr - mlx5_get_sq_num(qp_ctx[q_idx].qp) < 11)
    {
      perf_early_poll_cq();
      gettimeofday(&poll_end, NULL);
      uint64_t t = (poll_end.tv_sec - poll_start.tv_sec) * 1000000 + (poll_end.tv_usec - poll_start.tv_usec);
      if(t > poll_time)
      {
        poll_break = true;
        break;
      }
    }

    if(poll_break)
      break;

    if(chunk_num > 1)
    { 
      wr->wr_id = -1;
      wr->send_flags = 0;

      if(i % 8 == 0 || crail)
        wr->send_flags = IBV_SEND_SIGNALED;

      if(i < chunk_num - 1)
        wr->sg_list[0].length = CHUNK_SIZE;
      else
      {
        wr->send_flags = origin_flag;
        wr->wr_id = origin_wrid;
        wr->sg_list[0].length = origin_length - (i * CHUNK_SIZE);
      }

      wr->sg_list[0].addr = origin_addr + (i * CHUNK_SIZE);
    }
    
    //LOG_ERROR("perf_large_process start 1: %d %ld %d %ld %ld %d %d\n", q_idx, size, i, (uint64_t)(wr->sg_list[0].length), chunk_num, wr->send_flags, mlx5_get_sq_num(qp_ctx[q_idx].qp));

    if(mlx5_post_send2(qp_ctx[q_idx].qp, wr, &bad_wr, 1))
    {
      LOG_ERROR("qidx: %d, size:%lu, large process posting error in large process < MTU WR ID: %lu, addr: %lu, len: %d\n", q_idx, size, bad_wr->wr_id, bad_wr->sg_list[0].addr, bad_wr->sg_list[0].length);
      exit(1);
    }

    if(origin_length >= PERF_LARGE_FLOW) // require to improve!
      qp_ctx[q_idx].chunk_sent_bytes += wr->sg_list[0].length;

    manage_stop = false;
  }
  
  if(global_qnum > 1 && mqp_ctx && (global_qnum == mqp_ctx[0].manage_qnum || global_qnum == global_mqnum))
    perf_mqp_process();

  perf_update_active_state(q_idx);
  perf_update_tenant_state();

  if(i == chunk_num)
  {
    //LOG_ERROR("large procsss finished\n");
    wr->wr_id = origin_wrid;
    wr->send_flags = origin_flag;
    wr->sg_list[0].addr = origin_addr;
    wr->sg_list[0].length = origin_length;
    return true;
  }
  else
  {
    wr->wr_id = origin_wrid;
    wr->send_flags = origin_flag;
    wr->sg_list[0].length = origin_length - i * CHUNK_SIZE;
    wr->sg_list[0].addr = origin_addr + i * CHUNK_SIZE;
    return false;
  }
}


bool perf_large_recv_process(uint32_t q_idx, struct ibv_recv_wr *wr, uint64_t size)
{
  //LOG_ERROR("perf_large_recv_process start: %d %ld\n", q_idx, size);
  uint64_t chunk_num = ceil(size / (double)CHUNK_SIZE);
  struct ibv_recv_wr* bad_wr;

  //perf_preemption_process(qp_ctx[q_idx].qp, 0);
  uint64_t origin_length = wr->sg_list[0].length;
  uint64_t origin_addr = wr->sg_list[0].addr;
  uint64_t origin_wrid = wr->wr_id;

  uint32_t i;

  for(i=0; i<chunk_num; i++)
  {
    /*
    while(qp_ctx[q_idx].max_recv_wr - mlx5_get_rq_num(qp_ctx[q_idx].qp) == 0)
    {
      perf_early_poll_cq();
    }
    */
    if(qp_ctx[q_idx].max_recv_wr - mlx5_get_rq_num(qp_ctx[q_idx].qp) == 0)
    {
      perf_early_poll_cq();
      if(qp_ctx[q_idx].max_recv_wr - mlx5_get_rq_num(qp_ctx[q_idx].qp) == 0)
        break;
    }
 
    if(i < chunk_num - 1)
    {
      wr->sg_list[0].length = CHUNK_SIZE;
      wr->wr_id = -1;
    }
    else
    {
      wr->sg_list[0].length = origin_length - (i * CHUNK_SIZE);
    }

    wr->sg_list[0].addr = origin_addr + (i * CHUNK_SIZE);

    //LOG_ERROR("perf_large_recv_process start 1: %d %ld %d %ld %ld\n", q_idx, size, i, (uint64_t)(wr->sg_list[0].length), chunk_num);

    if(mlx5_post_recv2(qp_ctx[q_idx].qp, wr, &bad_wr, 1))
    {
      LOG_ERROR("Bacground posting error in large process < MTU\n");
      exit(1);
    }

    wr->wr_id = origin_wrid;
  }

  if(i == chunk_num)
  {
    return true;
  }
  else
  {
    wr->sg_list[0].length = origin_length - i * CHUNK_SIZE;
    wr->sg_list[0].addr = origin_addr + i * CHUNK_SIZE;
    return false;
  }
}

void perf_update_active_state(uint32_t q_idx)
{
  qp_ctx[q_idx].is_active = true;
}

void perf_preemption_process(struct ibv_qp  *qp, uint32_t nreq, bool is_read)
{
  if(!use_perf)
    return;

  if(mqp_ctx != NULL)
  {
    for(uint32_t i=0; i < global_mqnum; i++)
    {
      if(qp == mqp_ctx[i].qp)
        return;
    }
  }

  uint32_t q_idx = kh_value(qp_hash, kh_get(qph, qp_hash, qp->qp_num));

  if(is_read)
    qp_ctx[q_idx].is_reading = true;

  perf_update_active_state(q_idx);
  //LOG_ERROR("perf_preemption_process: %d\n", q_idx);

  if(crail)
    return;

  //if(!tenant_ctx.delay_sensitive && shm_ctx->active_tenant_num > 1 && (shm_ctx->active_qps_num > shm_ctx->max_qps_limit || shm_ctx->active_tenant_num != shm_ctx->active_stenant_num))
  if(!tenant_ctx.delay_sensitive && shm_ctx->active_tenant_num > 1 && shm_ctx->active_qps_num > shm_ctx->max_qps_limit)
  {
    if(qp_ctx[q_idx].is_first_wait)
    {
      //LOG_ERROR("perf_preemption_process: %d\n", q_idx);
      pthread_mutex_lock(&(tenant_ctx.wait_lock));
      if(!perf_check_paused(q_idx))
      {
        if(tenant_ctx.is_first_wait && qp_ctx[q_idx].is_first_wait)
        {
          //LOG_ERROR("Wait initialization: %d\n", q_idx);
          tenant_ctx.is_first_wait = false;
          qp_ctx[q_idx].is_first_wait = false;
          qp_ctx[q_idx].post_num = 0;

          gettimeofday(&(tenant_ctx.enabled_time[tenant_ctx.enabled_tail]), NULL);
          tenant_ctx.enabled_qps[tenant_ctx.enabled_tail] = q_idx;
          tenant_ctx.enabled_tail = (tenant_ctx.enabled_tail + 1) % global_qnum;
        }
        else if(!tenant_ctx.is_first_wait && qp_ctx[q_idx].is_first_wait)
        {
          //LOG_ERROR("Wait initialization 2: %d\n", q_idx);
          uint32_t mqp_idx = USE_MULTI_MQP ? q_idx : 0; 
          struct ibv_exp_send_wr *exp_bad_wr;

          if(mlx5_exp_post_send2(qp_ctx[q_idx].qp, &(mqp_ctx[mqp_idx].wait_cqe_wr), &exp_bad_wr, 1) != 0)
          {
            LOG_ERROR("Send wait error\n");
            exit(1);
          }

          tenant_ctx.wait_num++;
          qp_ctx[q_idx].is_first_wait = false;
          qp_ctx[q_idx].post_num = 0;

          tenant_ctx.waiting_qps[tenant_ctx.waiting_tail] = q_idx;
          tenant_ctx.waiting_tail = (tenant_ctx.waiting_tail + 1) % global_qnum;
        }
      }
      pthread_mutex_unlock(&(tenant_ctx.wait_lock));
    }
    qp_ctx[q_idx].post_num += nreq;

  }
  else
    qp_ctx[q_idx].post_num = 0;
}

int perf_poll_cq(struct ibv_cq *cq, uint32_t ne, struct ibv_wc *wc, int cqe_ver)
{
  if(!use_perf)
    return mlx5_poll_cq2(cq, ne, wc, cqe_ver, 1);
 
  while(rc_used && !read_qp_connected)
    sleep(1);

  if(!shm_ctx->btenant_can_post[tenant_id]) 
  {
    pthread_mutex_lock(&(tenant_ctx.poll_lock));
    while(!shm_ctx->btenant_can_post[tenant_id])
      pthread_cond_wait(&(tenant_ctx.poll_cond), &(tenant_ctx.poll_lock));
    pthread_mutex_unlock(&(tenant_ctx.poll_lock));
  } 

  uint32_t cq_idx = kh_value(cq_hash, kh_get(cqh, cq_hash, cq->handle));

  //pthread_mutex_lock(&(cq_ctx[cq_idx].lock));
  if(cq_ctx[cq_idx].early_poll_num)
  {
    int ret = cq_ctx[cq_idx].early_poll_num < ne ? cq_ctx[cq_idx].early_poll_num : ne;
    
    for(uint32_t i=0; i<ret; i++)
    {
      memcpy(wc + i, ((struct ibv_exp_wc*)cq_ctx[cq_idx].wc_list) + cq_ctx[cq_idx].wc_head, sizeof(struct ibv_wc));
      LOG_DEBUG("before real poll: %lu %d\n", (((struct ibv_exp_wc*)cq_ctx[cq_idx].wc_list) + cq_ctx[cq_idx].wc_head)->wr_id, (((struct ibv_exp_wc*)cq_ctx[cq_idx].wc_list) + cq_ctx[cq_idx].wc_head)->exp_opcode);
      cq_ctx[cq_idx].wc_head = (cq_ctx[cq_idx].wc_head + 1) % cq_ctx[cq_idx].max_cqe;

      //LOG_ERROR("real poll: %lu %d\n", wc[i].wr_id, wc[i].opcode);
    }

    cq_ctx[cq_idx].early_poll_num -= ret;
    //LOG_ERROR("Copy early polled: %d, wc_head: %d, %d %d\n", ret, cq_ctx[cq_idx].wc_head, cq_idx, cq_ctx[cq_idx].early_poll_num);
    //pthread_mutex_unlock(&(cq_ctx[cq_idx].lock));
    return ret;
  }
  //pthread_mutex_unlock(&(cq_ctx[cq_idx].lock));

  return mlx5_poll_cq2(cq, ne, wc, cqe_ver, 1);
 
  /* 
  int ret =   mlx5_poll_cq2(cq, ne, wc, cqe_ver, 1);
  
  if(ret > 0)
  {
    LOG_ERROR("Polled: %d\n", ret);
    for(uint32_t i=0; i<ret; i++)
    {
      LOG_ERROR("WC ID: %ld, Status: %d\n", wc[i].wr_id, wc[i].status);
    }
  }
  return  ret;
  */
  
}

void perf_create_read_qp()
{
	struct ibv_device      **dev_list;
	struct ibv_device	*ib_dev;

  char* env;  
  env = getenv("PERF_IB_PORT");
  if(env)
    read_qp_ctx.port_num = atoi(env);
  else
    read_qp_ctx.port_num = 1;
  
  env = getenv("PERF_MAX_READ_QP_NUM");
  if(env)
    MAX_READ_QP_NUM = atoi(env);
  
  env = getenv("PERF_MAX_READ_BATCH_NUM");
  if(env)
    MAX_READ_BATCH_NUM = atoi(env);
 
  dev_list = ibv_get_device_list(NULL);
	if (!dev_list) {
		LOG_ERROR("Failed to get IB devices list for read qp\n");
		exit(1);
	}

  ib_dev = *dev_list;
  if (!ib_dev) {
    LOG_ERROR("No IB devices found for read qp\n");
    exit(1);
  }

  read_qp_ctx.context = ibv_open_device(ib_dev); 
  if(!read_qp_ctx.context)
  {
    LOG_ERROR("No Context for read qp for read qp\n");
    exit(1);
  }

  read_qp_ctx.read_queue_size = read_qp_ctx.max_wr;
  read_qp_ctx.read_queue = (void*)malloc((sizeof(union perf_read_request) + 40) * (MAX_READ_BATCH_NUM + 1) * read_qp_ctx.read_queue_size + 100);

  read_qp_ctx.pd = ibv_alloc_pd(read_qp_ctx.context);
  if(!read_qp_ctx.pd)
  {
    LOG_ERROR("No PD for read qp\n");
    exit(1);
  }
  
  read_qp_ctx.mr = ibv_reg_mr(read_qp_ctx.pd, read_qp_ctx.read_queue, (sizeof(union perf_read_request) + 40) * (MAX_READ_BATCH_NUM + 1) * read_qp_ctx.read_queue_size + 100, IBV_ACCESS_LOCAL_WRITE);
  if(!read_qp_ctx.mr)
  {
    LOG_ERROR("No MR for read qp\n");
    exit(1);
  }

  read_qp_ctx.cq = ibv_create_cq(read_qp_ctx.context, read_qp_ctx.read_queue_size, NULL, NULL, 0);

  if(!read_qp_ctx.cq)
  {
    LOG_ERROR("No CQ for read qp\n");
    exit(1);
  }
 
  {
    struct ibv_qp_init_attr attr = {
      .send_cq = read_qp_ctx.cq,
      .recv_cq = read_qp_ctx.cq,
      .cap     = {
        .max_send_wr  = read_qp_ctx.max_wr,
        .max_recv_wr  = read_qp_ctx.max_wr,
        .max_send_sge = 1,
        .max_recv_sge = 1
      },
      .qp_type = IBV_QPT_UD,
    };

    read_qp_ctx.qp = ibv_create_qp(read_qp_ctx.pd, &attr);
  }

  {
    struct ibv_qp_attr attr = {
      .qp_state		= IBV_QPS_INIT,
      .pkey_index		= 0,
      .port_num		= read_qp_ctx.port_num,
      .qkey       = 0x1234
    };

    if (ibv_modify_qp(read_qp_ctx.qp, &attr,
          IBV_QP_STATE              |
          IBV_QP_PKEY_INDEX         |
          IBV_QP_PORT               |
          IBV_QP_QKEY)) {
      LOG_ERROR("Failed to modify QP to INIT for read qp\n");
      exit(1);
    }
  }
  
  {
    struct ibv_qp_attr attr = {
      .qp_state		= IBV_QPS_RTR
    };

    if (ibv_modify_qp(read_qp_ctx.qp, &attr, IBV_QP_STATE)) {
      LOG_ERROR("Failed to modify QP to RTR for read qp\n");
      exit(1);
    }

    attr.qp_state	    = IBV_QPS_RTS;
    attr.sq_psn	    = 0;

    if (ibv_modify_qp(read_qp_ctx.qp, &attr,
          IBV_QP_STATE              |
          IBV_QP_SQ_PSN)) {
      LOG_ERROR("Failed to modify QP to RTS for read qp\n");
      exit(1);
    }
  }
  
  read_qp_ctx.next_read_header = read_qp_ctx.read_queue;
  read_qp_ctx.read_queue_len = 0;
  read_qp_ctx.read_queue_tail = 0;
  read_qp_ctx.read_queue_head = 0;

  read_qp_ctx.remote_qpn = -1;
  pthread_mutex_init(&(read_qp_ctx.read_queue_lock), NULL);
  pthread_mutex_init(&(read_qp_ctx.read_qp_con_lock), NULL);
 
  read_qp_ctx.cmd_wr.sg_list = (struct ibv_sge*)malloc(sizeof(struct ibv_sge));
  read_qp_ctx.cmd_wr.sg_list[0].lkey = read_qp_ctx.mr->lkey;

  read_qp_ctx.cmd_wr.wr_id = -1;
  read_qp_ctx.cmd_wr.num_sge = 1;
  read_qp_ctx.cmd_wr.opcode = IBV_WR_SEND;
  read_qp_ctx.cmd_wr.send_flags = IBV_SEND_SIGNALED;
  read_qp_ctx.cmd_wr.imm_data   = 0;

  read_qp_ctx.cmd_recv_wr.wr_id = 0;
  read_qp_ctx.cmd_recv_wr.num_sge = 1;
  read_qp_ctx.cmd_recv_wr.sg_list = (struct ibv_sge*)malloc(sizeof(struct ibv_sge));
  read_qp_ctx.cmd_recv_wr.sg_list[0].lkey = read_qp_ctx.mr->lkey;
  read_qp_ctx.cmd_recv_wr.sg_list[0].addr = (uint64_t)read_qp_ctx.read_queue;
  read_qp_ctx.cmd_recv_wr.sg_list[0].length = sizeof(union perf_read_request) * (MAX_READ_BATCH_NUM + 1) + 40;

  read_qp_ctx.recv_wr.sg_list = &(read_qp_ctx.recv_sge);
  read_qp_ctx.send_wrs = (struct ibv_send_wr*)malloc(sizeof(struct ibv_send_wr) * MAX_READ_BATCH_NUM);
  read_qp_ctx.send_sge = (struct ibv_sge*)malloc(sizeof(struct ibv_sge) * MAX_SGE_LEN);


  for(uint32_t i=0; i<MAX_READ_BATCH_NUM;i++)
  {
    read_qp_ctx.send_wrs[i].wr_id = 0;
    read_qp_ctx.send_wrs[i].num_sge = 1;
    read_qp_ctx.send_wrs[i].sg_list = &(read_qp_ctx.send_sge[i]);

    read_qp_ctx.send_wrs[i].opcode = IBV_WR_RDMA_WRITE;
    read_qp_ctx.send_wrs[i].send_flags = 0;
    read_qp_ctx.send_wrs[i].imm_data = 0;
    read_qp_ctx.send_wrs[i].next = NULL;
  }

  post_cmd_recv_wrs(read_qp_ctx.max_wr);
}


void perf_set_dest_info(struct ibv_qp *qp, union ibv_gid dgid, int sgid_idx)
{
  if(!use_perf)
    return;

  if(qp_ctx[0].qp->qp_num == qp->qp_num && read_qp_ctx.ah == NULL)
  {
    struct ibv_ah_attr ah_attr = {
      .is_global = 1,
      .dlid = 0,
      .sl = 3,
      .src_path_bits = 0,
      .port_num = read_qp_ctx.port_num,
      .grh.hop_limit = 10,
      .grh.dgid = dgid,
      .grh.sgid_index = sgid_idx,
      .grh.traffic_class = 106
    };
    read_qp_ctx.dgid = dgid;
    read_qp_ctx.ah = ibv_create_ah(read_qp_ctx.pd, &ah_attr);

  }

  /*
  char wgid[33];
	uint32_t *raw = (uint32_t *)dgid.raw;
	int i;
	
	for (i = 0; i < 4; ++i)
		sprintf(&wgid[i * 8], "%08x",
			htonl(raw[i]));

  printf("Set Dest Info for UD QP: %u %s %lu %lu %d\n", qp->qp_num, wgid, dgid.global.subnet_prefix, dgid.global.interface_id, sgid_idx);
  */
}

void* perf_exchange_read_qpn()
{
  if(!use_perf)
    return NULL;

  if(read_qp_connected)
    return NULL;

  struct ibv_ah_attr ah_attr = {
    .is_global = 1,
    .dlid = 0,
    .sl = 3,
    .src_path_bits = 0,
    .port_num = read_qp_ctx.port_num,
    .grh.hop_limit = 10,
    //.grh.dgid = dgid,
    //.grh.sgid_index = sgid_idx,
    .grh.traffic_class = 106
  };

  bool is_server = false;
  char  remote_addr[16];
  union ibv_gid local_gid;
  union ibv_gid remote_gid;
  uint32_t local_qpn = read_qp_ctx.qp->qp_num;
  uint32_t remote_qpn = 0;
  
  int sock, conn;
  struct sockaddr_in address;
  struct sockaddr_in remote_address;

  char* env;
  env = getenv("PERF_GIDX");

  if(env)
    ah_attr.grh.sgid_index = atoi(env);
  else
    ah_attr.grh.sgid_index = 5;

  env = getenv("PERF_REMOTE_IP");
  if(env)
    strcpy(remote_addr, env);
  
  env = getenv("PERF_IS_SERVER");
  if(env)
  {
    if(atoi(env) == 1)
    {
      is_server = true;
      crail_type = CRAIL_NAMENODE;
    }
    else
    {
      is_server = false;
      crail_type = CRAIL_DATANODE;
    }
  }
  
  if(is_server)
    LOG_ERROR("Server Node\n");
  else
    LOG_ERROR("Client Node\n");

  LOG_ERROR("REMOTE IP: %s\n", remote_addr);
  ibv_query_gid(read_qp_ctx.context, read_qp_ctx.port_num, ah_attr.grh.sgid_index, &local_gid);

  if(is_server)
  {
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0)
    {
      LOG_ERROR("socket creation error\n");
      exit(1);
    }

    address.sin_family = AF_INET;
    address.sin_port = READ_PORT;
    address.sin_addr.s_addr = INADDR_ANY;

    int cnt = 0;
    while (bind(sock, (struct sockaddr*)&address, sizeof(address)) < 0)
    {
      address.sin_port = READ_PORT + cnt % 10;
      LOG_ERROR("Read_qp exchange Bind() error, change port READ Port: %d\n", READ_PORT);
      cnt++;
    }

    if(listen(sock, 1) != 0)
    {
      LOG_ERROR("Read_qp exchange Listen() error\n");
      exit(1);
    }

    socklen_t  namelen = sizeof(remote_address);

    if((conn = accept(sock, (struct sockaddr*)&remote_address, &namelen)) == -1)
    {
      LOG_ERROR("Read_qp exchange Accept() error\n");
      exit(1);
    }

    if(recv(conn, &remote_qpn, sizeof(remote_qpn), 0) == -1)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }

    if(send(conn, &local_qpn, sizeof(local_qpn), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }
    
    if(recv(conn, &remote_gid, sizeof(remote_gid), 0) == -1)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }

    if(send(conn, &local_gid, sizeof(local_gid), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }
  
    char wgid[33];
    uint32_t *raw = (uint32_t *)local_gid.raw;
    int i;
    
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("LOCAL GID for read_qp: %s\n", wgid);

    raw = (uint32_t *)remote_gid.raw;
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("Remote GID for read_qp: %s\n", wgid);

    close(conn);
    close(sock);
  }
  else
  {
    sleep(3);

    address.sin_family = AF_INET;
    address.sin_port = READ_PORT;
    inet_pton(AF_INET, remote_addr, &(address.sin_addr));

    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
      LOG_ERROR("Read_qp exchange Socket() error\n");
      exit(1);
    }

    int cnt = 0;

    while(connect(sock, (struct sockaddr*)&address, sizeof(address)) < 0)
    {
      address.sin_port = READ_PORT + cnt%10;

      LOG_ERROR("Read_qp connect faild, retry..., retry&change Port: %d\n", address.sin_port);
      sleep(1);
      cnt++;
    }

    if(send(sock, &local_qpn, sizeof(local_qpn), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }

    if(recv(sock, &remote_qpn, sizeof(remote_qpn), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }
 
    if(send(sock, &local_gid, sizeof(local_gid), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }
    
    if(recv(sock, &remote_gid, sizeof(remote_gid), 0) == -1)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }
 
    char wgid[33];
    uint32_t *raw = (uint32_t *)local_gid.raw;
    int i;
    
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("LOCAL GID for read_qp: %s\n", wgid);

    raw = (uint32_t *)remote_gid.raw;
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("Remote GID for read_qp: %s\n", wgid);


    close(sock);
    sleep(1);
  }

  read_qp_ctx.dgid = remote_gid;
  ah_attr.grh.dgid = remote_gid;
  read_qp_ctx.ah = ibv_create_ah(read_qp_ctx.pd, &ah_attr);
  read_qp_ctx.remote_qpn = remote_qpn;

  //for crail
  read_qp_ctx.ah_list[0] = read_qp_ctx.ah;
  read_qp_ctx.rqpn_list[0] = read_qp_ctx.remote_qpn;

  LOG_ERROR("LOCAL_QPN: %d, REMOTE_QPN: %d\n", read_qp_ctx.qp->qp_num, read_qp_ctx.remote_qpn);
  
  read_qp_connected = 1;
  pthread_mutex_unlock(&read_qp_ctx.read_qp_con_lock);

  return NULL;
}

void* perf_exchange_read_qpn2()
{
  if(!use_perf)
    return NULL;

  if(read_qp_connected)
    return NULL;

  struct ibv_ah_attr ah_attr = {
    .is_global = 1,
    .dlid = 0,
    .sl = 3,
    .src_path_bits = 0,
    .port_num = read_qp_ctx.port_num,
    .grh.hop_limit = 10,
    //.grh.dgid = dgid,
    //.grh.sgid_index = sgid_idx,
    .grh.traffic_class = 106
  };

  bool is_server = false;
  char  remote_addr[16];
  union ibv_gid local_gid;
  union ibv_gid remote_gid;
  uint32_t local_qpn = read_qp_ctx.qp->qp_num;
  uint32_t remote_qpn = 0;
  
  int sock, conn;
  struct sockaddr_in address;
  struct sockaddr_in remote_address;

  char* env;
  env = getenv("PERF_GIDX");

  if(env)
    ah_attr.grh.sgid_index = atoi(env);
  else
    ah_attr.grh.sgid_index = 5;

  env = getenv("PERF_REMOTE_IP_2");
  if(env)
  {
    strcpy(remote_addr, env);
    crail_type = CRAIL_CLIENT;
  }
  else
    is_server = true;
  
  if(is_server)
    LOG_ERROR("Server Node\n");
  else
    LOG_ERROR("Client Node\n");

  LOG_ERROR("REMOTE IP: %s\n", remote_addr);
  ibv_query_gid(read_qp_ctx.context, read_qp_ctx.port_num, ah_attr.grh.sgid_index, &local_gid);

  if(is_server)
  {
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0)
    {
      LOG_ERROR("socket creation error\n");
      exit(1);
    }

    address.sin_family = AF_INET;
    address.sin_port = READ_PORT;
    address.sin_addr.s_addr = INADDR_ANY;

    int cnt = 0;
    while (bind(sock, (struct sockaddr*)&address, sizeof(address)) < 0)
    {
      address.sin_port = READ_PORT + cnt % 10;
      LOG_ERROR("Read_qp exchange Bind() error, change port READ Port: %d\n", READ_PORT);
      cnt++;
    }

    if(listen(sock, 1) != 0)
    {
      LOG_ERROR("Read_qp exchange Listen() error\n");
      exit(1);
    }

    socklen_t  namelen = sizeof(remote_address);

    if((conn = accept(sock, (struct sockaddr*)&remote_address, &namelen)) == -1)
    {
      LOG_ERROR("Read_qp exchange Accept() error\n");
      exit(1);
    }

    if(recv(conn, &remote_qpn, sizeof(remote_qpn), 0) == -1)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }

    if(send(conn, &local_qpn, sizeof(local_qpn), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }
    
    if(recv(conn, &remote_gid, sizeof(remote_gid), 0) == -1)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }

    if(send(conn, &local_gid, sizeof(local_gid), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }
  
    char wgid[33];
    uint32_t *raw = (uint32_t *)local_gid.raw;
    int i;
    
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("LOCAL GID for read_qp: %s\n", wgid);

    raw = (uint32_t *)remote_gid.raw;
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("Remote GID for read_qp: %s\n", wgid);

    close(conn);
    close(sock);
  }
  else
  {
    sleep(3);

    address.sin_family = AF_INET;
    address.sin_port = READ_PORT;
    inet_pton(AF_INET, remote_addr, &(address.sin_addr));

    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
      LOG_ERROR("Read_qp exchange Socket() error\n");
      exit(1);
    }

    int cnt = 0;

    while(connect(sock, (struct sockaddr*)&address, sizeof(address)) < 0)
    {
      address.sin_port = READ_PORT + cnt%10;

      LOG_ERROR("Read_qp connect faild, retry..., retry&change Port: %d\n", address.sin_port);
      sleep(1);
      cnt++;
    }

    if(send(sock, &local_qpn, sizeof(local_qpn), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }

    if(recv(sock, &remote_qpn, sizeof(remote_qpn), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }
 
    if(send(sock, &local_gid, sizeof(local_gid), 0) < 0)
    {
      LOG_ERROR("Read_qp exchange Send() error\n");
      exit(1);
    }
    
    if(recv(sock, &remote_gid, sizeof(remote_gid), 0) == -1)
    {
      LOG_ERROR("Read_qp exchange Recv() error\n");
      exit(1);
    }
 
    char wgid[33];
    uint32_t *raw = (uint32_t *)local_gid.raw;
    int i;
    
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("LOCAL GID for read_qp: %s\n", wgid);

    raw = (uint32_t *)remote_gid.raw;
    for (i = 0; i < 4; ++i)
      sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
    LOG_ERROR("Remote GID for read_qp: %s\n", wgid);


    close(sock);
    sleep(1);
  }

  read_qp_ctx.dgid = remote_gid;
  ah_attr.grh.dgid = remote_gid;
  read_qp_ctx.ah = ibv_create_ah(read_qp_ctx.pd, &ah_attr);
  read_qp_ctx.remote_qpn = remote_qpn;

  //for crail
  read_qp_ctx.ah_list[1] = read_qp_ctx.ah;
  read_qp_ctx.rqpn_list[1] = read_qp_ctx.remote_qpn;
  
  LOG_ERROR("LOCAL_QPN: %d, REMOTE_QPN: %d\n", read_qp_ctx.qp->qp_num, read_qp_ctx.remote_qpn);
  
  read_qp_connected = 1;
  pthread_mutex_unlock(&read_qp_ctx.read_qp_con_lock);

  return NULL;
}

void send_read_request(uint32_t q_idx, struct ibv_send_wr *wr)
{
  pthread_mutex_lock(&read_qp_ctx.read_queue_lock);

  union perf_read_request* req_ptr = &(((union perf_read_request*)read_qp_ctx.read_queue)[read_qp_ctx.read_queue_tail]);
  struct perf_read_header* header_ptr = (struct perf_read_header*)(&(((union perf_read_request*)read_qp_ctx.read_queue)[read_qp_ctx.read_queue_tail]));
  struct perf_read_payload* payload_ptr = (struct perf_read_payload*)(&(((union perf_read_request*)read_qp_ctx.read_queue)[read_qp_ctx.read_queue_tail + 1]));
  
  uint32_t batch_num = 0;
  
  struct ibv_send_wr *bad_wr;
  struct ibv_recv_wr *bad_recv_wr;

//  while (wr != NULL) # read large batch not supported yet
//  {
    uint32_t i=0;
//    for(uint32_t i=0; i<wr->num_sge; i++) #scatter-gather not supported yet!
//    {
      payload_ptr[batch_num].addr = wr->sg_list[i].addr;
      payload_ptr[batch_num].len = wr->sg_list[i].length;
      payload_ptr[batch_num].lkey = wr->sg_list[i].lkey;
      payload_ptr[batch_num].remote_addr = wr->wr.rdma.remote_addr;
      payload_ptr[batch_num].rkey = wr->wr.rdma.rkey;
      batch_num++;
      //LOG_ERROR("Send %d read_request, ID: %lu, addr: %lu, length: %d, lkey: %d, remote_addr: %lu, rkey: %d\n", batch_num, wr->wr_id, wr->sg_list[i].addr, wr->sg_list[i].length, wr->sg_list[i].lkey, wr->wr.rdma.remote_addr, wr->wr.rdma.rkey);
 //   }

    uint32_t chunk_num = ceil(payload_ptr[batch_num-1].len / (double)CHUNK_SIZE);
    uint64_t last_chunk_offset = (chunk_num - 1) * CHUNK_SIZE;

    if((wr->send_flags & IBV_SEND_SIGNALED) != IBV_SEND_SIGNALED)
      read_qp_ctx.recv_wr.wr_id = -1;
    else
      read_qp_ctx.recv_wr.wr_id = wr->wr_id;

    read_qp_ctx.recv_wr.num_sge = 1;
    if(last_chunk_offset != 0 /*&& (chunk_num * CHUNK_SIZE) != wr[batch_num - 1].sg_list[0].length*/)
    {
      read_qp_ctx.recv_wr.sg_list[0].addr = payload_ptr[batch_num-1].addr + last_chunk_offset;
      read_qp_ctx.recv_wr.sg_list[0].length = payload_ptr[batch_num-1].len - last_chunk_offset;
    }
    else
    {
      read_qp_ctx.recv_wr.sg_list[0].addr = payload_ptr[batch_num-1].addr;
      read_qp_ctx.recv_wr.sg_list[0].length = payload_ptr[batch_num-1].len;
    }

    read_qp_ctx.recv_wr.sg_list[0].lkey = payload_ptr[batch_num-1].lkey;
    read_qp_ctx.recv_wr.next = NULL;

    //LOG_ERROR("Comp WR: %lu %lu %u\n", wr->wr_id, read_qp_ctx.recv_wr.sg_list[0].addr, read_qp_ctx.recv_wr.sg_list[0].length);
    if(mlx5_post_recv(qp_ctx[q_idx].qp, &(read_qp_ctx.recv_wr), &bad_recv_wr))
    {
      LOG_ERROR("Error, post recv error for PERF_READ: %d %d\n", read_qp_ctx.max_wr, mlx5_get_rq_num(read_qp_ctx.qp)); 
      exit(1);
    }

//    wr = wr->next;
//  }

  header_ptr->q_idx = q_idx;
  header_ptr->batch_num = batch_num;

  if(read_qp_ctx.read_queue_tail + MAX_READ_BATCH_NUM + 1 > read_qp_ctx.read_queue_size)
  {
    read_qp_ctx.read_queue_tail = 0;
    header_ptr->next_offset = 0;
  }
  else
  {
    read_qp_ctx.read_queue_tail += MAX_READ_BATCH_NUM + 1;
    header_ptr->next_offset = read_qp_ctx.read_queue_tail;
  }

  read_qp_ctx.cmd_wr.sg_list[0].addr   = (uint64_t)req_ptr;
  read_qp_ctx.cmd_wr.sg_list[0].length = sizeof(union perf_read_request) * (MAX_READ_BATCH_NUM + 1);
  
  //for crail
  if(crail)
  {
    if(crail_type == CRAIL_CLIENT)
      header_ptr->q_idx = 1;
    else if(crail_type == CRAIL_NAMENODE)
      header_ptr->q_idx = 0;
    
    read_qp_ctx.ah = read_qp_ctx.ah_list[q_idx];
    read_qp_ctx.remote_qpn = read_qp_ctx.rqpn_list[q_idx];
  }

  read_qp_ctx.cmd_wr.wr.ud.ah          = read_qp_ctx.ah;
  read_qp_ctx.cmd_wr.wr.ud.remote_qpn  = read_qp_ctx.remote_qpn;
  read_qp_ctx.cmd_wr.wr.ud.remote_qkey = 0x1234;
 
  if(mlx5_post_send2(read_qp_ctx.qp, &(read_qp_ctx.cmd_wr), &bad_wr, 1))
  {
    LOG_ERROR("Error, send read_request for PERF_READ: %d %d\n",mlx5_get_sq_num(read_qp_ctx.qp), read_qp_ctx.max_wr);
    exit(1);
  }

  pthread_mutex_unlock(&read_qp_ctx.read_queue_lock);

  perf_preemption_process(qp_ctx[q_idx].qp, batch_num, 0);

  struct ibv_wc wc[32];
  if(mlx5_poll_cq2(read_qp_ctx.cq, 32, wc, 1, 1))
  {
    LOG_ERROR("Error in POST READ CMD for PERF_READ\n");
    exit(1);
  }
}


void process_read_request(void* req_ptr, struct ibv_send_wr* wr, struct ibv_send_wr* comp_wr)
{
  union perf_read_request* req = (union perf_read_request*)req_ptr;

  uint32_t batch_num = req[0].header.batch_num;

  uint32_t i=0;
  //for(uint32_t i=0; i<batch_num; i++) #read large batch not supported yet
  //{
    //wr[i].wr_id = req[i+1].payload.wr_id;
    wr[i].wr_id = -1;
    wr[i].sg_list[0].addr = req[i+1].payload.remote_addr;
    wr[i].sg_list[0].length = req[i+1].payload.len;
    wr[i].sg_list[0].lkey = req[i+1].payload.rkey;

    if(i != batch_num -1)
    {
      wr[i].send_flags = 0;
      wr[i].opcode = IBV_WR_RDMA_WRITE;
      wr[i].wr.rdma.remote_addr = req[i+1].payload.addr;
      wr[i].wr.rdma.rkey = req[i+1].payload.lkey;
      wr[i].next = &(wr[i+1]);
    }
    //LOG_ERROR("%d addr: %lu, length: %d, lkey: %d\n",i, wr[i].sg_list[0].addr, wr[i].sg_list[0].length, wr[i].sg_list[0].lkey);
  //}

  uint32_t chunk_num = ceil(wr[batch_num - 1].sg_list[0].length / (double)CHUNK_SIZE);
  uint64_t last_chunk_offset = (chunk_num-1) * CHUNK_SIZE;

  if(last_chunk_offset != 0)
  {
   // if((chunk_num * CHUNK_SIZE) != wr[batch_num - 1].sg_list[0].length)
   // {
      comp_wr->wr_id = -1;
      comp_wr->send_flags = IBV_SEND_SIGNALED;
      comp_wr->opcode = IBV_WR_SEND;
      comp_wr->num_sge = 1;
      comp_wr->sg_list[0].addr = wr[batch_num-1].sg_list[0].addr + last_chunk_offset;
      comp_wr->sg_list[0].length = wr[batch_num-1].sg_list[0].length - last_chunk_offset;
      comp_wr->sg_list[0].lkey = wr[batch_num-1].sg_list[0].lkey;
      comp_wr->next = NULL;

      wr[batch_num-1].send_flags = 0;
      wr[batch_num-1].opcode = IBV_WR_RDMA_WRITE;
      wr[batch_num-1].wr.rdma.remote_addr = req[batch_num].payload.addr;
      wr[batch_num-1].wr.rdma.rkey = req[batch_num].payload.lkey;
      wr[batch_num-1].sg_list[0].length = last_chunk_offset;
      wr[batch_num-1].next = comp_wr;

      //LOG_ERROR("Comp WR: %lu %u\n", comp_wr->sg_list[0].addr,comp_wr->sg_list[0].length);
   // }
   // else
   // {

   // }
  }
  else
  {
    wr[batch_num-1].send_flags = IBV_SEND_SIGNALED;
    wr[batch_num-1].opcode = IBV_WR_SEND;
    wr[batch_num-1].next = NULL;
  }
}

void post_cmd_recv_wrs(uint32_t polled)
{
  struct ibv_recv_wr* bad_wr;
  
  while(polled)
  {
    if(mlx5_post_recv2(read_qp_ctx.qp, &(read_qp_ctx.cmd_recv_wr), &bad_wr, 1))
    {
      LOG_ERROR("Error, post recv error for PERF_READ: %d %d\n", read_qp_ctx.max_wr, mlx5_get_rq_num(read_qp_ctx.qp)); 
      exit(1);
    }
  
    read_qp_ctx.cmd_recv_wr.sg_list[0].addr += sizeof(union perf_read_request) * (MAX_READ_BATCH_NUM + 1) + 40;
    if(read_qp_ctx.cmd_recv_wr.sg_list[0].addr + sizeof(union perf_read_request) * (MAX_READ_BATCH_NUM + 1) + 40 > (uint64_t)read_qp_ctx.read_queue + (sizeof(union perf_read_request) + 40) * (MAX_READ_BATCH_NUM + 1) * read_qp_ctx.read_queue_size)
      read_qp_ctx.cmd_recv_wr.sg_list[0].addr = (uint64_t)read_qp_ctx.read_queue;
    
    polled--;
  }
}

int perf_poll_read_cmd()
{
  struct ibv_wc wc[32];
  return mlx5_poll_cq2(read_qp_ctx.cq, 32, wc, 1, 1);
  
  /*
  int ret = mlx5_poll_cq2(read_qp_ctx.cq, 16, wc, 1, 1);

  if(ret < 0)
  {
    LOG_ERROR("Error in post read_cmd\n");
    exit(1);
  }
  else if(ret > 0)
  {
    //LOG_ERROR("CMD Polled: %d\n", ret);
    for(int i=0; i<ret; i++)
    {
      if(wc[i].status != 0)
      {
        LOG_ERROR("CMD Polling Err: %d %d\n", wc[i].status, wc[i].vendor_err);
      }
    }
  }

  return read_qp_ctx.max_wr - mlx5_get_rq_num(read_qp_ctx.qp); 
  */
}

void* get_next_read_header()
{
  void* ret = read_qp_ctx.next_read_header + 40;
    
  if((uint64_t)(read_qp_ctx.next_read_header + sizeof(union perf_read_request) * (MAX_READ_BATCH_NUM + 1) + 40) > (uint64_t)read_qp_ctx.read_queue + (sizeof(union perf_read_request) + 40) * (MAX_READ_BATCH_NUM + 1) * read_qp_ctx.read_queue_size)
    read_qp_ctx.next_read_header = read_qp_ctx.read_queue;
  else
    read_qp_ctx.next_read_header += (40 + (MAX_READ_BATCH_NUM + 1) * sizeof(union perf_read_request));

  return ret;
}

void perf_resp_read_cmd()
{
  uint32_t polled = read_qp_ctx.max_wr - mlx5_get_rq_num(read_qp_ctx.qp);
  struct ibv_send_wr* bad_wr;
  
  uint32_t post_num = 0;
  //LOG_ERROR("Polled: %d\n", polled);
  while(polled - post_num)
  {
    void* req_ptr = get_next_read_header();
    uint32_t q_idx = ((union perf_read_request*)req_ptr)->header.q_idx;

    if(qp_ctx[q_idx].qp->state != IBV_QPS_RTS)
    {
      struct ibv_qp* qp = qp_ctx[q_idx].qp;
      struct ibv_qp_attr attr;

      memset(&attr, 0, sizeof(attr));

      attr.qp_state     = IBV_QPS_RTS;
      attr.sq_psn     = 0;
      attr.timeout      = 14;
      attr.retry_cnt      = 7;
      attr.rnr_retry      = 7; /* infinite */
      attr.max_rd_atomic  = 1;

      if (ibv_modify_qp(qp, &attr,
            IBV_QP_STATE              |
            IBV_QP_TIMEOUT            |
            IBV_QP_RETRY_CNT          |
            IBV_QP_RNR_RETRY          |
            IBV_QP_SQ_PSN             |
            IBV_QP_MAX_QP_RD_ATOMIC)) {
        LOG_ERROR("Failed to modify QP to RTS for PERF_READ\n");
        exit(1);;
      }

      //LOG_ERROR("SUCCESS Change to RTS for PERF_READ\n");
    }
   
    struct ibv_send_wr comp_wr;
    struct ibv_sge sge;
    comp_wr.sg_list = &sge;

    union perf_read_request* req = (union perf_read_request*)req_ptr;
    if(req[0].header.batch_num == 0)
    {
      qp_ctx[req[0].header.q_idx].is_active = true;
      tenant_ctx.passive_reading = true;
      tenant_ctx.passive_delay_sensitive = req[0].header.read_delay_sensitive ? true : false;
      tenant_ctx.passive_msg_sensitive = req[0].header.read_msg_sensitive ? true : false;
    }
    else
    {
      process_read_request(req_ptr, read_qp_ctx.send_wrs, &comp_wr);

      while(qp_ctx[q_idx].max_wr - mlx5_get_sq_num(qp_ctx[q_idx].qp) <  ((union perf_read_request*)req_ptr)->header.batch_num + 1)
      {
        perf_early_poll_cq();
        /*
        struct ibv_wc wc[32];
        mlx5_poll_cq2(qp_ctx[q_idx].qp->send_cq, 32, wc, 1, 1);
        */
      }

      int ret; 
      ret = mlx5_post_send(qp_ctx[q_idx].qp, read_qp_ctx.send_wrs, &bad_wr);
      if(ret)
      {
        LOG_ERROR("POST Send Error for PERF_READ: %d %d %d\n", qp_ctx[q_idx].max_wr, mlx5_get_sq_num(qp_ctx[q_idx].qp), ret);
        exit(1);
      }
    }

    post_num++;
  }

  post_cmd_recv_wrs(polled);
}

void perf_send_read_report(uint32_t q_idx)
{
  pthread_mutex_lock(&read_qp_ctx.read_queue_lock);
  union perf_read_request* req_ptr = &(((union perf_read_request*)read_qp_ctx.read_queue)[read_qp_ctx.read_queue_tail]);
  struct perf_read_header* header_ptr = (struct perf_read_header*)(&(((union perf_read_request*)read_qp_ctx.read_queue)[read_qp_ctx.read_queue_tail]));
  
  struct ibv_send_wr *bad_wr;

  header_ptr->q_idx = q_idx;
  header_ptr->batch_num = 0;
  header_ptr->read_report = 1;
  header_ptr->read_delay_sensitive = (shm_ctx->delay_sensitive[tenant_id] && !tenant_ctx.is_bw_read) ? 1 : 0;
  header_ptr->read_msg_sensitive = (shm_ctx->msg_sensitive[tenant_id] && !tenant_ctx.is_bw_read) ? 1 : 0;

  if(read_qp_ctx.read_queue_tail + MAX_READ_BATCH_NUM + 1 > read_qp_ctx.read_queue_size)
  {
    read_qp_ctx.read_queue_tail = 0;
    header_ptr->next_offset = 0;
  }
  else
  {
    read_qp_ctx.read_queue_tail += MAX_READ_BATCH_NUM + 1;
    header_ptr->next_offset = read_qp_ctx.read_queue_tail;
  }

  read_qp_ctx.cmd_wr.sg_list[0].addr   = (uint64_t)req_ptr;
  read_qp_ctx.cmd_wr.sg_list[0].length = sizeof(union perf_read_request) * (MAX_READ_BATCH_NUM + 1);
  
  //for crail 
  if(crail)
  {
    if(crail_type == CRAIL_CLIENT)
      header_ptr->q_idx = 1;
    else if(crail_type == CRAIL_NAMENODE)
      header_ptr->q_idx = 0;
    
    read_qp_ctx.ah = read_qp_ctx.ah_list[q_idx];
    read_qp_ctx.remote_qpn = read_qp_ctx.rqpn_list[q_idx];
  }

  read_qp_ctx.cmd_wr.wr.ud.ah          = read_qp_ctx.ah;
  read_qp_ctx.cmd_wr.wr.ud.remote_qpn  = read_qp_ctx.remote_qpn;
  read_qp_ctx.cmd_wr.wr.ud.remote_qkey = 0x1234;

  if(mlx5_post_send2(read_qp_ctx.qp, &(read_qp_ctx.cmd_wr), &bad_wr, 1))
  {
    LOG_ERROR("Error, send read_request for PERF_READ: %d %d\n",mlx5_get_sq_num(read_qp_ctx.qp), read_qp_ctx.max_wr);
    exit(1);
  }

  pthread_mutex_unlock(&read_qp_ctx.read_queue_lock);

  struct ibv_wc wc[32];
  if(mlx5_poll_cq2(read_qp_ctx.cq, 32, wc, 1, 1))
  {
    LOG_ERROR("Error in POST READ CMD for PERF_READ\n");
    exit(1);
  }
}

//EXP is not supported yet.
int perf_exp_process(struct ibv_qp *qp, struct ibv_exp_send_wr *wr)
{
  return PERF_BYPASS;
}
void perf_exp_small_process(uint32_t q_idx, struct ibv_exp_send_wr *wr, uint32_t nreq) {}
void perf_exp_large_process(uint32_t q_idx, struct ibv_exp_send_wr *wr, uint64_t size) {}
void perf_exp_bg_post(struct ibv_qp *qp, struct ibv_exp_send_wr *wr) {}
void perf_exp_poll_post(struct ibv_qp *qp, struct ibv_exp_send_wr *wr, uint32_t size) {}
int perf_exp_recv_process(struct ibv_qp *qp, struct ibv_recv_wr *wr)
{
  return PERF_BYPASS;
}
void perf_exp_bg_recv_post(struct ibv_qp *qp, struct ibv_recv_wr *wr) {}
int perf_exp_poll_cq(struct ibv_cq *cq, uint32_t ne, struct ibv_exp_wc *wc, uint32_t wc_size, int cqe_ver)
{
  if(!use_perf)
    return mlx5_poll_cq_ex2(cq, ne, wc, wc_size, cqe_ver, 1);
  
	return mlx5_poll_cq_ex2(cq, ne, wc, wc_size, cqe_ver, 1);
}





