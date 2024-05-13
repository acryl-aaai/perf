#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <fcntl.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define MAX_TENANT_NUM 3000
#define QPS_CHECK_INTERVAL 10000 //10ms
#define PRINT_INTERVAL 1000000 //1s
#define BTENANT_ENABLE_TIME 100000 //100ms

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

void main()
{
  struct perf_shm_context* shm_ctx = NULL;
  int shm_fd;

  printf("Init perf_shm\n");
  shm_fd = shm_open("/perf-shm", O_CREAT | O_RDWR, 0666);

  if(shm_fd == -1)
  {
    printf("Open perf_shm is failed\n");
    exit(1);
  }

  if(ftruncate(shm_fd, sizeof(struct perf_shm_context)) < 0)
    printf("ftruncate error\n");

  shm_ctx = (struct perf_shm_context*) mmap(0, sizeof(struct perf_shm_context), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

  if (shm_ctx == MAP_FAILED){
    printf("Error mapping shared memory perf_shm");
    exit(1);
  }

  shm_ctx->next_tenant_id = 0;
  shm_ctx->tenant_num = 0;
  shm_ctx->active_tenant_num = 0;
  shm_ctx->active_qps_num = 0;
  shm_ctx->active_stenant_num = 0;
  shm_ctx->active_mtenant_num = 0;
  shm_ctx->active_dtenant_num = 0;
  
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
  pthread_mutex_init(&(shm_ctx->lock), &attr);

  pthread_condattr_t attrcond;
  pthread_condattr_init(&attrcond);
  pthread_condattr_setpshared(&attrcond, PTHREAD_PROCESS_SHARED);

  for(uint32_t i=0; i<MAX_TENANT_NUM; i++)
  {
    shm_ctx->active_qps_per_tenant[i] = 0;
    shm_ctx->additional_qps_num[i] = 0;
    shm_ctx->delay_sensitive[i] = true;
    shm_ctx->msg_sensitive[i] = false;
    shm_ctx->resp_read[i] = false;
    shm_ctx->avg_msg_size[i] = 16;
    shm_ctx->btenant_can_post[i] = true;
    pthread_mutex_init(&(shm_ctx->perf_thread_lock[i]), &attr);
    pthread_cond_init(&(shm_ctx->perf_thread_cond[i]), &attrcond);
  }

  uint32_t NIC_QPS_CAPA = 8;
  uint32_t MAX_MSG_RATE = 12400; // Kpps
  uint64_t NIC_LINK_BW = 100000; // Mbps
  uint32_t MSEN_QP_LIMIT = 1;
  uint32_t MAX_SIM_BTENANT_NUM = 1;

  char* env = getenv("PERF_NIC_QPS_CAPA");
  if(env)
    NIC_QPS_CAPA = atoi(env);
  
  env = getenv("PERF_NIC_LINK_BW");
  if(env)
    NIC_LINK_BW = atoi(env);

  env = getenv("PERF_NIC_MSG_RATE");
  if(env)
    MAX_MSG_RATE = atoi(env);

  env = getenv("PERF_MSEN_QP_LIMIT");
  if(env)
    MSEN_QP_LIMIT = atoi(env);
  
  env = getenv("PERF_MAX_SIM_BTENANT_NUM");
  if(env)
    MAX_SIM_BTENANT_NUM = atoi(env);

  struct timeval now;
  struct timeval qps_check_timer;
  struct timeval print_timer;

  gettimeofday(&qps_check_timer, NULL);
  gettimeofday(&print_timer, NULL);

  struct timeval last_enable_btenant_time;
  gettimeofday(&last_enable_btenant_time, NULL);
  uint32_t post_stop_num = 0;
  uint32_t can_post_num = 0;

  uint32_t last_enable_btenant_idx = -1;
  uint32_t last_add_qp_tenant = -1;

  printf("PERF_NIC_QPS_CAPA: %d, PERF_NIC_LINK_BW: %ld, PERF_NIC_MSG_RATE: %d PERF_MSEN_QP_LIMIT: %d PERF_MAX_SIM_BTENANT_NUM: %d\n", NIC_QPS_CAPA, NIC_LINK_BW, MAX_MSG_RATE, MSEN_QP_LIMIT, MAX_SIM_BTENANT_NUM);
  while(true)
  {
    uint32_t atn=0, aqn=0, aqmn=0, asn=0, amn=0, adn=0, arrn = 0, abn=0;
    uint64_t max_msg_size = 0;
    uint32_t tnum = 0;

    can_post_num = 0;
    for(uint32_t i=0; i<MAX_TENANT_NUM; i++)
    {
      tnum++;
      if(shm_ctx->active_qps_per_tenant[i])
      {
        atn++;
        aqn += shm_ctx->active_qps_per_tenant[i];

        if(shm_ctx->delay_sensitive[i])
        {
          asn++;
          adn++;
        }
        else if(shm_ctx->msg_sensitive[i])
        {
          asn++;
          amn++;
          aqmn += shm_ctx->active_qps_per_tenant[i];
        }
        else
        {
          abn++;
          if(shm_ctx->btenant_can_post[i])
            can_post_num++;
        }

        if(shm_ctx->resp_read[i])
          arrn++;

        if(max_msg_size < shm_ctx->avg_msg_size[i] && !shm_ctx->delay_sensitive[i])
          max_msg_size = shm_ctx->avg_msg_size[i];

        if(shm_ctx->active_qps_per_tenant[i] <= 1)
          shm_ctx->additional_qps_num[i] = 0;

      }

      if(tnum == shm_ctx->tenant_num)
        break;
    }

    uint32_t fu_qp_num;
    if(max_msg_size == 0)
      fu_qp_num = NIC_QPS_CAPA;
    else
      fu_qp_num = (NIC_LINK_BW * 0.7 /(double)(max_msg_size * 8))/(double)(MAX_MSG_RATE / 1000.0);     

    if(fu_qp_num == 0)
      fu_qp_num = 1;

    pthread_mutex_lock(&(shm_ctx->lock));

    shm_ctx->active_tenant_num = atn;
    shm_ctx->active_qps_num = aqn;
    shm_ctx->active_stenant_num = asn;
    shm_ctx->active_mtenant_num = amn;
    shm_ctx->active_dtenant_num = adn;
    shm_ctx->active_rrtenant_num = arrn;
    shm_ctx->max_qps_limit = NIC_QPS_CAPA > fu_qp_num ? fu_qp_num : NIC_QPS_CAPA;

    uint32_t use_multiple_qps = 0;
    uint32_t mqp_tenants[MAX_TENANT_NUM];
    uint32_t add_qps_num[MAX_TENANT_NUM];
    uint32_t active_cnt = 0;

    for(uint32_t i=last_add_qp_tenant + 1; i<MAX_TENANT_NUM + last_add_qp_tenant + 1; i++)
    {
      if(shm_ctx->active_qps_per_tenant[i % MAX_TENANT_NUM])
      {
        if(shm_ctx->active_qps_per_tenant[i % MAX_TENANT_NUM] > 1)
        {
          mqp_tenants[use_multiple_qps] = i % MAX_TENANT_NUM;
          add_qps_num[use_multiple_qps] = 0;
          use_multiple_qps++;
        }
        
        active_cnt++;
      }

      if(active_cnt == shm_ctx->active_tenant_num)
        break;
    }

    uint32_t available_capa = shm_ctx->max_qps_limit > shm_ctx->active_tenant_num ? shm_ctx->max_qps_limit - shm_ctx->active_tenant_num : 0;

    uint32_t idx = 0;
    while(available_capa && use_multiple_qps)
    {
      bool repeat = false;
      bool no_chance = true;
      for(uint32_t i=0; i<use_multiple_qps; i++)
      {
        idx = mqp_tenants[i];
        if(shm_ctx->active_qps_per_tenant[idx] > 1 + add_qps_num[idx])
        {
          add_qps_num[i]++;
          last_add_qp_tenant = idx;
          available_capa--;

          if(!available_capa)
            break;

          repeat = true;
          no_chance = false;
        }
      }
      if(!repeat || no_chance)
        break;
    }
    

    if(abn > 0 && adn == 0 && use_multiple_qps)
    {
      int available_msen_capa = (int)MSEN_QP_LIMIT - (int)amn;
      while(available_msen_capa > 0 )
      {
        bool repeat = false;
        bool no_chance = true;
        for(uint32_t i=0; i<use_multiple_qps; i++)
        {
          idx = mqp_tenants[i];
          if(shm_ctx->msg_sensitive[idx] && shm_ctx->active_qps_per_tenant[idx] > 1 + add_qps_num[idx])
          {
            add_qps_num[i]++;
            last_add_qp_tenant = idx;
            available_msen_capa--;
            if(!available_msen_capa)
              break;

            repeat = true;
            no_chance = false;
          }

        }
        if(!repeat || no_chance)
          break;
      }
    }
    
    for(uint32_t i=0; i<use_multiple_qps; i++)
    {
      idx = mqp_tenants[i];
      shm_ctx->additional_qps_num[idx] = add_qps_num[i];
      //printf("Allow additonal QPs: %d %d\n", idx, shm_ctx->additional_qps_num[idx]);
    }

    if(abn > 4 && can_post_num >= MAX_SIM_BTENANT_NUM && asn)
    {
      gettimeofday(&now, NULL);
      uint64_t t = (now.tv_sec - last_enable_btenant_time.tv_sec) * 1000000 + (now.tv_usec - last_enable_btenant_time.tv_usec);
      
      if(t > BTENANT_ENABLE_TIME || post_stop_num < abn - MAX_SIM_BTENANT_NUM || can_post_num > MAX_SIM_BTENANT_NUM)
      {
        uint32_t last = last_enable_btenant_idx;

        for(uint32_t i=last+1; i<MAX_TENANT_NUM+last+1; i++)
        {
          uint32_t idx = i % MAX_TENANT_NUM;
          if(shm_ctx->active_qps_per_tenant[idx] && !shm_ctx->delay_sensitive[idx] && !shm_ctx->msg_sensitive[idx])
          {
            if(can_post_num == MAX_SIM_BTENANT_NUM && t > BTENANT_ENABLE_TIME) 
            {
              if(!shm_ctx->btenant_can_post[idx])
              {
                pthread_mutex_lock(&(shm_ctx->perf_thread_lock[idx]));
                if(last_enable_btenant_idx != -1)
                  shm_ctx->btenant_can_post[last_enable_btenant_idx] = false;
                else
                  can_post_num++;
                shm_ctx->btenant_can_post[idx] = true;
                pthread_cond_signal(&(shm_ctx->perf_thread_cond[idx]));
                pthread_mutex_unlock(&(shm_ctx->perf_thread_lock[idx]));
                last_enable_btenant_idx = idx;
                last_enable_btenant_time = now;
                t = 0;
                //printf("change enable: %d\n", idx);
              }
            }
            else if(can_post_num > MAX_SIM_BTENANT_NUM)
            {
              if(shm_ctx->btenant_can_post[idx])
              {
                shm_ctx->btenant_can_post[idx] = false;
                can_post_num--;
                post_stop_num++;
                //printf("diable: %d\n", idx);
              }
            }

            //printf("%d: %d\n", idx, shm_ctx->btenant_can_post[idx]);
          }
        }
      }
    }
    else if(abn > 4 && can_post_num < MAX_SIM_BTENANT_NUM && asn)
    {
      uint32_t last = last_enable_btenant_idx;

      for(uint32_t i=last+1; i<MAX_TENANT_NUM+last+1; i++)
      {
        uint32_t idx = i % MAX_TENANT_NUM;
        if(shm_ctx->active_qps_per_tenant[idx] && !shm_ctx->delay_sensitive[idx] && !shm_ctx->msg_sensitive[idx])
        {
          if(!shm_ctx->btenant_can_post[idx])
          {
            pthread_mutex_lock(&(shm_ctx->perf_thread_lock[idx]));
            shm_ctx->btenant_can_post[idx] = true;
            last_enable_btenant_time = now;
            last_enable_btenant_idx = idx;
            //printf("newly enable: %d\n", idx);
            can_post_num++;
            post_stop_num--;
            pthread_cond_signal(&(shm_ctx->perf_thread_cond[idx]));
            pthread_mutex_unlock(&(shm_ctx->perf_thread_lock[idx]));
          }
        }
        
        if(can_post_num == MAX_SIM_BTENANT_NUM)
          break;
      }
    }
    else if(post_stop_num)
    {
      uint32_t last = last_enable_btenant_idx;

      for(uint32_t i=last+1; i<MAX_TENANT_NUM+last+1; i++)
      {
        uint32_t idx = i % MAX_TENANT_NUM;
        if(!shm_ctx->btenant_can_post[idx])
        {
          pthread_mutex_lock(&(shm_ctx->perf_thread_lock[idx]));
          shm_ctx->btenant_can_post[idx] = true;
          last_enable_btenant_time = now;
          last_enable_btenant_idx = idx;
          //printf("refresh enable: %d\n", idx);
          can_post_num++;
          post_stop_num--;
          pthread_cond_signal(&(shm_ctx->perf_thread_cond[idx]));
          pthread_mutex_unlock(&(shm_ctx->perf_thread_lock[idx]));
        }

        if(!post_stop_num)
        {
          last_enable_btenant_idx = -1;
          break;
        }
      }
    }

    pthread_mutex_unlock(&(shm_ctx->lock));

  //printf("Instant Global Tenant Num: %d, Active Tenant Num: %d, Active QPs Num: %ld, Active Resp Read Tenant Num: %d,  Delay Sensitive Num: %d, Msg Senstivie Num: %d, Bandwidth Sesitive Num: %d, MAX_QPS_LIMIT: %d\n", shm_ctx->tenant_num, shm_ctx->active_tenant_num, shm_ctx->active_qps_num, shm_ctx->active_rrtenant_num, shm_ctx->active_dtenant_num, shm_ctx->active_mtenant_num, shm_ctx->active_tenant_num - shm_ctx->active_stenant_num, shm_ctx->max_qps_limit);
    qps_check_timer = now;

    gettimeofday(&now, NULL);
    uint64_t t = (now.tv_sec - print_timer.tv_sec) * 1000000 + (now.tv_usec - print_timer.tv_usec);

    if(t > PRINT_INTERVAL)
    {
      printf("Current Global Tenant Num: %d, Active Tenant Num: %d, Active QPs Num: %ld, Active Resp Read Tenant Num: %d,  Delay Sensitive Num: %d, Msg Senstivie Num: %d, Bandwidth Sesitive Num: %d, MAX_QPS_LIMIT: %d\n", shm_ctx->tenant_num, shm_ctx->active_tenant_num, shm_ctx->active_qps_num, shm_ctx->active_rrtenant_num, shm_ctx->active_dtenant_num, shm_ctx->active_mtenant_num, shm_ctx->active_tenant_num - shm_ctx->active_stenant_num, shm_ctx->max_qps_limit);

      print_timer = now;
    }

    usleep(QPS_CHECK_INTERVAL);
  }
}
 

