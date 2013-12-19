/*
 * test_data_generator.c
 *
 *  Created on: Dec 5, 2013
 *      Author(s): Oleg Klyudt
 */

/* This routine has two purposes:
 * 1. Unit testing of TSDB Wrapper API
 * 2. After unit tets completion it launches
 * data generator and issues writing queries
 * to TSDB Wrapper DB bundle. Another reading
 * routine should be invoked as a separate
 * process to read the DBs and assess its contents
 *
 * It also may serve as an example of TSDB Wrapper API
 * implementation and usage */

#include "tsdb_wrapper_api.h"
#include <unistd.h>
#include "seatest.h"

#define TSDB_DG_METRIC_NUM 6
#define TSDB_DG_EPOCHS_NUM 20
#define TSDB_DG_FINE_TS 2

int metrics_create(char ***metrics_arr) {

  char **metrics = *metrics_arr;
  u_int32_t cnt, rcnt;

  /* Outer allocation */
  metrics = (char **) malloc(TSDB_DG_METRIC_NUM * sizeof(char *));
  if (metrics == NULL) return -1;

  /* Inner allocation */
  for (cnt = 0; cnt < TSDB_DG_METRIC_NUM; ++cnt) {
      metrics[cnt] = (char *) malloc(MAX_METRIC_STRING_LEN * sizeof(char));
      if (metrics[cnt] == NULL) goto err_cleanup;
      if (sprintf(metrics[cnt], "m-%lu", cnt +1) < 0) goto err_cleanup;
  }

  return 0;

  err_cleanup:
  for (rcnt = 0; rcnt < cnt; ++rcnt) {
      free(metrics[rcnt]);
  }
  free(metrics);
  metrics = NULL;
  return -1;
}

void metrics_destroy(char ***metrics_arr) {
  char **metrics = *metrics_arr;
  u_int32_t cnt;
  for (cnt = 0; cnt < TSDB_DG_METRIC_NUM; ++cnt) free(metrics[cnt]);
  free(metrics);
  metrics = NULL;
}

int write_pattern_epoch(tsdbw_handle *db_bundle, int64_t **values, u_int8_t epoch_idx, u_int8_t metr_num, u_int8_t garbage_flag) {

  char **metrics; u_int8_t cnt;
  int rv;
  int64_t *vals_to_write;
  u_int16_t num_of_values = 0;

  vals_to_write = (int64_t *) calloc(metr_num, sizeof(int64_t));

  if (metrics_create(&metrics)) {return -1; }

  for (cnt = 0; cnt < metr_num; ++cnt) {
      vals_to_write[num_of_values] = values[cnt][epoch_idx];
      if (vals_to_write[num_of_values] != 0) {
          if (garbage_flag) vals_to_write[num_of_values] = 999; // garbage part, just some values bigger than those in the test
          rv = sprintf(metrics[num_of_values], "m-%u", cnt + 1); assert_true(rv >= 0);
          num_of_values++;
      }
  }

  rv = tsdbw_write(db_bundle, metrics, vals_to_write, num_of_values);
  assert_true(rv == 0);

  metrics_destroy(&metrics);

  return 0;
}

int create_pattern(int64_t ***values_p) {

  int64_t **values = *values_p;
  u_int32_t i, j;

  /* Allocate memory */
  values = (int64_t **)malloc(TSDB_DG_METRIC_NUM * sizeof(int64_t *));
  if (values == NULL) return -1;

  for (i = 0; i < TSDB_DG_METRIC_NUM; ++i) {
      values[i] = (int64_t *) calloc(TSDB_DG_EPOCHS_NUM, sizeof(int64_t));
      if (values[i] == NULL) return -1;
  }

  /* Creating a writing template, 0 - no value to write */
  for (i = 1; i < TSDB_DG_METRIC_NUM + 1; ++i) { //epochs i = 1:6
      for (j = 1; j < i + 1; ++j ) { //metrics            j = 1:1, 1:2, ..., 1:6
          values[j-1][i-1] = 10 * j;
      }
  }

  values[3-1][16-1] = 30;
  values[1-1][17-1] = 10;
  values[6-1][18-1] = 60;
  values[4-1][19-1] = 40;
  values[2-1][20-1] = 20;

  return 0;
}

int writing_cycle_engage(tsdbw_handle *db_bundle, int64_t **values) {

    u_int32_t epoch_cnt = 0, cur_epoch = time(NULL);
    u_int32_t last_epoch;

    normalize_epoch(db_bundle->db_hs[0], &cur_epoch);
    last_epoch = cur_epoch;

    while (epoch_cnt < TSDB_DG_EPOCHS_NUM) {

            epoch_cnt++;
            printf("Epoch %u. Writing... ", epoch_cnt);

            if (write_pattern_epoch(db_bundle, values, epoch_cnt -1, TSDB_DG_METRIC_NUM, 1)) {
                printf("Failed to write testing garbage\n");
                tsdbw_close(db_bundle);
                return -1;
            }

            if (write_pattern_epoch(db_bundle, values, epoch_cnt -1, TSDB_DG_METRIC_NUM, 0)) {
                printf("Failed to write pattern\n");
                tsdbw_close(db_bundle);
                return -1;
            }
            printf("Done\n");
            fflush(stdout);

            /* Wait for the epoch to change */
            while (cur_epoch == last_epoch) {
                cur_epoch = time(NULL);
                normalize_epoch(db_bundle->db_hs[0], &cur_epoch);
            }
            // Epoch rollover
            last_epoch = cur_epoch;
    }

  return 0;
}

int epochs_alligned(tsdbw_handle *db_bundle) {
  u_int8_t i;
  u_int32_t *epochs_db  = (u_int32_t *)malloc(TSDBW_DB_NUM * sizeof(u_int32_t));


  u_int32_t cur_time = (u_int32_t) time(NULL);
  for (i = 0; i < TSDBW_DB_NUM; ++i) {
      epochs_db[i] = cur_time;
      normalize_epoch(db_bundle->db_hs[i], &epochs_db[i]);
  }

  i = TSDBW_DB_NUM;

  while(--i>0 && epochs_db[i]==epochs_db[0]);

  free(epochs_db);

  return i == 0; // if all elements of an array are equal, i hits 0 and the routine returns true
}

u_int32_t emulate_outage(u_int32_t *time) {

  unsigned int time_uint = *time;

  u_int32_t time_left = sleep(time_uint);
  if (time_left){
      printf("Sleep phase was not finished. %u seconds left. Aborting\n", time_left);
      fflush(stdout);
  }
  return time_left;
}

int open_dbs(tsdbw_handle *db_bundle, const char **db_paths, char mode) {
  u_int16_t timestep = TSDB_DG_FINE_TS;
  if (!tsdbw_init(db_bundle, &timestep, db_paths, mode)) {
      printf("DBs are open!\n");
      return 0;
    }
  else {
      printf("Failed to open DBs\n"); return -1;
  }
}

int prepare_args_q_test1(tsdbw_handle *db_bundle, q_request_t *req) {

  req->granularity_flag = TSDBW_FINE;
  req->epoch_from = db_bundle->db_hs[0]->epoch_list[0] - db_bundle->db_hs[0]->slot_duration * 1.5;
  req->epoch_to = db_bundle->db_hs[0]->epoch_list[db_bundle->db_hs[0]->number_of_epochs] + db_bundle->db_hs[0]->slot_duration * 1.5;
  req->metrics_num = TSDB_DG_METRIC_NUM;

  char **metrics;
  if (metrics_create(&metrics) ) return -1;
  req->metrics = metrics;

  return 0;
}

int verify_reply_test1(tsdb_handler *h, q_reply_t *rep, u_int32_t *s_time) {

  /* form list of anticipated epochs */

  /* 2 from extra requested range: 1 epoch
   * before the first existing in the DB
   * and one epoch after the last existing
   * in the DB */
  u_int32_t num_epochs = (h->epoch_list[h->most_recent_epoch] - h->epoch_list[0]) / h->slot_duration +1 + 2;
  /* form 2D array (metrics, epochs) of anticipated data*/
  /* compare returned tuples with anticipated data in a loop across all epochs and metrics */

  return 0;
}


int main() {

  tsdbw_handle db_bundle;
  u_int32_t sleep_time;
  u_int8_t i;
  int64_t **values = NULL;
  int rv;
  const char *db_paths[3];
  db_paths[0] = "./DBs/f_db.tsdb";
  db_paths[1] = "./DBs/m_db.tsdb";
  db_paths[2] = "./DBs/c_db.tsdb";

  set_trace_level(99);

  /* Create test pattern of events */
  rv = create_pattern(&values); assert_true(rv == 0);

  /* Open DBs */
  rv = open_dbs(&db_bundle, db_paths, 'w'); assert_true(rv == 0);

  sleep_time = 2 * db_bundle.db_hs[2]->slot_duration; // 2 epochs of the coarse TSDB

  /* Writing epochs according to pattern once epochs in all TSDBs are aligned */
  while(!epochs_alligned(&db_bundle));

  rv = writing_cycle_engage(&db_bundle, values); assert_true(rv == 0);
  tsdbw_close(&db_bundle);

  /* Emulate outage time for DB */
  rv = emulate_outage(& sleep_time); assert_true(rv == 0);

  /* Open DBs in append mode this time*/
  rv = open_dbs(&db_bundle, db_paths, 'a'); assert_true(rv == 0);

  /* Commence writing duty cycle */
  rv = writing_cycle_engage(&db_bundle, values); assert_true(rv == 0);
  tsdbw_close(&db_bundle);

  /* Reading TSDBs */
  rv = open_dbs(&db_bundle, db_paths, 'r'); assert_true(rv == 0);

  /* 1 Read whole fine TSDB (with extra range) and check correctness of data */
  q_request_t req;
  q_reply_t rep;
  rv = prepare_args_q_test1(&db_bundle, &req); assert_true(rv == 0);
  rv = tsdbw_query(&db_bundle, &req, &rep); assert_true(rv == 0);
  rv = verify_reply_test1(db_bundle.db_hs[0], &rep, &sleep_time); assert_true(rv == 0);

  /* 2. Read whole moderate TSDB (with extra range) and check correctness of consolidated data */
  /* 3. Read whole coarse TSDB (with extra range) and check correctness of consolidated data */
  /* Testing querying of epoch ranges using times encompassing them: */
  /* 4. Fine TSDB: randomly read epoch ranges/metrics and check correctness  */
  /* 5. Moderate TSDB: randomly read epoch ranges/metrics and check correctness  */
  /* 6. Coarse TSDB: randomly read epoch ranges/metrics and check correctness  */


  for (i = 0; i < TSDB_DG_METRIC_NUM; ++i) free(values[i]);
  free(values);

  return 0;
}

