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
//TODO try out setting default value different from 0 when creating DB and the test must succeed still
//TODO try arbitrary pattern, it will be read from CSV file
//TODO num of metrics and epochs in pattern should be read and set globally for all tests, and not be predefined as of now

#include "tsdb_wrapper_api.h"
#include "seatest.h"
#include "tsdb_aux_tools.h"
#include <unistd.h>
#include <csv.h>

#define TSDB_DG_METRIC_NUM 6
#define TSDB_DG_EPOCHS_NUM 20
#define TSDB_DG_FINE_TS 2
#define MAX_PATH_LEN 50
#define COMMENT_CHAR '#'
#define NL_CHAR '\n'

typedef struct {
    int verbose_lvl;
    u_int8_t ronly;
    char *pfile;
} set_args;

static void help(int val) {

fprintf(stdout,"Use: test_tsdbwAPI [-vrh] pattern_file.csv\n");
fprintf(stdout,"This unit test checks TSDB wrapper API\n");
fprintf(stdout,"pattern_file contains write pattern in CSV format for tests and must be available for reading\n");
fprintf(stdout,"-r  enables read only mode. Use to omit TSDBs creation and proceed to reading\n");
fprintf(stdout,"-v  enables verbose mode\n");
fprintf(stdout,"-h  shows this help\n");
exit(val);
}

static void process_args(int argc, char **argv, set_args *args) {

  int c;
  args->verbose_lvl = 0;
  args->ronly = 0;

  while ((c = getopt(argc, argv,"vrh")) != -1) {
      switch (c) {
      case 'h':
          help(0);
          break;
      case 'v':
          args->verbose_lvl ++;
          break;
      case 'r':
          args->ronly = 1;
          break;
      default:
          help(1);
          break;
      }
  }

  if (optind < argc && optind + 1 == argc) {
      args->pfile = argv[optind];
      if (strlen(args->pfile) > MAX_PATH_LEN) {
          fprintf(stderr,"ERR: path provided is longer as %d symbols\n", MAX_PATH_LEN);
          exit(1);
      }
      if (!fexist(args->pfile)) {
          fprintf(stderr,"ERR: cannot read provided pattern file\n");
          exit(1);
      }
  } else help(1);

  if (args->verbose_lvl > 1) {
      help(1);
  }
}

int metrics_create(char ***metrics_arr) {

  char **metrics;
  u_int32_t cnt;

  metrics = (char **) malloc_darray(TSDB_DG_METRIC_NUM, MAX_METRIC_STRING_LEN, sizeof(char));
  if (!metrics) goto err_cleanup;

  for (cnt = 0; cnt < TSDB_DG_METRIC_NUM; ++cnt) {
      if (sprintf(metrics[cnt], "m-%lu", cnt +1) < 0) goto err_cleanup;
  }

  *metrics_arr = metrics;
  return 0;

  err_cleanup:
  free_darray(TSDB_DG_METRIC_NUM, (void **)metrics);
  *metrics_arr = NULL;
  return -1;
}

void metrics_destroy(char ***metrics_arr) {
  free_darray(TSDB_DG_METRIC_NUM,(void **) *metrics_arr);
  *metrics_arr = NULL;
}

int write_pattern_epoch(tsdbw_handle *db_bundle, int64_t **values, u_int8_t epoch_idx, u_int8_t metr_num, u_int8_t garbage_flag) {

  /* Writes data in the TSDB for the corresponding epoch.
   * values[metric][epoch_idx];
   * Effectively a column of values is written into TSDB
   * for the epoch indexed by epoch_idx */

  char **metrics; u_int8_t cnt;
  int rv;
  int64_t *vals_to_write;
  u_int16_t val_idx = 0;

  vals_to_write = (int64_t *) calloc(metr_num, sizeof(int64_t));

  if (metrics_create(&metrics)) {return -1; }

  for (cnt = 0; cnt < metr_num; ++cnt) {
      vals_to_write[val_idx] = values[cnt][epoch_idx];
      if (vals_to_write[val_idx] != 0) {
          if (garbage_flag) vals_to_write[val_idx] = 999; // garbage part, just some values bigger than those in the test
          rv = sprintf(metrics[val_idx], "m-%u", cnt + 1); assert_true(rv >= 0);
          val_idx++;
      }
  }

  rv = tsdbw_write(db_bundle, metrics, vals_to_write, val_idx);
  assert_true(rv == 0);

  metrics_destroy(&metrics);
  free(vals_to_write);

  return 0;
}

int parse_pattern(char **str_file, int64_t ***values_p) {
  /* **values_p are allocated here */
  /* **str_file is deallocated here after processing */ //TODO
  struct csv_parser p;
  csv_init(&p, 0);
  csv_free(&p);
  return 0;
}

void read_pattern(set_args *args, int64_t ***values_p) {
  FILE *pf;
  char mode = 'r';
  char *fline = NULL;
  char **str_file_o = NULL; //read file without comment and empty string lines
  char **str_file_n = NULL;
  size_t buf_size = MAX_PATH_LEN, metrics_num = 0;
  ssize_t char_read;

  if ((pf = fopen(args->pfile, &mode)) == NULL) {
      printf("Error opening file %s for reading\n", args->pfile);
      exit(1);
  }

  while (1) {
      if ((fline = (char *) malloc(MAX_PATH_LEN * sizeof(char))) == NULL) exit(4);
      char_read = getline(&fline, &buf_size, pf);
      if (buf_size > MAX_PATH_LEN) buf_size = MAX_PATH_LEN; // buf was increased due to long line
      if (char_read == 0) {free(fline); continue;}
      if (char_read <  0) {free(fline); break;}
      if (*fline == COMMENT_CHAR || *fline == NL_CHAR) {free(fline); continue;} //TODO use predefined NL char
      metrics_num++;
      str_file_n = (char **) realloc(str_file_o, metrics_num * sizeof(char *)); if (str_file_n == NULL) exit(5);
      str_file_o = str_file_n;
      str_file_o[metrics_num - 1] = fline;
  }

  if (!feof(pf)) {
      // We got an error state not because of EOF
      printf("Error while reading file %s\n",args->pfile);
      exit(2);
  }

  if ((fclose(pf)) == EOF) {
      printf("Error while closing file %s\n",args->pfile);
      exit(3);
  }

  if (parse_pattern(str_file_o, values_p) < 0) exit(6);
}

int create_pattern(int64_t ***values_p) {
  /* values[metric][epoch] */

  int64_t **values;
  u_int32_t i, j;

  /* Allocate memory */
  values = (int64_t **)calloc_darray(TSDB_DG_METRIC_NUM, TSDB_DG_EPOCHS_NUM, sizeof(int64_t));
  if (values == NULL) {*values_p = NULL; return -1;}

  /* Creating a writing template, 0 - no value to write */
  for (i = 1; i < TSDB_DG_METRIC_NUM + 1; ++i) { //epochs i = 1:6
      for (j = 1; j < i + 1; ++j ) { //metrics            j = 1:1, 1:2, ..., 1:6
          values[j-1][i-1] = 10 * j;
      }
  }

  values[3-1][2-1] = 30;
  values[3-1][16-1] = 30;
  values[1-1][17-1] = 10;
  values[6-1][18-1] = 60;
  values[4-1][19-1] = 40;
  values[2-1][20-1] = 20;

  *values_p = values;
  return 0;
}

int writing_cycle_engage(tsdbw_handle *db_bundle, int64_t **values, u_int32_t *epoch_glob_cnt) {

    u_int32_t epoch_cnt = 0, cur_epoch = time(NULL);
    u_int32_t last_epoch;

    normalize_epoch(db_bundle->db_hs[0], &cur_epoch);
    last_epoch = cur_epoch;

    while (epoch_cnt < TSDB_DG_EPOCHS_NUM) {

            epoch_cnt++; (*epoch_glob_cnt) ++;
            tfprintf(stdout,"Epoch %u. Writing... ", *epoch_glob_cnt);

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

u_int32_t emulate_outage(u_int32_t slp_time, u_int32_t ts, u_int32_t *ep_cnt) {

  u_int32_t time_left;
  printf("Emulating write process outage \n");

  for (;slp_time > 0; slp_time -= ts) {
      (*ep_cnt)++;
      tfprintf(stdout,"Epoch %u. Sleeping... ", *ep_cnt);
      time_left = (slp_time >= ts) ? sleep(ts) : sleep(ts - slp_time);
      if (time_left){
          printf("Sleep phase was not finished. %u seconds left. Aborting\n", time_left);
          fflush(stdout);
          return time_left;
      }
      printf("Done\n");
      if (slp_time < ts) break;
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
  req->epoch_to = db_bundle->db_hs[0]->epoch_list[db_bundle->db_hs[0]->number_of_epochs - 1] + db_bundle->db_hs[0]->slot_duration * 1.5;
  req->metrics_num = TSDB_DG_METRIC_NUM;

  char **metrics;
  if (metrics_create(&metrics) ) return -1;
  req->metrics = metrics;

  return 0;
}

int verify_reply_test1(tsdb_handler *h, q_reply_t *rep, u_int32_t *sleep_time) {

  /* form list of anticipated epochs */

  /* 3 from extra requested range:
   * 2 before the first one and 1 after the last one */
  int i, j, rv;
  u_int32_t ep_afront_num = 2, ep_trail_num = 1, ep_sleep = *sleep_time/ h->slot_duration;
  u_int32_t num_epochs = TSDB_DG_EPOCHS_NUM * 2 + ep_sleep + ep_afront_num + ep_trail_num; // == 48
  assert_true(num_epochs == rep->epochs_num_res);

  u_int32_t *epoch_list_an = (u_int32_t *) malloc(num_epochs * sizeof *epoch_list_an); //anticipated epochs

  /** Creating anticipated epochs **/
  /* First anticipated epoch */
  epoch_list_an[0] =  h->epoch_list[0] - ep_afront_num * h->slot_duration;

  /* Rest of them */
  for(i = 1; i < num_epochs; ++i) {
      epoch_list_an[i] = epoch_list_an[0]  + i * h->slot_duration;
  }

  /** Creating anticipated values **/
  int64_t **values_chunk_an;
  rv = create_pattern(& values_chunk_an); assert_true(rv == 0);
  int64_t **values_an = (int64_t **) malloc_darray(TSDB_DG_METRIC_NUM, num_epochs, sizeof(int64_t)); assert_true(values_an != NULL);

  /** Creating anticipated values **/
  /* First two epochs for all metrics must have default values
   * as they are not contained in the TSDB and were requested this
   * way deliberately */
  u_int32_t epochs_done = 0;
  for(i = 0; i <  ep_afront_num; ++i) {
      for(j = 0; j <  TSDB_DG_METRIC_NUM; ++j) {
          values_an[j][i] = h->unknown_value;
      }
  }
  epochs_done = i;
  assert_true(epochs_done == ep_afront_num);

  /* Then the pattern follows */
  for (i = 0; i < TSDB_DG_METRIC_NUM; ++i) {
      memcpy(&values_an[i][epochs_done], values_chunk_an[i], TSDB_DG_EPOCHS_NUM);
  }
  epochs_done += TSDB_DG_EPOCHS_NUM;
  assert_true(epochs_done == ep_afront_num + TSDB_DG_EPOCHS_NUM);

  /* Then the outage phase */
  for(i = 0; i <  ep_sleep; ++i) {
      for(j = 0; j <  TSDB_DG_METRIC_NUM; ++j) {
          values_an[j][epochs_done + i] = h->unknown_value;
      }
  }
  epochs_done += *sleep_time/ h->slot_duration;
  assert_true(epochs_done == ep_afront_num + TSDB_DG_EPOCHS_NUM + ep_sleep);

  /* Then the same pattern once more */
  for (i = 0; i < TSDB_DG_METRIC_NUM; ++i) {
      memcpy(&values_an[i][epochs_done], values_chunk_an[i], TSDB_DG_EPOCHS_NUM);
  }
  epochs_done += TSDB_DG_EPOCHS_NUM;
  assert_true(epochs_done == ep_afront_num + 2*TSDB_DG_EPOCHS_NUM + ep_sleep);
  free_darray(TSDB_DG_METRIC_NUM, (void **)values_chunk_an);

  /* Finally one trailing epoch with default data, as it does not exist in the TSDB */
  for (i = 0; i < TSDB_DG_METRIC_NUM; ++i) {
      values_an[i][epochs_done] = h->unknown_value;
  }
  epochs_done += ep_trail_num;
  assert_true(epochs_done == num_epochs);

  /** Comparing anticipated and actual results of the TSDB query **/
  for(i = 0; i <  TSDB_DG_METRIC_NUM; ++i) {
      for(j = 0; j <  num_epochs; ++j) {
          printf("Check. met %d, ep %d: val %d == %d\n", i+1, j+1, rep->tuples[i][j].value, values_an[i][j]);
          assert_true(rep->tuples[i][j].value == values_an[i][j]);
          assert_true(rep->tuples[i][j].epoch == epoch_list_an[j]);
      }
  }

  free(epoch_list_an);
  free_darray(TSDB_DG_METRIC_NUM, (void **)values_an);
  return 0;
}

void reply_data_destroy(q_reply_t *rep) {

  int i;
  for (i = 0; i < TSDB_DG_METRIC_NUM; ++i) free(rep->tuples[i]);
  free(rep->tuples);
}

int set_timers_norm (tsdbw_handle *db_bundle, time_t ctime) {

  u_int32_t ctime_mod = (u_int32_t) ctime;
  u_int32_t ctime_crs = (u_int32_t) ctime;
  assert_true(ctime_mod == ctime_crs); // epochs are aligned
  normalize_epoch(db_bundle->db_hs[TSDBW_MODERATE], &ctime_mod);
  normalize_epoch(db_bundle->db_hs[TSDBW_COARSE], &ctime_crs);
  db_bundle->mod_accum.last_flush_time = (time_t) ctime_mod;
  db_bundle->coarse_accum.last_flush_time = (time_t) ctime_crs;
  db_bundle->last_accum_update = ctime; // we can play around with it: it may point either to the current or prev epoch
  return 0;
}

void wait_alignment(tsdbw_handle *db_bundle) {

  printf("Waiting for current epochs of all open TSDBs to get aligned..."); fflush(stdout);
  while(!epochs_alligned(db_bundle));
  printf(" Done\n");
}

u_int8_t dbs_exist(const char **db_paths) {
  int i;
  for (i = 0; i < TSDBW_DB_NUM; ++i) {
      if (!fexist(db_paths[i])) return 0;
  }
  return 1;
}

void test_write(tsdbw_handle *db_bundle, const char **db_paths) {
  int rv;
  int64_t **values = NULL;
  u_int32_t sleep_time, epoch_num = 0, fine_timestep;

  /* Create test pattern of events */
  rv = create_pattern(&values); assert_true(rv == 0);

  /* Open DBs */
  rv = open_dbs(db_bundle, db_paths, 'w'); assert_true(rv == 0);

  fine_timestep = db_bundle->db_hs[TSDBW_FINE]->slot_duration;
  sleep_time = 2 * db_bundle->db_hs[TSDBW_COARSE]->slot_duration;; // 2 epochs of the coarse TSDB

  /* Writing epochs according to pattern once epochs in all TSDBs are aligned */
  wait_alignment(db_bundle);
  rv = set_timers_norm(db_bundle, time(NULL));  assert_true(rv == 0);
  rv = writing_cycle_engage(db_bundle, values, &epoch_num); assert_true(rv == 0);
  tsdbw_close(db_bundle); printf("DBs are closed\n");

  /* Emulate outage time for DB */
  rv = emulate_outage( sleep_time, fine_timestep, &epoch_num); assert_true(rv == 0);

  /* Open DBs in append mode this time*/
  rv = open_dbs(db_bundle, db_paths, 'a'); assert_true(rv == 0);

  /* Commence writing duty cycle */
  rv = writing_cycle_engage(db_bundle, values, &epoch_num); assert_true(rv == 0);
  tsdbw_close(db_bundle);
  free_darray(TSDB_DG_METRIC_NUM, (void **) values);

  /* Reporting the first and last epochs available in fine TSDB */
  rv = open_dbs(db_bundle, db_paths, 'a'); assert_true(rv == 0);
  char timestr[32];
  time2str(&db_bundle->db_hs[TSDBW_FINE]->epoch_list[0], timestr, 32);
  printf("Fine TSDB: first epoch: %s\n", timestr);
  time2str(&db_bundle->db_hs[TSDBW_FINE]->epoch_list[db_bundle->db_hs[TSDBW_FINE]->number_of_epochs - 1], timestr, 32);
  printf("Fine TSDB: last epoch: %s\n", timestr);
  tsdbw_close(db_bundle);

}

void test_read(tsdbw_handle *db_bundle, const char **db_paths) {
  int rv;
  u_int32_t sleep_time;

  /* Reading TSDBs */
  rv = open_dbs(db_bundle, db_paths, 'r'); assert_true(rv == 0);
  sleep_time = 2 * db_bundle->db_hs[2]->slot_duration;

  /* 1 Read whole fine TSDB (with extra range) and check correctness of data */
  q_request_t req;
  q_reply_t rep;
  rv = prepare_args_q_test1(db_bundle, &req); assert_true(rv == 0);
  rv = tsdbw_query(db_bundle, &req, &rep); assert_true(rv == 0);
  rv = verify_reply_test1(db_bundle->db_hs[0], &rep, &sleep_time); assert_true(rv == 0);

  reply_data_destroy(&rep);
  metrics_destroy(&req.metrics);


  /* 2. Read whole moderate TSDB (with extra range) and check correctness of consolidated data */
  /* 3. Read whole coarse TSDB (with extra range) and check correctness of consolidated data */
  /* Testing querying of epoch ranges using times encompassing them: */
  /* 4. Fine TSDB: randomly read epoch ranges/metrics and check correctness  */
  /* 5. Moderate TSDB: randomly read epoch ranges/metrics and check correctness  */
  /* 6. Coarse TSDB: randomly read epoch ranges/metrics and check correctness  */
}

int main(int argc, char *argv[]) {

  set_args args;
  process_args(argc, argv, &args);
  set_trace_level(args.verbose_lvl ? 99 : 0);

  tsdbw_handle db_bundle;
  const char *db_paths[3];
  db_paths[0] = "./DBs/f_db.tsdb";
  db_paths[1] = "./DBs/m_db.tsdb";
  db_paths[2] = "./DBs/c_db.tsdb";

  if (! dbs_exist(db_paths) && args.ronly == 1) return 2;
  if (args.ronly == 0) test_write(& db_bundle, db_paths);

  test_read(& db_bundle, db_paths);

  return 0;
}

