/*
 * test_tsdbwAPI.c
 *
 *  Created on: Dec 5, 2013
 *      Author(s): Oleg Klyudt
 */

/* This routine has two purposes:
 * 1. Unit testing of TSDB Wrapper API
 * 2. After unit test completion it launches
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
#define LF_CHAR '\n'
#define CR_CHAR '\r'

typedef struct {
    int verbose_lvl;
    u_int8_t ronly;
    char *pfile;
} set_args;

typedef struct {
  size_t crow;
  size_t ccol;
  DArray *storage;
} csv_tracker;

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

int metrics_create(char ***metrics_arr, u_int32_t mnum) {

  char **metrics;
  u_int32_t cnt;

  metrics = (char **) malloc_darray(mnum, MAX_METRIC_STRING_LEN, sizeof(char));
  if (!metrics) goto err_cleanup;

  for (cnt = 0; cnt < mnum; ++cnt) {
      if (sprintf(metrics[cnt], "m-%u", cnt +1) < 0) goto err_cleanup;
  }

  *metrics_arr = metrics;
  return 0;

  err_cleanup:
  free_darray(mnum, (void **)metrics);
  *metrics_arr = NULL;
  return -1;
}

void metrics_destroy(char ***metrics_arr, u_int32_t mnum) {
  free_darray(mnum,(void **) *metrics_arr);
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

  if (metrics_create(&metrics, metr_num)) {return -1; }

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

  metrics_destroy(&metrics, metr_num);
  free(vals_to_write);

  return 0;
}

void print_pattern(DArray *p, char *str) {
  size_t i, j;
  if (p == NULL) exit(1);
  if (p->__data_allocated == 0) exit(2);
  printf("%s\n",str);

  for (i = 0; i < p->rown; ++i) {
      for (j = 0; j < p->coln; ++j) {
          printf("%2ld ", ((int64_t **)p->data)[i][j]);
      }
      printf("\n");
  }

}

void csv_cb_field(void *field, size_t field_size, void *ext_data) {

  long int pval;
  csv_tracker *arg = (csv_tracker *) ext_data;
  if (arg->ccol == 0) arg->storage->add_row(arg->storage, 1);
  arg->ccol ++;
  if (arg->ccol > arg->storage->coln) arg->storage->add_col(arg->storage, 1, 1);
  if (field != NULL) { //if field is not empty
      errno = 0;
      pval = strtol(field, NULL, 0);
      if (errno) printf("%d ERR: converting str into long integer", __LINE__);
      ((int64_t **)arg->storage->data)[arg->crow -1][arg->ccol -1] = pval;
  }
}

void csv_cb_record(int end_record_char, void *ext_data) {
  assert_true(end_record_char != CSV_CR && end_record_char != -1);
  csv_tracker *arg = (csv_tracker *) ext_data;
  arg->crow ++;
  arg->ccol = 0;
}

int pattern_consistent(DArray *p) {
  if (p->rown == 0) return 0;
  if (p->rown == 1) return 1;

  size_t i = p->rown - 1;
  for(i = p->rown - 1; i > 0; --i){
      if (((int64_t **)p->data)[0][i-1] >= ((int64_t **)p->data)[0][i]) break;
  }

  return i == 0;
}

DArray * inflate_pattern(DArray *p) {
  /* Newly allocated and inflated version of DArray is returned, source DArray *p is deallocated */
  if (p == NULL) return NULL;

  DArray *inf = new_darray(p->rown -1, ((int64_t **)p->data)[0][p->coln -1], p->__elem_size, p->__fill_val); // -1 row to remove epochs, as its number is clear from number of columns
  if (inf == NULL) return NULL;

  size_t ci, ri;
  for (ci = 0; ci < p->coln; ++ci) {
      for (ri = 0; ri < p->rown - 1; ++ri) {
          ((int64_t **)inf->data)[ri][((int64_t **)p->data)[0][ci] -1] = ((int64_t **)p->data)[ri+1][ci];
      }
  }
  p->destroy(p);
  return inf;
}

int parse_pattern(char *str_file, DArray **pattern) {
  /* *pattern is allocated externally */
  /* *str_file is deallocated externally */
  csv_tracker csvt;
  csvt.ccol = 0; // current column == 0 means the the current row must be allocated in the storage
  csvt.crow = 1; // current row
  csvt.storage = *pattern;

  struct csv_parser p;
  size_t rv;
  csv_init(&p, CSV_APPEND_NULL | CSV_EMPTY_IS_NULL);
  rv = csv_parse(&p, str_file, strlen(str_file), csv_cb_field, csv_cb_record, &csvt);
  csv_free(&p);
  if (rv != strlen(str_file)) {printf("ERR: csv_parse processed too few bytes\n"); return -1;}
  assert_true((*pattern)->coln == 11 && (*pattern)->rown == 7); // debug for current pattern
//  print_pattern(*pattern); //for debug only
  if (!pattern_consistent(*pattern)) {printf("ERR: pattern first row must have increasing numbers\n"); return -1;}
  *pattern = inflate_pattern(*pattern);
  print_pattern(*pattern, "Fine TSDB duty cycle pattern to write"); //for debug only
  return 0;
}

void read_fpattern(set_args *args, DArray **pattern) {
  FILE *pf = NULL;
  char mode = 'r';
  char *fline = NULL;
  char *str_file_o = NULL; //read file without comment and empty string lines
  char *str_file_n = NULL;
  size_t buf_size = MAX_PATH_LEN; //, metrics_num = 0;
  ssize_t char_read = 0;

  str_file_o = (char *) malloc(sizeof(char));
  if (str_file_o == NULL) exit(7);
  *str_file_o = '\0'; //empty string

  pf = fopen(args->pfile, &mode);
  if (pf == NULL) {
      printf("Error opening file %s for reading\n", args->pfile);
      exit(1);
  }

  /* Read off the file into one long string omitting comment strings (csv parser does not support comment strings)*/
  while (1) {
      if ((fline = (char *) malloc(MAX_PATH_LEN * sizeof(char))) == NULL) exit(4);
      char_read = getline(&fline, &buf_size, pf);
      if (buf_size > MAX_PATH_LEN) buf_size = MAX_PATH_LEN; // buf was increased due to long line
      if (char_read == 0) {free(fline); continue;}
      if (char_read <  0) {free(fline); break;}
      if (*fline == COMMENT_CHAR ||
          *fline == LF_CHAR ||
          *fline == CR_CHAR) {free(fline); continue;} //ignore a line it it start with # or \n or \r
      str_file_n = strapp(str_file_o, fline); if (str_file_n == NULL) exit(5);
      str_file_o = str_file_n;
      free(fline);
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

  if (parse_pattern(str_file_o, pattern) < 0) exit(6);
  free(str_file_o);
//  exit(0);
}

//int create_pattern(int64_t ***values_p) {
//  /* values[metric][epoch] */
//
//  int64_t **values;
//  u_int32_t i, j;
//
//  /* Allocate memory */
//  values = (int64_t **)calloc_darray(TSDB_DG_METRIC_NUM, TSDB_DG_EPOCHS_NUM, sizeof(int64_t));
//  if (values == NULL) {*values_p = NULL; return -1;}
//
//  /* Creating a writing template, 0 - no value to write */
//  for (i = 1; i < TSDB_DG_METRIC_NUM + 1; ++i) { //epochs i = 1:6
//      for (j = 1; j < i + 1; ++j ) { //metrics            j = 1:1, 1:2, ..., 1:6
//          values[j-1][i-1] = 10 * j;
//      }
//  }
//
//  values[3-1][2-1] = 30;
//  values[3-1][16-1] = 30;
//  values[1-1][17-1] = 10;
//  values[6-1][18-1] = 60;
//  values[4-1][19-1] = 40;
//  values[2-1][20-1] = 20;
//
//  *values_p = values;
//  return 0;
//}

int writing_cycle_engage(tsdbw_handle *db_bundle, DArray *vals, u_int32_t *epoch_glob_cnt) {
    u_int32_t epoch_cnt = 0, cur_epoch = time(NULL);
    u_int32_t last_epoch;

    normalize_epoch(db_bundle->db_hs[0], &cur_epoch);
    last_epoch = cur_epoch;

    while (epoch_cnt < vals->coln) {

            epoch_cnt++; (*epoch_glob_cnt) ++;
            tfprintf(stdout,"Epoch %u. Writing... ", *epoch_glob_cnt);

            if (write_pattern_epoch(db_bundle, (int64_t **)vals->data, epoch_cnt -1, vals->rown, 1)) {
                printf("Failed to write testing garbage\n");
                tsdbw_close(db_bundle);
                return -1;
            }

            if (write_pattern_epoch(db_bundle, (int64_t **)vals->data, epoch_cnt -1, vals->rown, 0)) {
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
    /* Loop ends at the beginning of vals->coln + 1 epoch*/

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

int prepare_args_q_test1(tsdbw_handle *db_bundle, q_request_t *req, u_int32_t mnum) {

  req->granularity_flag = TSDBW_FINE;
  req->epoch_from = db_bundle->db_hs[TSDBW_FINE]->epoch_list[0] - db_bundle->db_hs[TSDBW_FINE]->slot_duration * 1.5;
  req->epoch_to = db_bundle->db_hs[TSDBW_FINE]->epoch_list[db_bundle->db_hs[TSDBW_FINE]->number_of_epochs - 1] + db_bundle->db_hs[TSDBW_FINE]->slot_duration * 1.5;
  req->metrics_num = mnum;

  char **metrics;
  if (metrics_create(&metrics, mnum) ) return -1;
  req->metrics = metrics;

  return 0;
}

int prepare_args_q_test2(tsdbw_handle *db_bundle, q_request_t *req, u_int32_t mnum) {

  req->granularity_flag = TSDBW_MODERATE;
  req->epoch_from = db_bundle->db_hs[TSDBW_FINE]->epoch_list[0] - db_bundle->db_hs[TSDBW_FINE]->slot_duration * 1.5;
  req->epoch_to = db_bundle->db_hs[TSDBW_FINE]->epoch_list[db_bundle->db_hs[TSDBW_FINE]->number_of_epochs - 1] + db_bundle->db_hs[TSDBW_FINE]->slot_duration * 1.5;
  req->metrics_num = mnum;

  char **metrics;
  if (metrics_create(&metrics, mnum) ) return -1;
  req->metrics = metrics;

  return 0;
}

int prepare_args_q_test3(tsdbw_handle *db_bundle, q_request_t *req, u_int32_t mnum) {

  req->granularity_flag = TSDBW_COARSE;
  req->epoch_from = db_bundle->db_hs[TSDBW_FINE]->epoch_list[0] - db_bundle->db_hs[TSDBW_FINE]->slot_duration * 1.5;
  req->epoch_to = db_bundle->db_hs[TSDBW_FINE]->epoch_list[db_bundle->db_hs[TSDBW_FINE]->number_of_epochs - 1] + db_bundle->db_hs[TSDBW_FINE]->slot_duration * 1.5;
  req->metrics_num = mnum;

  char **metrics;
  if (metrics_create(&metrics, mnum) ) return -1;
  req->metrics = metrics;

  return 0;
}

void replicate_ftsdb(DArray *pattern, DArray *replica, u_int32_t ep_sleep) {
    int i, rv;
    /* pattern follows first */
    for (i = 0; i < pattern->coln; ++i) replica->app_col(replica, pattern->get_col(pattern, i), pattern->rown);
    assert_true(replica->coln == pattern->coln );

    /* Then the outage phase */
    rv = replica->add_col(replica, ep_sleep, 1);
    assert_true(rv == 0);
    assert_true(replica->coln ==  pattern->coln + ep_sleep);

    /* Then the same pattern once more */
    for (i = 0; i < pattern->coln; ++i) replica->app_col(replica, pattern->get_col(pattern, i), pattern->rown);
    assert_true(replica->coln == 2*pattern->coln + ep_sleep);
}

uint_fast8_t isarreq(void *arr, void *val, uint_fast8_t typelen, size_t elem_num) {

  size_t i;
  uint_fast8_t j;

  for (i = 0; i < elem_num; ++i) {
      for (j = 0; j < typelen; ++j) {
          if (((BYTE(arr + i * typelen))[j] ^ (BYTE(val))[j]) != 0) goto end;
      }
  }

end:
return i == elem_num && j == typelen;
}

DArray * consolidate(DArray *base, float cint, void *fillval){
  /* *base is array of values from the fine TSDB to consolidate. Columns are epochs, rows are metrics */
  /* cint is consolidation interval calculated as: epoch_length_curren_TSDB / epoch_length_fine_TSDB */
  /* *fillval is default filling values for the array of consolidated data*/

  size_t ci, mult;
  tsdb_value *col, cval = 0;

  tsdb_row_t accum;
  memset(&accum, 0, sizeof(accum));
  accum.data = calloc(base->rown, sizeof(tsdb_value));
  if (accum.data == NULL) return NULL;
  accum.size = base->rown;

  DArray *carr = new_darray(base->rown, 0, sizeof(int64_t), fillval);
  if (carr == NULL) {
      free(accum.data);
      return NULL;
  }

  for (ci = 0, mult = 1; ci < base->coln; ++ci) {

      col = (tsdb_value *)base->get_col(base, ci); assert_true(col != NULL);

      if (!isarreq(col, &cval, sizeof(tsdb_value), accum.size)) {
          consolidate_incrementally(col, &accum);
      }

      if ((ci + 1 >= (float) (cint * mult)) || // if a new epoch has come
          (ci + 1 == base->coln)) {             // or TSDB is going to get closed
          mult++;
          carr->app_col(carr, (void *)accum.data, base->rown);
          accum.cr_elapsed = 0;

          memset(accum.data, 0, base->rown * sizeof(tsdb_value));
      }
  }

  free(accum.data);

  assert_true(mult - 1 == carr->coln);
  return carr;
}


int32_t ep_num(tsdb_handler *h, u_int32_t from, u_int32_t to) {

  if (to < from || h == NULL) return -1;
  normalize_epoch(h, &from);
  normalize_epoch(h, &to);
  return (to - from) / h->slot_duration;
}

int verify_reply_test1(set_args *args, tsdb_handler *h, q_reply_t *rep,q_request_t *req, u_int32_t *sleep_time) {
  int i, j;
  DArray *pattern, *anvals;
  pattern = new_darray(0, 0, sizeof(h->unknown_value), &h->unknown_value);
  assert_true(pattern != NULL);
  read_fpattern(args, &pattern);

  u_int32_t ep_afront_num, ep_trail_num, ep_sleep_ftsdb = *sleep_time/ TSDB_DG_FINE_TS;
  ep_afront_num = ep_num(h, req->epoch_from, h->epoch_list[0]); // == 2
  ep_trail_num = ep_num(h, h->most_recent_epoch, req->epoch_to); // == 1
  assert_true(ep_afront_num >= 0 && ep_trail_num >= 0);

  u_int32_t num_epochs = pattern->coln * 2 + ep_sleep_ftsdb + ep_afront_num + ep_trail_num; // == 48
  assert_true(num_epochs == rep->epochs_num_res);

  u_int32_t *anepochs = (u_int32_t *) malloc(num_epochs * sizeof *anepochs); //anticipated epochs
  assert_true(anepochs != NULL);

  /** Creating anticipated epochs **/
  /* First anticipated epoch */
  anepochs[0] =  h->epoch_list[0] - ep_afront_num * h->slot_duration;

  /* Rest of them */
  for(i = 1; i < num_epochs; ++i) {
      anepochs[i] = anepochs[0]  + i * h->slot_duration;
  }

  /** Creating anticipated values **/
  anvals = new_darray(0, 0, sizeof(int64_t), (void *) h->unknown_value);assert_true(anvals != NULL);
  replicate_ftsdb(pattern, anvals, ep_sleep_ftsdb);

  anvals->add_col(anvals, ep_afront_num, 0);
  anvals->add_col(anvals, ep_trail_num, 1);

  printf("Test: correctness of fine TSDB data...\n");
  /** Comparing anticipated and actual results of the TSDB query **/
  for(i = 0; i <  anvals->rown; ++i) {
      for(j = 0; j <  anvals->coln; ++j) {
          if (rep->tuples[i][j].value != ((int64_t**)anvals->data)[i][j]) {
              printf("Metric value wrong. Met %d, epoch %d, val: available %ld, anticipated %ld\n", i+1, j+1, rep->tuples[i][j].value, ((int64_t**)anvals->data)[i][j]);
          }
          if (rep->tuples[i][j].epoch != anepochs[j]) {
              printf("Epoch is wrong. Available %d, anticipated %d\n", (u_int32_t)rep->tuples[i][j].epoch, anepochs[j]);

          }
          assert_true(rep->tuples[i][j].value == ((int64_t**)anvals->data)[i][j]);
          assert_true(rep->tuples[i][j].epoch == anepochs[j]);
      }
  }

  free(anepochs);
  anvals->destroy(anvals);
  pattern->destroy(pattern);
  printf("Done\n");
  return 0;
}

int verify_reply_test2(set_args *args, tsdb_handler *h, q_reply_t *rep, q_request_t *req, u_int32_t *sleep_time) {
  int i, j;
  DArray *pattern, *anvals, *ftsdb_replica;
  pattern = new_darray(0, 0, sizeof(h->unknown_value), &h->unknown_value);
  assert_true(pattern != NULL);
  read_fpattern(args, &pattern);

  printf("Test: correctness of moderate TSDB data...\n");
  u_int32_t ep_afront_num, ep_trail_num, ep_sleep_ftsdb = *sleep_time/ TSDB_DG_FINE_TS;
  ep_afront_num = ep_num(h, req->epoch_from, h->epoch_list[0]); // == 1
  ep_trail_num = ep_num(h, h->most_recent_epoch, req->epoch_to); // == 0
  assert_true(ep_afront_num >= 0 && ep_trail_num >= 0);

  /** Creating anticipated values **/
  ftsdb_replica = new_darray(0, 0, sizeof(int64_t), (void *) h->unknown_value);
  replicate_ftsdb(pattern, ftsdb_replica, ep_sleep_ftsdb);
  anvals = consolidate(ftsdb_replica, (float)h->slot_duration / (float)TSDB_DG_FINE_TS,(void *) h->unknown_value);
  assert_true(anvals != NULL);

  u_int32_t num_epochs = ep_afront_num + anvals->coln + ep_trail_num;// == ?
  assert_true(num_epochs == rep->epochs_num_res);

  anvals->add_col(anvals, ep_afront_num, 0);
  anvals->add_col(anvals, ep_trail_num, 1);
  print_pattern(anvals, "Anticipated values in moderate TSDB:");

  if (args->verbose_lvl > 0) {
      printf("Moderate TSDB contents:\n");
      for(i = 0; i <  anvals->rown; ++i) {
          for(j = 0; j <  anvals->coln; ++j) {
              printf("%2ld ",rep->tuples[i][j].value);
          }
          printf("\n");
      }
  }

  /** Creating anticipated epochs **/
  u_int32_t *anepochs = (u_int32_t *) malloc(num_epochs * sizeof *anepochs); //anticipated epochs
  assert_true(anepochs != NULL);

  /* First anticipated epoch */
  anepochs[0] =  h->epoch_list[0] - ep_afront_num * h->slot_duration;

  /* Rest of them */
  for(i = 1; i < num_epochs; ++i) {
      anepochs[i] = anepochs[0]  + i * h->slot_duration;
  }

  /** Comparing anticipated and actual results of the TSDB query **/
  for(i = 0; i <  anvals->rown; ++i) {
      for(j = 0; j <  anvals->coln; ++j) {
          if (rep->tuples[i][j].value != ((int64_t**)anvals->data)[i][j]) {
              printf("Metric value wrong. Met %d, epoch %d, val: available %ld, anticipated %ld\n", i+1, j+1, rep->tuples[i][j].value, ((int64_t**)anvals->data)[i][j]);
          }
          if (rep->tuples[i][j].epoch != anepochs[j]) {
              printf("Epoch is wrong. Available %d, anticipated %d\n", (u_int32_t)rep->tuples[i][j].epoch, anepochs[j]);

          }
          assert_true(rep->tuples[i][j].value == ((int64_t**)anvals->data)[i][j]);
          assert_true(rep->tuples[i][j].epoch == anepochs[j]);
      }
  }

  free(anepochs);
  pattern->destroy(pattern);
  anvals->destroy(anvals);
  ftsdb_replica->destroy(ftsdb_replica);
  printf("Done\n");

  return 0;
}

int verify_reply_test3(set_args *args, tsdb_handler *h, q_reply_t *rep, q_request_t *req, u_int32_t *sleep_time) {
  int i, j;
  DArray *pattern, *anvals, *ftsdb_replica;
  pattern = new_darray(0, 0, sizeof(h->unknown_value), &h->unknown_value);
  assert_true(pattern != NULL);
  read_fpattern(args, &pattern);

  printf("Test: correctness of coarse TSDB data...\n");
  u_int32_t ep_afront_num , ep_trail_num , ep_sleep_ftsdb = *sleep_time/ TSDB_DG_FINE_TS;
  ep_afront_num = ep_num(h, req->epoch_from, h->epoch_list[0]); // == 1
  ep_trail_num = ep_num(h, h->most_recent_epoch, req->epoch_to); // == 1
  assert_true(ep_afront_num >= 0 && ep_trail_num >= 0);

  /** Creating anticipated values **/
  ftsdb_replica = new_darray(0, 0, sizeof(int64_t), (void *) h->unknown_value);
  replicate_ftsdb(pattern, ftsdb_replica, ep_sleep_ftsdb);
  anvals = consolidate(ftsdb_replica, (float)h->slot_duration / (float)TSDB_DG_FINE_TS,(void *) h->unknown_value);
  assert_true(anvals != NULL);

  u_int32_t num_epochs = ep_afront_num + anvals->coln + ep_trail_num;// == 19
  assert_true(num_epochs == rep->epochs_num_res);

  anvals->add_col(anvals, ep_afront_num, 0);
  anvals->add_col(anvals, ep_trail_num, 1);
  print_pattern(anvals, "Anticipated values in coarse TSDB:");

  if (args->verbose_lvl > 0) {
      printf("Coarse TSDB contents:\n");
      for(i = 0; i <  anvals->rown; ++i) {
          for(j = 0; j <  anvals->coln; ++j) {
              printf("%2ld ",rep->tuples[i][j].value);
          }
          printf("\n");
      }
  }

  /** Creating anticipated epochs **/
  u_int32_t *anepochs = (u_int32_t *) malloc(num_epochs * sizeof *anepochs); //anticipated epochs
  assert_true(anepochs != NULL);
  /* First anticipated epoch */
  anepochs[0] =  h->epoch_list[0] - ep_afront_num * h->slot_duration;

  /* Rest of them */
  for(i = 1; i < num_epochs; ++i) {
      anepochs[i] = anepochs[0]  + i * h->slot_duration;
  }

  /** Comparing anticipated and actual results of the TSDB query **/
  for(i = 0; i <  anvals->rown; ++i) {
      for(j = 0; j <  anvals->coln; ++j) {
          if (rep->tuples[i][j].value != ((int64_t**)anvals->data)[i][j]) {
              printf("Metric value wrong. Met %d, epoch %d, val: available %ld, anticipated %ld\n", i+1, j+1, rep->tuples[i][j].value, ((int64_t**)anvals->data)[i][j]);
          }
          if (rep->tuples[i][j].epoch != anepochs[j]) {
              printf("Epoch is wrong. Available %d, anticipated %d\n", (u_int32_t)rep->tuples[i][j].epoch, anepochs[j]);

          }
          assert_true(rep->tuples[i][j].value == ((int64_t**)anvals->data)[i][j]);
          assert_true(rep->tuples[i][j].epoch == anepochs[j]);
      }
  }

  free(anepochs);
  pattern->destroy(pattern);
  anvals->destroy(anvals);
  ftsdb_replica->destroy(ftsdb_replica);
  printf("Done\n");

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

void print_epochs(tsdb_handler *h){
  char timestr[32];
  size_t i;
  printf("List of epochs (CTSDB):\n");

  for (i = 0; i < h->number_of_epochs; ++i) {
      time2str(&h->epoch_list[i], timestr, 32);
      printf("%s\n", timestr);
  }
}

void test_write(set_args *args, tsdbw_handle *db_bundle, const char **db_paths) {
  int rv;

  DArray *pattern;
  u_int32_t sleep_time, epoch_num = 0, fine_timestep;

  /* Open DBs */
  rv = open_dbs(db_bundle, db_paths, 'w'); assert_true(rv == 0);

  /* Create test pattern of events */
  //rv = create_pattern(&values); assert_true(rv == 0);
  pattern = new_darray(0, 0, sizeof(db_bundle->db_hs[TSDBW_FINE]->unknown_value), &db_bundle->db_hs[TSDBW_FINE]->unknown_value);
  assert_true(pattern != NULL);
  read_fpattern(args, &pattern);

  fine_timestep = db_bundle->db_hs[TSDBW_FINE]->slot_duration;
  sleep_time = 2 * db_bundle->db_hs[TSDBW_COARSE]->slot_duration;; // 2 epochs of the coarse TSDB

  /* Writing epochs according to pattern once epochs in all TSDBs are aligned */
  wait_alignment(db_bundle);
  rv = set_timers_norm(db_bundle, time(NULL));  assert_true(rv == 0);
  rv = writing_cycle_engage(db_bundle, pattern, &epoch_num); assert_true(rv == 0);
  tsdbw_close(db_bundle); printf("DBs are closed\n");

  /* Emulate outage time for DB */
  rv = emulate_outage( sleep_time, fine_timestep, &epoch_num); assert_true(rv == 0);

  /* Open DBs in append mode this time*/
  rv = open_dbs(db_bundle, db_paths, 'a'); assert_true(rv == 0);

  /* Commence writing duty cycle */
  rv = writing_cycle_engage(db_bundle, pattern, &epoch_num); assert_true(rv == 0);
  tsdbw_close(db_bundle);
  pattern->destroy(pattern);

  /* Reporting the first and last epochs available in fine TSDB */
  rv = open_dbs(db_bundle, db_paths, 'r'); assert_true(rv == 0);
  print_epochs(db_bundle->db_hs[TSDBW_COARSE]);
  tsdbw_close(db_bundle);

}

void test_read(set_args *args, tsdbw_handle *db_bundle, const char **db_paths) {
  int rv;
  u_int32_t sleep_time;

  /* Reading TSDBs */
  rv = open_dbs(db_bundle, db_paths, 'r'); assert_true(rv == 0);
  sleep_time = 2 * db_bundle->db_hs[TSDBW_COARSE]->slot_duration;


  q_request_t req;
  q_reply_t rep;
  u_int32_t metrics_num;
  DArray *pattern = new_darray(0, 0, sizeof(int64_t), &db_bundle->db_hs[TSDBW_FINE]->unknown_value);
  read_fpattern(args, &pattern);
  metrics_num = pattern->rown;
  pattern->destroy(pattern);

  /* 1 Read whole fine TSDB (with extra range) and check correctness of data */
  memset(&req, 0, sizeof(req));
  memset(&rep, 0, sizeof(rep));
  rv = prepare_args_q_test1(db_bundle, &req, metrics_num); assert_true(rv == 0);
  rv = tsdbw_query(db_bundle, &req, &rep); assert_true(rv == 0);
  rv = verify_reply_test1(args, db_bundle->db_hs[TSDBW_FINE], &rep, &req, &sleep_time); assert_true(rv == 0);

  reply_data_destroy(&rep);
  metrics_destroy(&req.metrics, metrics_num);


  /* 2. Read whole moderate TSDB (with extra range) and check correctness of consolidated data */
  memset(&req, 0, sizeof(req));
  memset(&rep, 0, sizeof(rep));
  rv = prepare_args_q_test2(db_bundle, &req, metrics_num); assert_true(rv == 0);
  rv = tsdbw_query(db_bundle, &req, &rep); assert_true(rv == 0);
  rv = verify_reply_test2(args, db_bundle->db_hs[TSDBW_MODERATE], &rep, &req, &sleep_time); assert_true(rv == 0);

  reply_data_destroy(&rep);
  metrics_destroy(&req.metrics, metrics_num);

  /* 3. Read whole coarse TSDB (with extra range) and check correctness of consolidated data */
  memset(&req, 0, sizeof(req));
  memset(&rep, 0, sizeof(rep));
  rv = prepare_args_q_test3(db_bundle, &req, metrics_num); assert_true(rv == 0);
  rv = tsdbw_query(db_bundle, &req, &rep); assert_true(rv == 0);
  rv = verify_reply_test3(args, db_bundle->db_hs[TSDBW_COARSE], &rep, &req, &sleep_time); assert_true(rv == 0);

  reply_data_destroy(&rep);
  metrics_destroy(&req.metrics, metrics_num);

  /* Testing querying of epoch ranges using times encompassing them: */
  /* 4. Fine TSDB: randomly read epoch ranges/metrics and check correctness  */
  /* 5. Moderate TSDB: randomly read epoch ranges/metrics and check correctness  */
  /* 6. Coarse TSDB: randomly read epoch ranges/metrics and check correctness  */
  tsdbw_close(db_bundle);
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
  if (args.ronly == 0) test_write(&args, & db_bundle, db_paths);

  test_read(&args, & db_bundle, db_paths);

  return 0;
}

