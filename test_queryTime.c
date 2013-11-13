/*
 * test_queryTime.c
 *
 *  Created on: Nov 12, 2013
 *      Author: Oleg Klyudt
 */

#include "tsdb_api.h"
#include "seatest.h"
#include <sys/timex.h>
#include <unistd.h>
#include <string.h>

typedef struct {
    char *DB_file_name;
    u_int8_t populate;
    u_int8_t query;
} set_container;

#define DEFAULT_VALUE 500
#define METRICS_NUM 1000000
#define STRING_MAX_LEN 20
#define TIME_STEP 60 //seconds
#define NUM_EPOCHS 60 //in time steps

static void help(int code) {
    printf("test-queryTime (-c DB_file_name | -q DB_file_name | -h) \n");
    printf("-c creates a new DB file given by DB_file_name for subsequent test with the key -q\n");
    printf("-q performs query tests on the given DB DB_file_name and print profiling time they took\n");
    printf("-h shows this brief help\n");
    exit(code);
}

static void process_args(int argc, char *argv[], set_container *settings) {

  if (argc < 2 || argc > 3){
      help(1);
  }

  int c;
  settings->populate = 0;
  settings->query = 0;

  while ((c = getopt(argc, argv, "hc:q:")) != -1) {
      switch (c) {
      case 'h':
        help(0);
        break;
      case 'c':
        settings->DB_file_name = optarg;
        settings->populate = 1;
        break;
      case 'q':
        settings->DB_file_name = optarg;
        settings->query = 1;
        break;
      default:
        help(1);
      }
  }
}

int timeval_subtract (result, x, y)
          struct timeval *result, *x, *y;
     {
  long int tmp;
  if (x->tv_sec == y->tv_sec){
      result->tv_sec = 0;
      tmp = x->tv_usec - y->tv_usec;
      if (tmp < 0) {
          tmp = -tmp;
      }
      result->tv_usec = tmp;
      }
  else if (x->tv_sec > y->tv_sec){
      tmp = x->tv_usec - y->tv_usec;
      if (tmp < 0) {
          result->tv_sec = x->tv_sec - y->tv_sec -1;
          result->tv_usec = 1000000 + tmp;
      }else {
          result->tv_sec = x->tv_sec - y->tv_sec;
          result->tv_usec = tmp;
      }
  }
  else {
      tmp = y->tv_usec - x->tv_usec;
      if (tmp < 0) {
          result->tv_sec = y->tv_sec - x->tv_sec -1;
          result->tv_usec = 1000000 + tmp;
      } else {
          result->tv_sec = y->tv_sec - x->tv_sec;
          result->tv_usec = tmp;
  }
     }
  return 0;
}


void ensure_old_dbFile_is_gone(const char* fname) {
  extern char *program_invocation_short_name;
  if (!strlen(fname)){
      fprintf (stderr, "%s: DB file name is empty\n",
                                        program_invocation_short_name);
      exit(-1);
  }
  if (access(fname,F_OK) == -1) { //can't access the file
      if (errno != ENOENT){ //error is not of a file non existence type
          fprintf (stderr, "%s: While trying to access the file %s, we got the error:%s\n",
                                            program_invocation_short_name, fname, strerror (errno));
          exit(-1);
      }
  } else {//file exists, remove it
      if (remove(fname)) {
          fprintf (stderr, "%s: Error while removing the file %s:%s\n",
                                            program_invocation_short_name, fname, strerror (errno));
          exit(-1);
      }
  }
}

void populate_DB(set_container* settings){
    extern char *program_invocation_short_name;
    tsdb_handler db_handler;
    char metric[32];
    int8_t rv;
    u_int16_t values_per_entry = 1;
    tsdb_value default_value = DEFAULT_VALUE;
    u_int32_t slot_duration = TIME_STEP; //seconds
    u_int8_t read_only = 0;
    time_t cur_time = time(NULL);
    u_int32_t num_Epochs = NUM_EPOCHS; // number of Epochs in the TSDB (effectively rows, not taking into account splitting in chunks)
    u_int32_t num_Metrics = METRICS_NUM, i, j;
    struct ntptimeval time_start, time_end;
    struct timeval diff;

    //const char settin = "test-queryTime.tsdb";

    ensure_old_dbFile_is_gone(settings->DB_file_name);

    if(tsdb_open(settings->DB_file_name,&db_handler,&values_per_entry,slot_duration,read_only)) {
        fprintf (stderr, "%s: Couldn't create file %s; %s\n",
                            program_invocation_short_name, settings->DB_file_name, strerror (errno));
        exit(-1);
    }

    fprintf(stdout,"Start populating DB. Number of columns: %u, number of rows: %lu\n",num_Metrics,num_Epochs);
    for(j=0; j < num_Epochs; j++) {
        ntp_gettime(&time_start);
        rv = tsdb_goto_epoch(&db_handler, (u_int32_t)(cur_time - j*(time_t)slot_duration), 0, 1);
        assert_int_equal(0,rv);

        if (j == 0) {
            for(i=0; i < num_Metrics; i++){
                sprintf(metric,"metric-%u",i+1);
                rv = tsdb_set(&db_handler,metric,&default_value);
                assert_int_equal(0,rv);
            }
        }else{
            //for(i=num_Metrics; i > 0; i--){
                sprintf(metric,"metric-%u",num_Metrics); //num_Metrics <-> i
                rv = tsdb_set(&db_handler,metric,&default_value);
                assert_int_equal(0,rv);
           // }
        }

        ntp_gettime(&time_end);
        timeval_subtract(&diff,&time_start.time,&time_end.time);
        fprintf(stdout,"Epoch written: %lu / %lu, time for one epoch: %lu.%lu s\n",j+1,num_Epochs,diff.tv_sec,diff.tv_usec);
        rv=fflush(stdout);
        assert_int_equal(0,rv);
    }
    fprintf(stdout,"\n");
    tsdb_close(&db_handler);
    fprintf(stdout,"DB was populated and flushed\n");
}

void query_and_profile_DB(set_container* settings){
  extern char *program_invocation_short_name;
  tsdb_handler db_handler;
  int8_t rv;
  u_int32_t *index, j, i;
  index = (u_int32_t*)malloc(METRICS_NUM*sizeof(u_int32_t));
  u_int16_t values_per_entry;
  tsdb_value **interim_data, *returnedValue;
  struct ntptimeval time_start, time_end;
  struct timeval diff;

  ntp_gettime(&time_start);
  //Though we dont need values_per_entry, but it must be provided to the function, otherwise segmentation_fault occurs
  if(tsdb_open(settings->DB_file_name,&db_handler,&values_per_entry,0,1)) {
      fprintf (stderr, "%s: Couldn't open file %s; %s\n",
                          program_invocation_short_name, settings->DB_file_name, strerror (errno));
      exit(-1);
  }

  interim_data = (tsdb_value**)malloc(sizeof(tsdb_value*));
  *interim_data = (tsdb_value*)malloc(db_handler.number_of_epochs*sizeof(tsdb_value));

  rv = tsdb_get_key_index(&db_handler,"metric-5",&index[0]);
  assert_int_equal(0,rv);

  for(j=0; j < db_handler.number_of_epochs; j++) {
      rv = tsdb_goto_epoch(&db_handler, (u_int32_t)(db_handler.most_recent_epoch - j*(time_t)db_handler.slot_duration), 1, 0);
      assert_int_equal(0,rv);

      rv=tsdb_get_by_index(&db_handler,&index[0],&returnedValue);
      assert_int_equal(0,rv);
      (*interim_data)[j] = *returnedValue;
      assert_int_equal(DEFAULT_VALUE,(*interim_data)[j]); //checking if the retrieved value is the one we have stored previously
  }

  ntp_gettime(&time_end);
  tsdb_close(&db_handler);
  free(*interim_data);
  free(interim_data);

  timeval_subtract(&diff,&time_start.time,&time_end.time);
  fprintf(stdout,"We have read successfully 1 column in the TSDB. It took: %lu.%lu s\n",diff.tv_sec,diff.tv_usec);

  /*****************NEW TEST*******************/
  //char metrics_to_query[METRICS_NUM][STRING_MAX_LEN];
  char **metrics_to_query;
  metrics_to_query = (char**)malloc(METRICS_NUM*sizeof(char*));
  for(i=0;i<METRICS_NUM;++i){
      metrics_to_query[i] = (char*)malloc(STRING_MAX_LEN*sizeof(char));
  }
  for(i=0;i<METRICS_NUM;++i){
      rv=sprintf(metrics_to_query[i],"metric-%u",i+1);
      assert_true(rv > 0);
  }

  ntp_gettime(&time_start);
    //Though we dont need values_per_entry, but it must be provided to the function, otherwise segmentation_fault occurs
  if(tsdb_open(settings->DB_file_name,&db_handler,&values_per_entry,0,1)) {
      fprintf (stderr, "%s: Couldn't open file %s; %s\n",
          program_invocation_short_name, settings->DB_file_name, strerror (errno));
      exit(-1);
  }

  interim_data = (tsdb_value**)malloc(sizeof(tsdb_value*)*METRICS_NUM);
  assert_true(interim_data != NULL);
  for(i=0;i<METRICS_NUM;++i){
      interim_data[i] = (tsdb_value*)malloc(db_handler.number_of_epochs*sizeof(tsdb_value));
      assert_true(interim_data[i] != NULL);
  }

  for(i=0;i < METRICS_NUM; ++i){
      rv = tsdb_get_key_index(&db_handler,metrics_to_query[i],&index[i]);
      assert_int_equal(0,rv);
  }

  for(j=0; j < db_handler.number_of_epochs; j++) {
      rv = tsdb_goto_epoch(&db_handler, (u_int32_t)(db_handler.most_recent_epoch - j*(time_t)db_handler.slot_duration), 1, 0);
      assert_int_equal(0,rv);

      for(i=0;i < METRICS_NUM; ++i){
          rv=tsdb_get_by_index(&db_handler,&index[i],&returnedValue);
          assert_int_equal(0,rv);
          interim_data[i][j] = *returnedValue;
          assert_int_equal(DEFAULT_VALUE, interim_data[i][j]); //checking if the retrieved value is the one we have stored previously
      }
  }

  ntp_gettime(&time_end);
  tsdb_close(&db_handler);

  for(i=0;i<METRICS_NUM;++i){
      free(interim_data[i]);
  }
  free(interim_data);
  for(i=0;i<METRICS_NUM;++i){
      free(metrics_to_query[i]);
  }
  free(metrics_to_query);
  free(index);

  timeval_subtract(&diff,&time_start.time,&time_end.time);
  fprintf(stdout,"We have read successfully %u columns in the TSDB. It took: %lu.%lu s\n",METRICS_NUM,diff.tv_sec,diff.tv_usec);
}

int main(int argc, char *argv[]) {
  set_container settings;
  set_trace_level(0);

  process_args(argc, argv, &settings);

  if (settings.populate) {
      populate_DB(&settings);
  } else if (settings.query) {
      query_and_profile_DB(&settings);
  }
  else {
      return -1;
  }

  return 0;
}
