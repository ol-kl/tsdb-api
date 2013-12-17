/*
 * test_queryTime.c
 *
 *  Created on: Nov 12, 2013
 *      Author: Oleg Klyudt
 *
 * To compile this file, one needs libdb-dev and librrd-dev packages installed.
 */

#include "tsdb_api.h"
#include "seatest.h"
#include <sys/timex.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>

typedef struct {
    char *DB_file_name;
    u_int8_t populate;
    u_int8_t query;
    u_int8_t debug_lvl;
    u_int32_t seed;
} set_container;

#define BYTE(X) ((unsigned char *)(X))
#define PBYTE(X) ((unsigned char **)(X))
#define DEFAULT_VALUE 1000000
#define METRICS_NUM 1000000
#define STRING_MAX_LEN 20
#define TIME_STEP 60 //seconds
#define NUM_EPOCHS 60 //in time steps
#define RANDOM_FILL 0
#define CONTIGUOUS_FILL 1
#define FNAME ".TSDB_test_conf.bin"

static void help(int code) {
    printf("test-queryTime (-c DB_file_name | -q DB_file_name | -h ) [-s seed] \n");
    printf("-c creates a new DB file given by DB_file_name for subsequent test with the key -q\n");
    printf("-q performs query tests on the given DB DB_file_name and print profiling time they took\n");
    printf("-s to set a seed for a random generator. 1 by default. If it was set during the creation of DBs with the option -c, then the same seed value must be provided while performing profiling tests with the option -q\n");
    printf("-d to set a debug level in the range 0-99, where 99 is the most verbose and 0 for quiet mode. 0 by default.\n");
    printf("-h shows this brief help\n\n");
    printf("Usage: test-queryTime -c myDB.tsdb -s 50\n");
    printf("Then: test-queryTime -q myDB.tsdb -s 50\n");
    exit(code);
}

static void process_args(int argc, char *argv[], set_container *settings) {

  if (argc < 2 || argc > 7){
      help(1);
  }

  int c;
  settings->populate = 0;
  settings->query = 0;
  settings->seed = 1;
  settings->debug_lvl = 0;

  while ((c = getopt(argc, argv, "hc:q:s:d:")) != -1) {
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
      case 's':
        settings->seed = *optarg;
        break;
      case 'd':
        if (*optarg >= 0 && *optarg <= 99){
            settings->debug_lvl = *optarg;
        } else {
            fprintf(stdout,"debug level was wrong\n");
            help(1);
        }
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

float timeval2float (struct timeval *timestamp) {

  return (float)timestamp->tv_sec + (float)timestamp->tv_usec*1E-6;

}

void time2str(u_int32_t *tmr, char* str, size_t strsize) {
  time_t temp_buf = *tmr; //we need it because alignment of types u_int32_t and time_t may not coincide
  struct tm *tmr_struct = gmtime(&temp_buf);
  strftime(str,strsize,"%T %d-%m-%Y",tmr_struct);
}

void print_tsdb_info(tsdb_handler* handler) {
  char str[30];
  u_int32_t i;
  time2str(&handler->most_recent_epoch, str, 30);
  fprintf(stdout,"========== TSDB INFO ==========\n");
  fprintf(stdout,"num of columns: %d\n",handler->lowest_free_index);
  fprintf(stdout,"num of rows: %d\n",handler->number_of_epochs);
  fprintf(stdout,"most recent epoch: %s\n",str);
  fprintf(stdout,"time step between rows: %d s\n",handler->slot_duration);
  fprintf(stdout,"size of one value in TSDB: %d bytes\n",handler->values_len);
  fprintf(stdout,"=========== EPOCHS ============\n");
  for(i = 0; i< handler->number_of_epochs; ++i) {
      time2str(&(handler->epoch_list[i]), str, 30);
      fprintf(stdout,"%5d: %s (%ld)\n", i+1, str, handler->epoch_list[i]);
  }
  fprintf(stdout,"===============================\n");
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


int rrand(int m)
{
  return (int)((double)m * ( rand() / (RAND_MAX+1.0) ));
}


void shuffle(void *obj, size_t nmemb, size_t size)
{
  //Fisher–Yates shuffle algorithm
  void *temp = malloc(size);
  size_t n = nmemb;
  while ( n > 1 ) {
    size_t k = rrand(n--);
    memcpy(temp, BYTE(obj) + n*size, size);
    memcpy(BYTE(obj) + n*size, BYTE(obj) + k*size, size);
    memcpy(BYTE(obj) + k*size, temp, size);
  }
  free(temp);
}

int write_conf_file(void *buf, size_t buf_size) {
  int conf_file;
  ssize_t bytes_written = 0;

  conf_file = open(FNAME, O_WRONLY | O_CREAT | O_FSYNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (conf_file < 0) {
      fprintf(stdout,"Failed to write DB configuration file .TSDB_test_conf.txt. Tests on the DB querying will fail.\n");
      return -1;
  }

  while (buf_size != 0) {
      bytes_written = write(conf_file, &(BYTE(buf)[bytes_written]), buf_size);
      if (bytes_written < 0) {
          fprintf(stdout,"Failed to write DB configuration file .TSDB_test_conf.txt. Tests on the DB querying will fail.\n");
          return -1;
      }
      buf_size -= bytes_written;
  }

  if (close(conf_file) == -1) {
      fprintf(stdout,"Failed to close DB configuration file .TSDB_test_conf.txt. Tests on the DB querying may fail.\n");
      return -1;
  }
  return 0;
}

ssize_t read_conf_file(void **buf) {
  extern char *program_invocation_short_name;
  unsigned char *data;
  int conf_file;
  ssize_t bytes_read = 0, assumed_size = (NUM_EPOCHS ) * sizeof(u_int32_t);
  size_t actual_size = 0;

  data = BYTE(malloc(assumed_size + sizeof(u_int32_t))); // one extra element to check later for EOF

  if (data == NULL) {
      fprintf (stderr, "%s: Couldn't allocate memory to read config file\n",
                                        program_invocation_short_name);
      return -1;
  }

  conf_file = open(FNAME, O_RDONLY);

  if (conf_file < 0) {
      fprintf (stderr, "%s: Couldn't open conf file %s; %s\n",
                                  program_invocation_short_name, FNAME, strerror (errno));
      return -1;
  }

  do {
      bytes_read = read(conf_file, &(data[actual_size]), assumed_size);
      if (bytes_read < 0) {
          fprintf (stderr, "%s: Couldn't read conf file %s; %s\n",
                                      program_invocation_short_name, FNAME, strerror (errno));
          free(data);
          close(conf_file);
          return -1;
      }
      actual_size += bytes_read;
      assumed_size -= bytes_read;
  } while (assumed_size != 0);  // 0 == EOF

  // Check if the actual file size is larger than it should be
  bytes_read = read(conf_file, &(data[actual_size]),  sizeof(u_int32_t)); // read next one element
  if (bytes_read > 0) {
      fprintf (stderr, "%s: Read DB configuration does not match the current build. Tests on the DB querying will fail.\n",
                                  program_invocation_short_name);
      free(data);
      close(conf_file);
      return -1;
  }

  if (close(conf_file) == -1) {
      fprintf(stdout,"Failed to close DB configuration file .TSDB_test_conf.txt. Tests on the DB querying may fail.\n");
      return -1;
  }

  * PBYTE(buf) = data;

  //u_int32_t *test = (u_int32_t *)data;

  return actual_size;
}

void populate_DB(set_container* settings, u_int32_t* index, int mode){
    extern char *program_invocation_short_name;
    tsdb_handler db_handler;
    char metric[32], timeStr[30];
    int8_t rv;
    u_int16_t values_per_entry = 1;
    u_int32_t slot_duration = TIME_STEP; //seconds
    u_int8_t read_only = 0, time_noise = 0;
    u_int32_t cur_time = time(NULL), first_time = 0, checkTime;
    u_int32_t num_Epochs = NUM_EPOCHS; // number of Epochs in the TSDB (effectively rows, not taking into account splitting in chunks)
    u_int32_t num_Metrics = METRICS_NUM, missed_epochs = 0, j;
    u_int32_t *epoch_to_miss;
    tsdb_value i, transient_value;
    struct ntptimeval time_start, time_end;
    struct timeval diff;
    float result = 0;

    if (mode == RANDOM_FILL) {
        //make random permutations of the array of indices keeping the original array untouched
        u_int32_t *index_rand = (u_int32_t* ) malloc(sizeof(u_int32_t)*METRICS_NUM);
        memcpy(index_rand, index, sizeof(u_int32_t)*METRICS_NUM);

        shuffle(index_rand,METRICS_NUM,sizeof(u_int32_t));
        index = (u_int32_t*)index_rand;
    }

    epoch_to_miss = (u_int32_t*) malloc(NUM_EPOCHS * sizeof(u_int32_t));
    epoch_to_miss[0] = 0; // always write the first epoch
    epoch_to_miss[NUM_EPOCHS - 1] = 0; // and the last
    for(j = 1; j < NUM_EPOCHS - 1; ++j) {
        epoch_to_miss[j] = rand() % 2;
        if (epoch_to_miss[j]) missed_epochs++;
    }

    ensure_old_dbFile_is_gone(settings->DB_file_name);

    if(tsdb_open(settings->DB_file_name,&db_handler,&values_per_entry,slot_duration,read_only)) {
        fprintf (stderr, "%s: Couldn't create file %s; %s\n",
                            program_invocation_short_name, settings->DB_file_name, strerror (errno));
        exit(-1);
    }

    db_handler.unknown_value = 999;

    normalize_epoch(&db_handler,&cur_time);
    first_time += cur_time + TIME_STEP * (NUM_EPOCHS - 1);
    time2str(&first_time, timeStr, 30);
    fprintf(stdout,"Most recent epoch will be (check it!): %s (UTC)\n", timeStr);

    fprintf(stdout,"Start populating DB. Number of columns: %u, number of rows: %lu\n", num_Metrics, num_Epochs);

    if (mode == RANDOM_FILL) {
        fprintf(stdout,"Using random column indices to write, every write call is affected by uniform time noise\n");
    } else if (mode == CONTIGUOUS_FILL) {
        fprintf(stdout,"Using contiguous column indices to write, every write call is affected by uniform time noise\n");
    }

    for(j=0; j < num_Epochs; j++) {

        ntp_gettime(&time_start);

        if (j == 0) {
            for(i=0; i < num_Metrics; i++){
                rv = tsdb_goto_epoch(&db_handler, cur_time + j*slot_duration, 0, 1);
                assert_int_equal(0,rv);
                sprintf(metric,"metric-%u",i+1);
                rv = tsdb_set(&db_handler,metric,&i);
                assert_int_equal(0,rv);
            }
        } else {
            if (epoch_to_miss[j]) continue;
            for (i=0; i < num_Metrics; i++){
                transient_value = index[i]; //for type conversion
                time_noise = rand()%(TIME_STEP - 1);
                rv = tsdb_goto_epoch(&db_handler,cur_time + j*slot_duration + time_noise, 0, 1);
                assert_int_equal(0,rv);
                checkTime = cur_time + j*(time_t)slot_duration;
                normalize_epoch(&db_handler,&checkTime);
                assert_int_equal(db_handler.chunk.epoch, checkTime);
                sprintf(metric,"metric-%u", transient_value + 1);
                rv = tsdb_set(&db_handler, metric, &transient_value);
                assert_int_equal(0,rv);
            }
        }

        ntp_gettime(&time_end);
        timeval_subtract(&diff,&time_start.time,&time_end.time);
        result += timeval2float(&diff);
        fprintf(stdout,"Epoch written (attempts): %lu / %lu, for %.6f s\r", j+1, num_Epochs, timeval2float(&diff));
        rv=fflush(stdout);
        assert_int_equal(0,rv);
    }
    fprintf(stdout,"\n");

    /* Checking erroneous intention handling (going into non-existent past)*/
    fprintf(stdout,"Testing wrong epoch numbers addressing...");
    for (j = 1; j < NUM_EPOCHS - 1; ++j) {
        if (!epoch_to_miss[j]) continue;
        time_noise = rand()%(TIME_STEP - 1);
        rv = tsdb_goto_epoch(&db_handler,cur_time + j*slot_duration + time_noise, 0, 1);
        assert_int_equal(rv, -1);
    }
    fprintf(stdout," Done.\n");


    tsdb_flush(&db_handler);
    print_tsdb_info(&db_handler);

    tsdb_close(&db_handler);

    fprintf(stdout,"DB was populated and flushed\n");
    fprintf(stdout,"Avg time to write one row: %.6f s\n\n",result/(num_Epochs - missed_epochs));

    if (mode == RANDOM_FILL) {
        free(index);
    }

    /* Writing config file */
    write_conf_file(epoch_to_miss, NUM_EPOCHS * sizeof(*epoch_to_miss));

    free(epoch_to_miss);
}

void query_and_profile_DB(set_container* settings, u_int32_t* index){
  extern char *program_invocation_short_name;
  tsdb_handler db_handler;
  int8_t rv;
  u_int32_t j, i, *epoch_to_miss = NULL;
  ssize_t len;
  u_int16_t values_per_entry;
  float one_value_read=0, row_read=0;
  tsdb_value **interim_data, *returnedValue;
  struct ntptimeval time_start, time_end, time_start_long;
  struct timeval diff;

  len = read_conf_file((void **)&epoch_to_miss); //len in bytes

  if (len <= 0) {
      exit(-1);
  }

  //Though we dont need values_per_entry, but it must be provided to the function, otherwise segmentation_fault occurs
  if(tsdb_open(settings->DB_file_name,&db_handler,&values_per_entry,0,1)) {
      fprintf (stderr, "%s: Couldn't open file %s; %s\n",
                          program_invocation_short_name, settings->DB_file_name, strerror (errno));
      exit(-1);
  }

  interim_data = (tsdb_value**)malloc(sizeof(tsdb_value*));
  *interim_data = (tsdb_value*)malloc(db_handler.number_of_epochs*sizeof(tsdb_value));

  /* Test consistency of epochs: */
    /* 1. Correct epochs missed, calculations are correct */
    for(j=0; j < NUM_EPOCHS; j++) {
        rv = tsdb_goto_epoch(&db_handler, db_handler.most_recent_epoch - j*db_handler.slot_duration, 1, 0);
        if (!epoch_to_miss[NUM_EPOCHS - 1 - j]) {
            assert_int_equal(0,rv);
        } else {
            assert_int_equal(-1,rv);
        }
    }
    /* 2. List of epochs is correct, all epochs exist and are available */
    for(j=0; j < db_handler.number_of_epochs; j++) {
        rv = tsdb_goto_epoch(&db_handler, db_handler.epoch_list[j], 1, 0);
        assert_int_equal(0,rv);
    }

  /* Test performance of reading 1 column (with index METRICS_NUM/2) in the TSDB */
  ntp_gettime(&time_start_long);

  for(j=0; j < db_handler.number_of_epochs; j++) {
      rv = tsdb_goto_epoch(&db_handler, db_handler.epoch_list[j], 1, 0);
      assert_int_equal(0,rv);

      /* Let's check if the retrieved value is the one we have stored originally in the TSDB */
      rv=tsdb_get_by_index(&db_handler,&index[METRICS_NUM/2],&returnedValue);
      assert_int_equal(0,rv);
      (*interim_data)[j] = *returnedValue;
      assert_int_equal(index[METRICS_NUM/2],(*interim_data)[j]);
  }

  ntp_gettime(&time_end);
  tsdb_close(&db_handler);
  free(*interim_data);
  free(interim_data);

  timeval_subtract(&diff,&time_start_long.time,&time_end.time);
  fprintf(stdout,"We have read successfully 1 column in the TSDB. It took: %lu.%06lu s\n",diff.tv_sec,diff.tv_usec);

  /* Test performance of reading all columns in the TSDB */
  /* Testing separately contiguous and random access reading within a row
   * does not make much sense as the whole row gets loaded into memory,
   * so the only performance limitation between these two cases will be
   * accessing indices on different memory pages, which is negligible
   * compared to HDD read time */

  /* allocation on heap. */
  char **metrics_to_query;
  /* If static allocation, as
   * char metrics_to_query[METRICS_NUM][STRING_MAX_LEN];
   * then being put on stack the array addressing
   * will cause segmentation fault,
   * given the array size big enough */

  metrics_to_query = (char**)malloc(METRICS_NUM*sizeof(char*));
  for(i=0;i<METRICS_NUM;++i){
      metrics_to_query[i] = (char*)malloc(STRING_MAX_LEN*sizeof(char));
  }
  for(i=0;i<METRICS_NUM;++i){
      rv=sprintf(metrics_to_query[i],"metric-%u",i+1);
      assert_true(rv > 0);
  }

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

  ntp_gettime(&time_start_long);
  for(j=0; j < db_handler.number_of_epochs; j++) {
      rv = tsdb_goto_epoch(&db_handler, db_handler.epoch_list[j], 1, 0);
      assert_int_equal(0,rv);

      /* Profiling time to read a one random value within a row */
      ntp_gettime(&time_start); i=rand()%METRICS_NUM;
      rv=tsdb_get_by_index(&db_handler,&index[i],&returnedValue);
                assert_int_equal(0,rv);
                interim_data[i][j] = *returnedValue;
                assert_int_equal(index[i], interim_data[i][j]); //checking if the retrieved value is the one we have stored previously
      ntp_gettime(&time_end);
      timeval_subtract(&diff,&time_start.time,&time_end.time);
      one_value_read += timeval2float(&diff);

      /* Profiling time to read a whole row */
      ntp_gettime(&time_start);
      for(i=0;i < METRICS_NUM; ++i){
          rv=tsdb_get_by_index(&db_handler,&index[i],&returnedValue);
          assert_int_equal(0,rv);
          interim_data[i][j] = *returnedValue;
          assert_int_equal(index[i], interim_data[i][j]); //checking if the retrieved value is the one we have stored previously
      }
      ntp_gettime(&time_end);
      timeval_subtract(&diff,&time_start.time,&time_end.time);
      row_read += timeval2float(&diff);
      //fprintf(stdout,"Time to read %lu random values from the same epoch: %lu.%06lu s\n", METRICS_NUM, diff.tv_sec,diff.tv_usec); fflush(stdout);
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
  free(epoch_to_miss);
  //free(index);

  timeval_subtract(&diff,&time_start.time,&time_start_long.time);
  fprintf(stdout,"We have read successfully all %u columns in the TSDB. It took: %lu.%06lu s\n",METRICS_NUM,diff.tv_sec,diff.tv_usec);
  fprintf(stdout,"Avg time to read one random element in a row: %.6f\n", one_value_read / NUM_EPOCHS);
  fprintf(stdout,"Avg time to read a row: %.6f\n", row_read / NUM_EPOCHS);
}

int main(int argc, char *argv[]) {
  set_container settings;
  u_int32_t *index;
  u_int32_t i;

  process_args(argc, argv, &settings);

  set_trace_level(settings.debug_lvl);
  srand(settings.seed);

  index = (u_int32_t* ) malloc(sizeof(u_int32_t)*METRICS_NUM);

  for (i=0;i<METRICS_NUM;++i){
      index[i]=i;
  }

  if (settings.populate) {
      fprintf(stdout,"*** TEST 1 ***\n");
      populate_DB(&settings,index,CONTIGUOUS_FILL);
      fprintf(stdout,"*** TEST 2 ***\n");
      populate_DB(&settings,index,RANDOM_FILL);
  } else if (settings.query) {
      query_and_profile_DB(&settings,index);
  }
  else {
      free(index);
      return -1;
  }


  free(index);
  return 0;
}
