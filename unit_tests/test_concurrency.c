/*
 * test_concurrency.c
 * In general the test fails if:
 * 1. Some keys are missing in the DB
 * 2. Values related to keys are not correct
 * In both cases this is the result of conflicting concurrent access.
 *
 * With the current state of tsdb API, even if locks are enabled,
 * it will solve only the problem of missing keys, but not of missing values.
 *
 *  Created on: Jan 13, 2014
 *      Author: Oleg Klyudt
 */
#include "tsdb_api.h"
#include "seatest.h"
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/wait.h>
#define FNAME "test-conc-tsdb.tsdb"

typedef struct {
    int verbose_lvl;
} set_args;

static void help(int val) {

fprintf(stdout,"Use: test-concurrency [-v[v]]\n");
fprintf(stdout,"This unit test checks TSDB API for concurrent access to a DB\n");
fprintf(stdout,"Upon successful completion with the return code 0, the locking during write feature was implemented and works.\n");
fprintf(stdout,"Without options only test results are printed\n");
fprintf(stdout,"-v  enables verbose mode for the main process, \n   which will be checking DB consistency after concurrent writing cycle\n");
fprintf(stdout,"-vv enables verbose output for all processes this routine creates\n");
exit(val);

}

static void process_args(int argc, char **argv,set_args *args) {

  int c;
  args->verbose_lvl = 0;

  while ((c = getopt(argc, argv,"v")) != -1) {
      switch (c) {
      case 'h':
          help(0);
          break;
      case 'v':
          args->verbose_lvl ++;
          break;
      default:
          help(1);
          break;
      }
  }

  if (args->verbose_lvl >= 3) {
      help(1);
  }
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

static void init_trace(int verbose) {
    set_trace_level(verbose ? 99 : 0);
}

static void err_msg(char *msg, int id) {

  fprintf(stderr, "ID: %d, msg: %s\n", id, msg);
  exit(1);
}

static void write_key(char *key, int suffix) {

  int rv;
  if (rv = sprintf(key,"key-%d",suffix), rv <= 0)
      err_msg("failed to write key string",suffix);
}



int main(int argc, char *argv[]) {
  set_args args;
  process_args(argc, argv, &args);

  int rv, i;
  char db_file[20], report[20];
  u_int32_t timestep = 60; //seconds
  u_int8_t readonly = 0;
  u_int8_t conc_proc_num = 10;

  rv = sprintf(db_file, FNAME); assert_true(rv >= 0);

  if (args.verbose_lvl == 2) init_trace(99);

  ensure_old_dbFile_is_gone(db_file);

  for (i = 1; i < conc_proc_num; ++i){
      if (fork()) break;
  }
  fprintf(stdout, "PID %d started, num %d\n", getpid(), i); fflush(stdout);

  if (args.verbose_lvl == 1 && i == 1) init_trace(99);

  tsdb_handler h;
  u_int16_t val_per_entr = 1;

  //open DB
  if (rv = tsdb_open(db_file, &h, &val_per_entr, timestep , readonly), rv != 0)
    err_msg("couldnt open TSDB", i);

  //move to an epoch
  u_int32_t epoch = (u_int32_t) time(NULL);
  u_int8_t dont_create_epochs = 0, grow_fragments_in_chunk = 1;
  if (rv = tsdb_goto_epoch(&h,epoch, dont_create_epochs, grow_fragments_in_chunk), rv != 0)
    err_msg("failed to create epoch",i);

  //write DB within the set epoch
  char key[20];
  tsdb_value *val = (tsdb_value *) malloc(sizeof(tsdb_value));
  write_key(key, i);
  *val = (tsdb_value) i;
  if (rv = tsdb_set(&h, key, val), rv != 0)
    err_msg("failed to set value",i);
  free(val);

  //close DB
  tsdb_close(&h);

  fprintf(stdout, "PID: %d, num: %d, I'm done\n", getpid(), i);

  // wait for child processes to finish and reap them
  pid_t pid;
  while((pid = wait(NULL))) {
      if (pid == -1) {
          if (errno == ECHILD) break;
          else err_msg("Unknown error in wait()", 1);
      }
  }

  //check DB consistency across concurrent writes
  if (i == 1) { //only one process executes it
      readonly = 1;
      if (rv = tsdb_open(db_file, &h, &val_per_entr, timestep , readonly), rv != 0)
        err_msg("couldnt open TSDB", i);

      dont_create_epochs = 1; grow_fragments_in_chunk = 0;
      tsdb_goto_epoch(&h,epoch, dont_create_epochs, grow_fragments_in_chunk);

      for (i = 1; i < conc_proc_num + 1; ++i){
          write_key(key, i);
          rv = tsdb_get_by_key(&h, key, &val);

          if (rv != 0 ) {
              sprintf(report, "Key \"%s\" was not found in DB\n", key);
              err_msg(report, 1);
          }

          if (i != *val) {
              sprintf(report, "Value for key \"%s\" is wrong\n", key);
              err_msg(report, 1);
          }
      }

      tsdb_close(&h);
  }

  return 0;


}
