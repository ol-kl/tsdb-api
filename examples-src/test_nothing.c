/*
 * test_queryTime.c
 * This program is used for test and debugging purposes only
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
    char* DB_file_name;
    u_int8_t populate;
    u_int8_t query;
} set_container;

#define DEFAULT_VALUE 500
#define METRICS_NUM 1000000
#define STRING_MAX_LEN 20
#define TIME_STEP 60 //seconds
#define NUM_EPOCHS 60 //in time steps

void query_and_profile_DB(set_container* settings) {
  //extern char *program_invocation_short_name;
    tsdb_handler db_handler;
    int8_t rv;
    //u_int32_t index[METRICS_NUM], j, i;
    u_int16_t values_per_entry;
    tsdb_value **interim_data, *returnedValue;
    struct ntptimeval time_start, time_end;
    struct timeval diff;

    printf("test\n");


  /*****************NEW TEST*******************/
}

int main(int argc, char *argv[]) {

  set_container settings;
  query_and_profile_DB(&settings);

  return 0;
}
