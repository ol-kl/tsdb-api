/*
 * tsdb_wrapper_api.h
 *
 *  Created on: Nov 25, 2013
 *      Author: Oleg Klyudt
 */

/* This API provides high level tailored functionality
 * to store values in the TSDB and retrieve them back.
 * Three DBs will be created with configured time step
 * between epochs, internal consolidation function will
 * take care of calculation and timely update of consolidated
 * data poitns in the respective DBs. Data points are expected
 * to be fed at the frequency corresponding to the finest time
 * step across all configured DBs. They will be saved in the
 * finest DB, others will be populated with consolidated values
 * by the internal consolidation function at the respective DBs'
 * intervals. */

#ifndef TSDB_WRAPPER_API_H_
#define TSDB_WRAPPER_API_H_

#include "tsdb_api.h"
#include "tsdb_aux_tools.h"
#include <time.h>

#define MAX_PATH_STRING_LEN 200
#define TSDBW_DB_NUM 3
#define TSDBW_MM 2               // medium DB time step multiplier
#define TSDBW_CM 2.5             // coarse DB time step multiplier
#define MAX_METRIC_STRING_LEN 27 // see set_key_index() in tsdb_api.c for details on why
#define TSDBW_UNKNOWN_VALUE 0

#define TSDBW_FINE 0
#define TSDBW_MODERATE 1
#define TSDBW_COARSE 2

#define TSDBW_MODE_READ 3
#define TSDBW_MODE_WRITE 4
#define TSDBW_MODE_APPEND 5

//#define TSDBW_UNKNOWN_VAL -1 //being assigned to an unsigned type we get max value of that type. It is the designation of an unknown value

typedef struct {
   char **list;
   size_t num_of_entries;
} metrics_t;

typedef struct {
  time_t epoch;
  int64_t value;
} data_tuple_t;

typedef struct {
  tsdb_value *data;
  size_t size; // of data
  u_int32_t cr_elapsed; //consolidation rounds elapsed on data
  metrics_t new_metrics; // emptied during each write cycle in a respective consolidated DB
  time_t last_flush_time; // last sync'ed epoch in the related consolidated TSDB as well
} tsdb_row_t;

typedef  struct {
  tsdb_row_t **rows; // it is a pointer to pointers to tsdbw_handle.mod_accum and tsdbw_handle.coarse_accum
  u_int8_t num_of_rows;
  time_t *last_accum_update; // pointer to tsdbw_handle.last_accum_update
} pointers_collection_t;

typedef struct {
  char mode;
  tsdb_handler **db_hs; // number of DBs is defined by TSDBW_DB_NUM
  tsdb_row_t mod_accum;
  tsdb_row_t coarse_accum;
  time_t last_accum_update; // using this time we can find out which epoch the consolidated data should be attributed to. Every fine TSDB sync -> data callback -> consolidation buffers updated incrementally -> this timer updated
  pointers_collection_t cb_communication;
} tsdbw_handle;

typedef struct {
  data_tuple_t **tuples;       // internally allocated and filled result array:[metrics_num][epochs_num]. Must be freed manually!
  u_int32_t epochs_num_res;    // number of epochs found within the window (epoch_from, epoch_to)
} q_reply_t;

typedef struct {
  time_t epoch_from;
  time_t epoch_to;
  char **metrics;         // array of strings, which are names of metrics
  u_int32_t metrics_num;        // number of metrics in "metrics" array
  char granularity_flag;        // the TSDB where search is to be done (fine, moderate, coarse)
} q_request_t;

int tsdbw_query(tsdbw_handle *db_set_h, // handle of all DBs, must be preallocated
                q_request_t *req,
                q_reply_t *rep);        // deallocation of rep->tuples has to be done explicitly


int tsdbw_write(tsdbw_handle *db_set_h,      // handle of all DBs, must be preallocated
                char **metrics,        // array of strings, which are names of metrics, length num_elem
                const int64_t *values,       // array of values for the metrics, length num_elem
                u_int32_t num_elem);         // number of metrics and respective values to write into TSDB

int tsdbw_init(tsdbw_handle *db_set_h,    // handle of all DBs, must be preallocated
               u_int16_t *finest_timestep,// num of seconds between entries in the finest TSDB.
                                          // time step for moderate TSDB: 5 * finest_timestep
                                          // for coarse one: 60 * finest_timestep
               const char **db_files,     // 3 strings with paths to DB files (fine, moderate, coarse) to write/create/append
               char io_flag);             // 'r' for read only,
                                          // 'w' for creating anew and reading/writing,
                                          // 'a' to open existing for reading/writing

void tsdbw_close(tsdbw_handle *handle);


#endif /* TSDB_WRAPPER_API_H_ */
