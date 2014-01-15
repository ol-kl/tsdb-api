/*
 * tsdb_wrapper_api.c
 *
 *  Created on: Dec 5, 2013
     Author(s): Oleg Klyudt
 */

/* Things to implement optionally:
 * 1. Create a local variant of errno, a structure which will report an error type and provide extra info. So that unified error handling can be implemented.
 * 2. Consolidation function should be triggered on a schedule base, not by write requests as of now, as there can be no write requests at all during outage. Implement it as a separate process / thread.
 * 3. Support of values_per_entry > 1 by local functions. Some of them support it already. This however will require implementation of arithmetic for long types (more than int64_t).
 * 4. Syslogging / custom logging to file of all tracing info instead of stdout.
 * */

#include "tsdb_wrapper_api.h"

#define lambda(l_ret_type, l_arguments, l_body)         \
        ({                                                    \
         l_ret_type l_anonymous_functions_name l_arguments   \
         l_body                                            \
         &l_anonymous_functions_name;                        \
         })

static int _reportNewMetricCB(void *int_data, void *ext_data) {

  /* typeof int_data == char* */
  /* typeof ext_data == pointers_collection_t* */

  if (int_data == NULL || ext_data == NULL) {
      return -1;
  }
  /* Make a deep copy of the key for every accumulation buffer to make it persistent */
  pointers_collection_t *cb_pointers = (pointers_collection_t*) ext_data;
  size_t mtr_size = strlen((char *)int_data) + 1; // +1 to incorporate /0 character
  char *key_m = (char *) malloc( mtr_size ); if (key_m == NULL ) return -1;
  char *key_c = (char *) malloc( mtr_size ); if (key_c == NULL ) return -1;

  memcpy(key_m, (char *)int_data, mtr_size);
  memcpy(key_c, (char *)int_data, mtr_size);

  /* Add the key to the list of metrics with reallocation of the latter */
  char **intermediate_array;

  u_int32_t i;
  size_t numElems;
  for (i = 0; i < cb_pointers->num_of_rows; ++i) {
      numElems = cb_pointers->rows[i]->new_metrics.num_of_entries;

      /* Add a new key (metric) to every row */
      intermediate_array = (char**) realloc(cb_pointers->rows[i]->new_metrics.list,
          (numElems + 1) * sizeof(char*) );
      if (intermediate_array == NULL) {free(key_m); free(key_c); return -1;}
      intermediate_array[numElems] = (i == TSDBW_MODERATE - 1) ? key_m : key_c;
      cb_pointers->rows[i]->new_metrics.list = intermediate_array;
      cb_pointers->rows[i]->new_metrics.num_of_entries++;
      intermediate_array = NULL;
  }

  return 0;
}

static int consolidate_incrementally(tsdb_value *new_data, tsdb_row_t *row) {
  /* The algorithm currently does not support values,
   * which span several contiguous tsdb_values elements.
   * Hence it works correctly only values_per_entry = 1
   * foe TSDB DB. It is possible to implement the support
   * for larger values, however one would need to introduce
   * arithmetic for large integers not covered by any type. */

  /* MUST BE: lenof(new_data) == lenof(row->data) == row->size */
  /* Thus function implements incremental average algorithm.
   * Let S_n = (a_1 + a_2 + ... + a_n) / n be a partial sum for
   * a sequence a_1, a_2, a_3, ..., a_n, ... The sum is an average
   * in fact. Then S_(n+1) = S_n * n / (n+1) + a_(n+1)/(n+1)
   * represents an average as well. Proof is evident. */
  /* Here S_n is every element of row, whereas a_(n+1) is an element
   * of new_data array. */
  size_t i;
  u_int32_t n = row->cr_elapsed;

  for (i=0; i < row->size; ++i) {
      row->data[i] = (tsdb_value)((long double)((int64_t) row->data[i]) * (long double)n / (long double)(n + 1)
                     + (long double)((int64_t)new_data[i]) / (long double)(n+1));
  }

  row->cr_elapsed ++;
  return 0;
}

static int _reportChunkDataCB(void *int_data, void *ext_data) {
  /* Data in chunk and data in accumulation buffers get aligned
   * and a consolidation function is invoked upon them */

  /* typeof int_data == tsdb_handler* */
  /* typeof ext_data == pointers_collection_t* */

  if (int_data == NULL || ext_data == NULL) {
      return -1;
  }

  /* Definitions and type conversions */

  u_int8_t i;

  pointers_collection_t *rows_bundle = (pointers_collection_t*) ext_data;

  tsdb_value *r_data = (tsdb_value *) ((tsdb_handler *) int_data)->chunk.data; // reported data array
  tsdb_value *r_data_prepared = NULL;
  size_t tsdb_val_len = ((tsdb_handler *) int_data)->values_len; //size in bytes (i.e. chars)
  size_t r_data_size = ((tsdb_handler *) int_data)->chunk.data_len / tsdb_val_len;
  size_t unified_size = r_data_size;

  /* Find out max size across all rows and the chunk data to add */
  for (i = 0; i < rows_bundle->num_of_rows; ++i ) {
      if (unified_size < rows_bundle->rows[i]->size) unified_size = rows_bundle->rows[i]->size;
  }

  /* If size of initially provided data in chunk is smaller
   * than unified size, the data has to be reallocated to
   * be aligned in size */
  if (r_data_size != unified_size) {
      /* Align the data chunk to the new size*/
      r_data_prepared = (tsdb_value *) malloc(unified_size * tsdb_val_len);
      if (r_data_prepared == NULL) return -1;
      memset(r_data_prepared, ((tsdb_handler *) int_data)->unknown_value, unified_size * tsdb_val_len);

      /* fill it with the data passed to the callBack as r_data*/
      memcpy(r_data_prepared, r_data, r_data_size * tsdb_val_len);
  }

  for (i = 0; i < rows_bundle->num_of_rows; ++i ) {
      if (rows_bundle->rows[i]->size != unified_size) { // then rows_bundle->rows[i].size < unified_size
          /* Reallocate the row */
          tsdb_value *row_data_prepared = (tsdb_value *) realloc(rows_bundle->rows[i]->data, unified_size * tsdb_val_len);
          if (row_data_prepared == NULL) {
              free(r_data_prepared);
              return -1;
          }

          /* Fill the grown undefined portion of data with default value */
          memset(&row_data_prepared[rows_bundle->rows[i]->size],
                 ((tsdb_handler *) int_data)->unknown_value,
                 (unified_size - rows_bundle->rows[i]->size) * tsdb_val_len);
          rows_bundle->rows[i]->data = row_data_prepared;
          rows_bundle->rows[i]->size = unified_size;
      }

      /* Now data in chunk and data in accumulation arrays are prepared
       * and all the arrays are aligned in size. Now one can safely
       * perform consolidation */
      if (r_data_prepared == NULL) { //reported data chunk was not reallocated, as it is the largest one
          if (consolidate_incrementally(r_data, rows_bundle->rows[i])) return -1;
      } else {
          if (consolidate_incrementally(r_data_prepared, rows_bundle->rows[i])) {
              free(r_data_prepared);
              return -1;
          }
      }
      free(r_data_prepared);
      r_data_prepared = NULL;
  }

  *(rows_bundle->last_accum_update) = (time_t) ((tsdb_handler *) int_data)->chunk.epoch;

  return 0;
}



static int check_args_init(tsdbw_handle *handle, u_int16_t *finest_timestep,
    const char **db_files,
    char io_flag) {

  int idx;
  size_t s;

  if (db_files == NULL || handle == NULL) {
      trace_error("NULL ptr detected. Is array of DB files paths empty? DBs handle?");
      return -1;
  }

  for(idx=0; idx < TSDBW_DB_NUM; ++idx) {
      s = strlen(db_files[idx]);
      if (s == 0 || s > MAX_PATH_STRING_LEN ) {
          trace_error("Zero/too long string of a DB file path");
          return -1;
      }
  }

  switch (io_flag) {
  case 'r':
    handle->mode = TSDBW_MODE_READ;
    break;
  case 'a':
    handle->mode = TSDBW_MODE_APPEND;
    break;
  case 'w':
    handle->mode = TSDBW_MODE_WRITE;
    break;
  default:
    trace_error("Unknown mode flag");
    return -1;
  }

  return 0;
}


static void free_dbhs (tsdb_handler **h_dbs) {
  int i = 0;
  for (; i < TSDBW_DB_NUM; ++i) {
      free(h_dbs[i]); h_dbs[i] = NULL;
  }
  free(h_dbs); h_dbs = NULL;
}

static int open_DBs(tsdbw_handle *handle, u_int16_t *finest_timestep,
    const char **db_files,
    char io_flag) {

  int i, j;
  u_int16_t values_per_entry = 1;
  u_int16_t timesteps[] = {*finest_timestep,
                           *finest_timestep * TSDBW_MM,
                           *finest_timestep * TSDBW_CM};

  /* Allocate memory for DBs handles */
  tsdb_handler **h_dbs = (tsdb_handler **) calloc(TSDBW_DB_NUM, sizeof(tsdb_handler *));
  if (h_dbs == NULL) {
      trace_error("Failed to allocate memory for DB handles");
      return -1;
  }

  for (i=0; i < TSDBW_DB_NUM; ++i ) {
      h_dbs[i] = (tsdb_handler *) calloc(1, sizeof(tsdb_handler));
      if (h_dbs[i] == NULL) {
          trace_error("Failed to allocate memory for DB handles");
          free(h_dbs);
          return -1;
      }
  }

  /* Delete old DB files if WRITE mode was set */
  if (handle->mode == TSDBW_MODE_WRITE) {
      for (i=0; i < TSDBW_DB_NUM; ++i ) {
          if (fremove(db_files[i]) != 0) {
              trace_error("Could not remove old DB files. Mode - writing.");
              return -1;
          }
      }
      trace_info("Mode = writing. All old DBs were deleted.");
  }

  /* If Berkeley DBs are to be opened in environment (to enable locks)
   * then the following snippet might be of use
   *
   *
   *    typedef struct {
        ...
        DB *db;
        DB_ENV *db_env;
        } tsdb_handler;
   *
   * if ((ret = db_env_create(&handler->db_env, 0)) != 0) {
        trace_error("Error while creating DB_ENV handler [%s]", db_strerror(ret));
        return -1;
    }

    if ((ret = handler->db_env->set_shm_key(handler->db_env , 150)) != 0) {
      trace_error("Error while setting DB_ENV handler's shared memory key [%s]", db_strerror(ret));
      return -1;
    }

    if ((ret = handler->db_env->open(handler->db_env,
                          "/home/admin/Documents/tsdb-src-refactor",
                          DB_INIT_LOCK | DB_INIT_MPOOL | DB_SYSTEM_MEM | (read_only ? 0 : DB_CREATE),
                          mode)) != 0) {
        trace_error("Error while opening DB_ENV handler [%s]", db_strerror(ret));
        return -1;
    }

    if ((ret = db_create(&handler->db, handler->db_env, 0)) != 0) {
        trace_error("Error while creating DB handler [%s]", db_strerror(ret));
        return -1;
    }
   *
   * To close environment:
   *     handler->db_env->close(handler->db_env, 0);
   * */

  /* Open TSDBs */
  for (i=0; i < TSDBW_DB_NUM; ++i ) {
      if (tsdb_open(db_files[i],
                    h_dbs[i],
                    &values_per_entry,
                    timesteps[i],
                    (handle->mode == TSDBW_MODE_READ))) {

          //close already open DBs and remove files they were assigned to
          for (j = 0; j < i; ++j){
              tsdb_close(h_dbs[j]);
              fremove(db_files[j]);
          }
          //free allocated memory
          free_dbhs(h_dbs);
          return -1;
      } else {
          trace_info("DB %s opened.",db_files[i]);
      }
  }

  /* Make open TSDBs available through tsdbw handle */
  handle->db_hs = h_dbs;
  return 0;
}

static int init_structures_and_callbacks(tsdbw_handle *h) {

  int i;
  u_int32_t cur_time = (u_int32_t) time(NULL);
  u_int32_t cur_time_norm;

  /* Assigning initial values */
  for (i = 0; i < TSDBW_DB_NUM; ++i){
      h->db_hs[i]->unknown_value = TSDBW_UNKNOWN_VALUE;
  }

  h->mod_accum.data = NULL;
  h->mod_accum.size = 0;
  h->mod_accum.cr_elapsed = 0;
  h->mod_accum.new_metrics.list = NULL;
  h->mod_accum.new_metrics.num_of_entries = 0;
  if (h->db_hs[TSDBW_MODERATE]->most_recent_epoch == 0) {
      cur_time_norm = cur_time;
      normalize_epoch(h->db_hs[TSDBW_MODERATE], &cur_time_norm);
      h->mod_accum.last_flush_time =  (time_t) cur_time_norm;
  } else {
      h->mod_accum.last_flush_time =  (time_t) h->db_hs[TSDBW_MODERATE]->most_recent_epoch;
  }

  h->coarse_accum.data = NULL;
  h->coarse_accum.size = 0;
  h->coarse_accum.cr_elapsed = 0;
  h->coarse_accum.new_metrics.list = NULL;
  h->coarse_accum.new_metrics.num_of_entries = 0;
  if (h->db_hs[TSDBW_COARSE]->most_recent_epoch == 0) {
      cur_time_norm = cur_time;
      normalize_epoch(h->db_hs[TSDBW_COARSE], &cur_time_norm);
      h->coarse_accum.last_flush_time =  (time_t) cur_time_norm;
  } else {
      h->coarse_accum.last_flush_time =  (time_t) h->db_hs[TSDBW_COARSE]->most_recent_epoch;
  }

  h->last_accum_update = (time_t) cur_time;

  h->cb_communication.last_accum_update = &h->last_accum_update;
  h->cb_communication.num_of_rows = TSDBW_DB_NUM - 1; // assuming every but fine DB has its own accumulation buffer for incremental consolidation
  h->cb_communication.rows = (tsdb_row_t**) malloc(h->cb_communication.num_of_rows * sizeof(tsdb_row_t*));
                        if (h->cb_communication.rows == NULL) return -1;
  h->cb_communication.rows[0] = &h->mod_accum;
  h->cb_communication.rows[1] = &h->coarse_accum;

  /* Defining callbacks for the finest TSDB.
   * For other TSDBs these have NULL values
   * and will be ignored within the original TSDB API */
  h->db_hs[0]->reportNewMetricCB.external_data = & h->cb_communication;
  h->db_hs[0]->reportChunkDataCB.external_data = & h->cb_communication;
  h->db_hs[0]->reportNewMetricCB.cb = _reportNewMetricCB;
  h->db_hs[0]->reportChunkDataCB.cb = _reportChunkDataCB;

  return 0;
}

static int consolidation_start(tsdbw_handle *h) {
  /* Here consolidation routine should either
   * be forked or started in a separate thread.
   * This however requires IPC/mutex implementation
   * and opening DBs with THREAD flag to make
   * handles thread-free, i.e., eligible to use
   * across threads  */

  /* Currently consolidation is being done during
   * DBs writing process*/
  return 0;
}

int tsdbw_init(tsdbw_handle *h, u_int16_t *finest_timestep,
               const char **db_files,
               char io_flag) {

  /* Cautious memory cleaning */
  memset(h, 0, sizeof(tsdbw_handle));

  /* Sanity checks and mode setting*/
  // h->mode is set by check_args_init
  if (check_args_init(h, finest_timestep, db_files, io_flag) != 0) return -1;

  /* Open the given TSDBs*/
  //h->db_hs is set by open_DBs()
  if (open_DBs(h, finest_timestep, db_files, io_flag) != 0) return -1;

  /* Assigning initial values */
  if (init_structures_and_callbacks(h)) return -1;

  /* Start consolidation daemon */
  if (consolidation_start(h) != 0) return -1;

  return 0;
}

static int tsdbw_consolidated_flush(tsdb_handler *tsdb_h, tsdb_row_t *accum_buf, time_t last_update_time ) {
  //TODO: add flag for strict writing error handling

  u_int32_t i, j, start_idx;
  u_int8_t nvpe = tsdb_h->values_per_entry; //number of values per entry
  u_int8_t err_flag = 0, err_if_epoch_missing = 0, allowed_to_grow_epochs = 1;
  u_int32_t epoch_to_write = last_update_time;
  normalize_epoch(tsdb_h, &epoch_to_write);

  u_int32_t epoch_current = (u_int32_t) time(NULL);
  normalize_epoch(tsdb_h, &epoch_current);

  /* If no data to flush */
  if (accum_buf->size == 0) {
      if (accum_buf->new_metrics.num_of_entries != 0) return -1; //error in logic
      accum_buf->last_flush_time = (time_t) epoch_current;
      return 0;
  }

  tsdb_goto_epoch(tsdb_h, epoch_to_write, err_if_epoch_missing, allowed_to_grow_epochs);

  if (epoch_to_write + tsdb_h->slot_duration < epoch_current && epoch_to_write != 0) { //they are equal if no epochs were missed
      /* some consolidation epochs were missed (spent as outage),
       *  i.e., not written. We may want to do smth about it here */

      /* We have a choice - either fill the missed epochs
       * in consolidated DBs with default values
       * meaning absence of data, or we can just omit their writing at all.
       * As the TSDBs have a list of epochs internally, one can detect
       * missing epochs in DB based on this list and slot_duration time
       * and decide what to return for the values from these epochs.
       * We favor the latter option.*/
      char str_beg[20], str_end[20];
      time2str(&epoch_to_write, str_beg, 20);
      time2str(&epoch_current, str_end, 20);
      trace_warning("Missing epochs detected in a consolidated DB. Time step %u. Interval: %s -- %s", tsdb_h->slot_duration, str_beg, str_end);
  }

  //if (tsdb_h->lowest_free_index !=  accum_buf->size - accum_buf->new_metrics.num_of_entries) {
      /* This IF checks for absence of gaps in metrics. We dont want to end up in
       * a situation where not all data columns have associated metrics (names). This
       * will render us being unable to query these columns */
      // tsdb_h->lowest_free_index is number of metrics in the TSDB at the time of a last written epoch (N.B. lowest_free_index counts since 0)
      // accum_buf->size is the size of an array containing data, is a multiple of chunk size
      // accum_buf->new_metrics.num_of_entries is the number of NEW metrics which are to be
      // added to the current TSDB
  //    trace_error("Not enough metric names for provided data to write in a consolidated DB. Nothing will be written.");
  //    return -1;
  //}

  /* Firstly we write values for already existing metrics in the consolidated DB.
   * Hence we address metrics by index rather than name. It is possible only
   * due to monotonic allocation of column indices to new metrics, so that
   * new metrics are appended always at the very end one by one */

  size_t data_entries_num = tsdb_h->lowest_free_index > accum_buf->size ? accum_buf->size : tsdb_h->lowest_free_index;
  for (i = 0; i < data_entries_num; ++i) {
      if (tsdb_set_by_index(tsdb_h, &accum_buf->data[i * nvpe], &i)) {
          trace_error("Failed to write a value in consolidated TSDB. New metrics were not being added and the DB consistency is intact.");
          break;
      }
  }

  /* Now we write new metrics and respective values in the consolidated DB.
   * We use regular tsdb_set() to create the mappings metric-column index internally. */
  if (accum_buf->size > tsdb_h->lowest_free_index) {
      start_idx = tsdb_h->lowest_free_index;
      for (i = 0; i < accum_buf->new_metrics.num_of_entries; ++i) {
          if (tsdb_set(tsdb_h, accum_buf->new_metrics.list[i], &accum_buf->data[(start_idx + i) * nvpe])) {
              err_flag = 1;
              trace_error("Failed to write a value in consolidated TSDB. New metrics were being written, attempting to recover for the next flush.");
              /* Attempt of recovery: all values get nullified in the accum buffer,
               * its size is preserved, unwritten metrics are preserved. So that they can
               * be written upon next flushing */
              memset(accum_buf->data, 0, accum_buf->size * tsdb_h->values_len); // we deliberately nullify it and not setting it to an undefined value, because arithmetic operations in the consolidation function are undefined in general for an undefined value

              /* by setting "accum_buf->cr_elapsed = 0;" at the end of the function
               * we effectively cancel the difference for _reportChunkDataCB
               * between unallocated accum_buf->data and
               * allocated and filled with zeros. Hence
               * the consolidated values (after consolidation function
               * passage over accum_buf->data) will not be biased */
              char **saved_metrics = (char **) malloc((accum_buf->new_metrics.num_of_entries - i) * sizeof(char*));
              for (j = 0; j < accum_buf->new_metrics.num_of_entries - i; ++j) {
                  saved_metrics[j] = accum_buf->new_metrics.list[i + j]; //copying pointers to unwritten metrics
              }
              for (j = 0; j < i; ++j) {
                  free(accum_buf->new_metrics.list[j]); //freeing successfully written metrics
              }
              free(accum_buf->new_metrics.list);
              accum_buf->new_metrics.list = saved_metrics;
              accum_buf->new_metrics.num_of_entries = accum_buf->new_metrics.num_of_entries - i;

              trace_info("Recovery of unwritten metrics succeeded");
              break;
          }
      }
  }


  if (!err_flag) {
      free(accum_buf->data); // allocated within data callback
      accum_buf->data = NULL; // MUST BE NULL, so that realloc in a callback can allocate memory anew as malloc
      accum_buf->size = 0;
      for (j = 0; j < accum_buf->new_metrics.num_of_entries; ++j) { //accum_buf->new_metrics.num_of_entries is intact only if no errors happened
          free(accum_buf->new_metrics.list[j]);
      }
      free(accum_buf->new_metrics.list); // allocated within metric callback
      accum_buf->new_metrics.list = NULL;
      accum_buf->new_metrics.num_of_entries = 0;
  }

  accum_buf->cr_elapsed = 0;
  accum_buf->last_flush_time = (time_t) epoch_current;

  tsdb_flush(tsdb_h);

  return 0;
}

void tsdbw_close(tsdbw_handle *handle) {

  u_int32_t i;

  /* Write consolidated data into respective DBs  */
  for (i = 1; i < TSDBW_DB_NUM; ++i) { // the finest DB will be flushed automatically when closed
      if (tsdbw_consolidated_flush(handle->db_hs[i], handle->cb_communication.rows[i-1], handle->last_accum_update)) {
          trace_error("Could not flush %u th consolidated DB", i);
      }
  }

  /* Close DBs */
  for (i = 0; i < TSDBW_DB_NUM; ++i ) {
      tsdb_close(handle->db_hs[i]);
  }

  /* Release memory allocated for those DBs*/
  free_dbhs(handle->db_hs);

  /* Release memory allocated for accums */
  if (handle->mod_accum.data != NULL) {
      free(handle->mod_accum.data);
      handle->mod_accum.size = 0;
      handle->mod_accum.data = NULL;
  }
  if (handle->coarse_accum.data != NULL) {
      free(handle->coarse_accum.data);
      handle->coarse_accum.size = 0;
      handle->coarse_accum.data = NULL;
  }
  if (handle->mod_accum.new_metrics.list != NULL) {
      for (i = 0; i < handle->mod_accum.new_metrics.num_of_entries; ++i) {
          free(handle->mod_accum.new_metrics.list[i]);
      }
      free(handle->mod_accum.new_metrics.list);
      handle->mod_accum.new_metrics.num_of_entries = 0;
      handle->mod_accum.new_metrics.list = NULL;
  }
  if (handle->coarse_accum.new_metrics.list != NULL) {
      for (i = 0; i < handle->coarse_accum.new_metrics.num_of_entries; ++i) {
          free(handle->coarse_accum.new_metrics.list[i]);
      }
      free(handle->coarse_accum.new_metrics.list);
      handle->coarse_accum.new_metrics.num_of_entries = 0;
      handle->coarse_accum.new_metrics.list = NULL;
  }
  if (handle->cb_communication.rows != NULL) {
      free(handle->cb_communication.rows);
      handle->cb_communication.num_of_rows = 0;
      handle->cb_communication.rows = NULL;
  }

}

static int check_args_write(tsdbw_handle *db_set_h, char **metrics, const int64_t *values, u_int32_t num_elem) {

  int i;

  if (db_set_h == NULL) {
      trace_error("DBs handle not allocated");
      return -1;
  }

  for (i = 0; i < TSDBW_DB_NUM; ++i) {
      if (db_set_h->db_hs[i] == NULL) {
          trace_error("DBs handle not allocated");
          return -1;
      }
      if (! db_set_h->db_hs[i]->alive) {
          trace_error("DB is not alive (closed?)");
          return -1;
      }
  }

  if (metrics == NULL) {
        trace_error("Array of metric names is empty");
        return -1;
    }

  if (values == NULL) {
        trace_error("Array of values for writing is empty");
        return -1;
    }

  for (i = 0; i < num_elem; ++i) {

      if (metrics[i] == NULL) {
          trace_error("Trying to address NULL pointer");
          return -1;
      }

      if (strlen(metrics[i]) > MAX_METRIC_STRING_LEN ) {
          trace_error("Maximum allowed string length for a matric name is exceeded");
          return -1;
      }

  }

  return 0;
}

static int fine_tsdb_update(tsdbw_handle *db_set_h,
    /* This function does not support currently values_per_entry > 1*/
    char **metrics,
    const int64_t *values,
    u_int32_t num_elem) {

  int rv;
  tsdb_value *buf = (tsdb_value *) calloc(num_elem, db_set_h->db_hs[0]->values_len);
  if (buf == NULL) {
      trace_error("Failed to allocate memory");
      return -1;
  }

#if defined __GNUC__

  /* This hack works only with GCC. The function is unpacked for other compilers. */
  rv = lambda(int,
          (tsdb_value *buf,
          tsdbw_handle *db_set_h,
          char **metrics,
          const int64_t *values,
          u_int32_t num_elem),
          {
              int i;
              int fail_if_missing = 0;
              int is_growable = 1;
              u_int32_t cur_time = (u_int32_t) time(NULL);

              /* Converting values into the proper type for TSDB */
              for (i = 0; i < num_elem; ++i) {
                  buf[i] = (tsdb_value) values[i];
              }

              for (i = 0; i < num_elem; ++i) {

                  if (strlen(metrics[i]) == 0) continue; //skip empty metric

                  if (tsdb_goto_epoch(db_set_h->db_hs[0], cur_time, fail_if_missing, is_growable)) {
                      trace_error("Failed to advance to a new epoch");
                      return -1;
                  }
                  if (tsdb_set(db_set_h->db_hs[0], metrics[i], &buf[i])) {
                      trace_warning("Failed to set value in a TSDB. ");
                      /* An entry in TSDB with an unset value will preserve its initially
                       * set one by default (which can be adjusted on per TSDB basis)  */
                  }
              }
              return 0;
          })(buf, db_set_h, metrics, values, num_elem );
#else
  int i;
  int fail_if_missing = 0;
  int is_growable = 1;
  u_int32_t cur_time = (u_int32_t) time(NULL);

  /* Converting values into the proper type for TSDB */
  for (i = 0; i < num_elem; ++i) {
      buf[i] = (tsdb_value) values[i];
  }

  for (i = 0; i < num_elem; ++i) {

      if (strlen(metrics[i]) == 0) continue; //skip empty metric

      if (tsdb_goto_epoch(db_set_h->db_hs[0], cur_time, fail_if_missing, is_growable)) {
          trace_error("Failed to advance to a new epoch");
          free(buf);
          return -1;
      }
      if (tsdb_set(db_set_h->db_hs[0], metrics[i], &buf[i])) {
          trace_warning("Failed to set value in a TSDB. ");
          /* An entry in TSDB with an unset value will preserve its initially
           * set one by default (which can be adjusted on per TSDB basis)  */
      }
  }
  rv = 0;
#endif

  free(buf);
  return rv;
}



int tsdbw_write(tsdbw_handle *db_set_h,
                char **metrics,
                const int64_t *values,
                u_int32_t num_elem) {

  int i;
  char report_str[20];
  if (db_set_h->mode == TSDBW_MODE_READ) return -1;

  /* Sanity checks */
  if (check_args_write(db_set_h, metrics, values, num_elem) != 0) return -1;

  /* Updating the fine TSDB with values for metrics*/
  if (fine_tsdb_update(db_set_h, metrics, values, num_elem) != 0) return -1;

  /* Flushing arrays of consolidated data, if we step over an epoch */
  time_t cur_time = time(NULL);
  time_t time_diff, time_step;

  for (i = 1; i < TSDBW_DB_NUM; ++i) { // omitting the finest TSDB

     // if (db_set_h->cb_communication.rows[i-1]->last_flush_time == 0) continue; //skip if the given TSDB is empty (newly created)

      time_diff = cur_time - db_set_h->cb_communication.rows[i-1]->last_flush_time;
      time_step = (time_t)db_set_h->db_hs[i]->slot_duration;

      if (time_diff < 0) {

          trace_error("current time is less then time of last DB flush. It is either a logical mistake or type overflow.");
          tsdbw_close(db_set_h);
          return -1;

      } else if (time_diff >= time_step ) {

          i == TSDBW_MODERATE ? sprintf(report_str,"moderate") : sprintf(report_str,"coarse");

          trace_info("Flushing %s TSDB\n",report_str);

          if (tsdbw_consolidated_flush(db_set_h->db_hs[i], db_set_h->cb_communication.rows[i-1], db_set_h->last_accum_update  )) {
              return -1;
          }

      }
  }

  return 0;
}

static int get_list_of_epochs(tsdb_handler *db_h, u_int32_t epoch_from, u_int32_t epoch_to,
                        u_int32_t **epochs_list_p, u_int8_t **isEpochEmpty_p, u_int32_t *epoch_num) {
  /* The function searches epochs in interval provided by arguments
   * in the given TSDB. A new sorted list (time ascending) of epochs
   * in the range as well as list of missing epochs are
   * written into epochs_list and isEpochEmpty arrays
   * respectively. Number of epochs is set
   * in epoch_num. */

  if (epoch_from > epoch_to ) {
      trace_error("Wrong epoch range");
      return -1;
  }

  u_int32_t *epochs_list;
  u_int8_t *isEpochEmpty;

  normalize_epoch(db_h, &epoch_from);
  normalize_epoch(db_h, &epoch_to);

  if (epoch_from == epoch_to || epoch_from + db_h->slot_duration == epoch_to) {
      /* One epoch */
      epochs_list = (u_int32_t *) malloc(sizeof(u_int32_t));
      isEpochEmpty = (u_int8_t *) malloc(sizeof(u_int8_t));
      if (epochs_list == NULL || isEpochEmpty == NULL) return -1;
      *epochs_list = epoch_to;
      *isEpochEmpty = 0;
      *epoch_num = 1;

  } else {
      /* All other cases */
      u_int32_t i, j;
      *epoch_num = epoch_from / epoch_to;
      epochs_list = (u_int32_t *) malloc(sizeof(u_int32_t) * *epoch_num);
      isEpochEmpty = (u_int8_t *) malloc(sizeof(u_int8_t) * *epoch_num);
      if (epochs_list == NULL || isEpochEmpty == NULL) return -1;
      for (i = 0, j = 0; i < *epoch_num; ++i) {
          epochs_list[i] = epoch_from + i * db_h->slot_duration; //TODO check epochs_list[*epoch_num - 1] == epoch_to
          if (epochs_list[i] < db_h->epoch_list[0] ||
              epochs_list[i] > db_h->most_recent_epoch ||
              epochs_list[i] < db_h->epoch_list[j]) {
              isEpochEmpty[i] = 1;
          } else {
              isEpochEmpty[i] = 0;
              j++;
          }
      }
      //TODO check j == db_h->number_of_epochs
  }

  *epochs_list_p = epochs_list;
  *isEpochEmpty_p = isEpochEmpty;

  return 0;
}

static int check_args_query(tsdb_handler *tsdb_h, time_t *epoch_from,
    time_t *epoch_to,  char **metrics, u_int32_t metrics_num, data_tuple_t ***tuples ) {

  int i;

  if (tsdb_h == NULL) {
      trace_error("TSDB handle not allocated");
      return -1;
  }

  if (epoch_from == NULL || epoch_to == NULL ) {
      trace_error("Null epoch pointers");
      return -1;
  }

  if (*epoch_from > *epoch_to) {
      trace_error("Wrong epoch range");
      return -1;
  }

  if (*epoch_to > (u_int32_t) time(NULL)) {
      trace_info("Epoch range exceeds the current time. The upper bound was set to the current time.");
      *epoch_to = (u_int32_t) time(NULL);
  }

  if (metrics == NULL) {
      trace_error("Argument for an array of metrics is NULL pointer");
      return -1;
  }

  for (i = 0; i < metrics_num; ++i) {
      if (strlen(metrics[i]) > MAX_METRIC_STRING_LEN) {
          trace_error("The metric %s exceeds max allowed string length of %u bytes", metrics[i], MAX_METRIC_STRING_LEN);
          return -1;
      }
  }

  if (tuples == NULL) {
      trace_error("Argument for an address of an array of query results is NULL pointer");
      return -1;
  }

  return 0;

}

static int tsdbw_query_alloc_result_array(data_tuple_t ***tuples,
    u_int32_t metrics_num,
    u_int32_t epoch_num) {

  data_tuple_t **query_res = *tuples;
  u_int32_t metr_idx;

  /* Outer allocation */
  query_res = (data_tuple_t **) calloc(metrics_num, sizeof(data_tuple_t *));

  if (query_res == NULL) {
      trace_error("Failed to allocate memory");
      return -1;
  }

  /* Inner allocation */
  for (metr_idx = 0; metr_idx < metrics_num; ++metr_idx) {
      query_res[metr_idx] = (data_tuple_t *) calloc(epoch_num, sizeof(data_tuple_t));
      if (query_res[metr_idx] == NULL) {
          u_int32_t i;
          for (i = 0; i < metr_idx; ++i) {
              free(query_res[i]);
          }
          free(query_res);
          trace_error("Failed to allocate memory");
          return -1;
      }
  }
  return 0;
}

int tsdbw_query(tsdbw_handle *db_set_h, q_request_t *req, q_reply_t *rep) {

  /* Unpacking request*/
  time_t epoch_from =  req->epoch_from;
  time_t epoch_to = req->epoch_to;
  char **metrics = req->metrics;
  u_int32_t metrics_num = req->metrics_num;
  char granularity_flag = req->granularity_flag;

  tsdb_handler *tsdb_h;

  if (metrics_num == 0) {
      rep->epochs_num_res = 0;
      rep->tuples = NULL;
      return 0;
  }

  switch(granularity_flag) {
  case TSDBW_FINE:
    tsdb_h = db_set_h->db_hs[TSDBW_FINE];
    break;
  case TSDBW_MODERATE:
    tsdb_h = db_set_h->db_hs[TSDBW_MODERATE];
    break;
  case TSDBW_COARSE:
    tsdb_h = db_set_h->db_hs[TSDBW_COARSE];
    break;
  default:
    return -1;
  }

  if (check_args_query(tsdb_h, &epoch_from, &epoch_to, metrics, metrics_num, &rep->tuples )) return -1;

  u_int32_t *epochs_list = NULL, epoch_num = 0;
  u_int8_t *isEpochEmpty = NULL;
  if (get_list_of_epochs(tsdb_h, epoch_from, epoch_to, &epochs_list, &isEpochEmpty, &epoch_num)) return -1;

  /* Allocate memory for the array of the query results */
  if (tsdbw_query_alloc_result_array(&rep->tuples, metrics_num, epoch_num) ) {
      free(epochs_list);
      free(isEpochEmpty);
      return -1;
  }
  /* Filling the query result array - for every metric, every epoch in the requested range */
  data_tuple_t **query_res = rep->tuples;
  tsdb_value *val = NULL;
  u_int8_t fail_if_epoch_missing = 1, allowed_to_grow_epochs = 0;
  u_int32_t metr_idx, epch_idx;
  char epoch_descr[20];

  for (epch_idx = 0; epch_idx < epoch_num; ++epch_idx){
      for (metr_idx = 0; metr_idx < metrics_num; ++metr_idx) {

          query_res[metr_idx][epch_idx].epoch = (time_t) epochs_list[epch_idx];

          if (isEpochEmpty[epch_idx] == 0) {
              /* If Epoch is not empty and exists: */
              if (tsdb_goto_epoch(tsdb_h, epochs_list[epch_idx], fail_if_epoch_missing, allowed_to_grow_epochs)) {
                  /* If we have failed to load this epoch, we treat it as empty one for all metrics: */
                  trace_error("Epoch was not found, though it must exist. Treating it like empty one.");
                  isEpochEmpty[epch_idx] = 1;
                  query_res[metr_idx][epch_idx].value = tsdb_h->unknown_value;
                  continue;
              }

              /* The epoch exists and we have loaded it */

              if (tsdb_get_by_key(tsdb_h, metrics[metr_idx], &val )) {
                  /* The value for the given metric and epoch does not exist, or the key is missing */
                  time2str(&epochs_list[epch_idx], epoch_descr, 20);
                  trace_info("No value or metric found. Epoch %s, metric %s", epoch_descr, metrics[metr_idx]);
                  query_res[metr_idx][epch_idx].value = tsdb_h->unknown_value;
              } else {
                  /* The value for the given metric and epoch does exist, but it
                   * might be either a SNMP provided value or default unknown one */
                  query_res[metr_idx][epch_idx].value = *((int64_t*) val);
              }
          } else {
              /* If Epoch does not exist: */
              query_res[metr_idx][epch_idx].value = tsdb_h->unknown_value;
          }
      }
  }

  rep->epochs_num_res = epoch_num;
  free(epochs_list);
  free(isEpochEmpty);

  return 0;
}
