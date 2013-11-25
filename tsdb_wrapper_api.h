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

typedef struct {
  u_int32_t epoch;
  u_int64_t value;
} data_tuple_t;

int tsdbw_query(u_int32_t epoch_from, u_int32_t epoch_to, const char **metrics, data_tuple_t **tuples, char granularity_flag);

int tsdbw_write(const char **metrics, const data_tuple_t **tuples);

int tsdbw_init(u_int16_t timestep, const char **db_files, char io_flag);


#endif /* TSDB_WRAPPER_API_H_ */
