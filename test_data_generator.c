/*
 * test_data_generator.c
 *
 *  Created on: Dec 5, 2013
 *      Author: Oleg Klyudt
 */

/* This routine emulates continous
 * data arrival and write in into a set
 * of TSDBs.
 * Purpose: implementation example and test of TSDBW API*/

#include "tsdb_wrapper_api.h"

int main() {

  tsdbw_handle db_bundle;
  u_int16_t basic_timestep = 60;

  const char *db_paths[3];
  db_paths[0] = "./DBs/f_db.tsdb";
  db_paths[1] = "./DBs/m_db.tsdb";
  db_paths[2] = "./DBs/c_db.tsdb";
  const char *metric = "metric-1";
  int64_t value = 100;

  set_trace_level(99);

  if (!tsdbw_init(&db_bundle, &basic_timestep, db_paths, 'w')) {
      printf("DBs are open!\n");
      tsdbw_write(&db_bundle, &metric, &value, 1);
  }
  else {
      printf("Failed to open DBs\n");
  }

  tsdbw_close(&db_bundle);

  return 0;
}

