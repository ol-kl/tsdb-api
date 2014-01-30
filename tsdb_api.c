/*
 * Modifications are made by
 *  Oleg Klyudt
 *
 *ORIGINAL COPYRIGHT:
 *  Copyright (C) 2011 IIT/CNR (http://www.iit.cnr.it/en)
 *                     Luca Deri <deri@ntop.org>
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include "tsdb_api.h"
#include "tsdb_bitmap.h"
#include "seatest.h"

static void db_put(tsdb_handler *handler,
                   void *key, u_int32_t key_len,
                   void *value, u_int32_t value_len) {
    DBT key_data, data;

    if (handler->read_only) {
        trace_warning("Unable to set value (read-only mode)");
        return;
    }

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = key;
    key_data.size = key_len;
    data.data = value;
    data.size = value_len;

    if (handler->db->put(handler->db, NULL, &key_data, &data, 0) != 0) {
        trace_error("Error while map_set(%s, %d)", (char*)key, *((tsdb_value*)value));
    }
}

static int db_get(tsdb_handler *handler,
                  void *key, u_int32_t key_len,
                  void **value, u_int32_t *value_len) {
  /* DBT data;
   * db->get() with default flags of data DBT structure
   * set to 0 will allocate memory for the data internally and
   * will return pointer to the first element in data.data.
   * Size of allocated memory will be data.size.
   * HOWEVER! Every subsequent call to this function will deallocate previously
   * allocated area of memory to serve new "get" request. It means the pointer
   * returned to a newly allocated memory area will be exactly the same as in the
   * previous call. Thus if data from prev function call was copied shallowly, than it
   * may be lost. Deep copy must be exercised. Alternatively flags in the
   * DBT structure for the data can be changed, e.g.,
   * data.flags = DB_DBT_REALLOC can be set.
   * I could not find this behaviour description in the Berkeley DB docs,
   * therefore it is important. */

    DBT key_data, data;

    memset(&key_data, 0, sizeof(key_data));
    memset(&data, 0, sizeof(data));

    key_data.data = key;
    key_data.size = key_len;

    if (handler->db->get(handler->db, NULL, &key_data, &data, 0) == 0) {
        *value = data.data, *value_len = data.size;
        return 0;
    } else {
        return -1;
    }
}

static int db_key_exists (tsdb_handler *handler, void *key, u_int32_t key_len) {
  int rv;
  DBT key_data;

  memset(&key_data, 0, sizeof(key_data));

  key_data.data = key;
  key_data.size = key_len;

  rv = handler->db->exists(handler->db,NULL,&key_data,0);

  if (rv ==  DB_NOTFOUND) {
      return 0;
  }
  else {
      return 1;
  }
}

int tsdb_open(const char *tsdb_path, tsdb_handler *handler,
	      u_int16_t *values_per_entry,
	      u_int32_t slot_duration,
	      u_int8_t read_only) {
    void *value;
    u_int32_t value_len;
    int ret, mode;

    memset(handler, 0, sizeof(tsdb_handler));

    handler->read_only = read_only;
    mode = (read_only ? 00444 : 00664 );

    if ((ret = db_create(&handler->db, NULL, 0)) != 0) {
        trace_error("Error while creating DB handler [%s]", db_strerror(ret));
        return -1;
    }



    if ((ret = handler->db->open(handler->db,
                                 NULL,
                                 (const char*)tsdb_path,
                                 NULL,
                                 DB_BTREE,
                                 (read_only ? DB_RDONLY : DB_CREATE),
                                 mode)) != 0) {
        trace_error("Error while opening DB %s [%s][r/o=%u,mode=%o]",
                    tsdb_path, db_strerror(ret), read_only, mode);
        return -1;
    }


    if (db_get(handler, "lowest_free_index",
               strlen("lowest_free_index"),
               &value, &value_len) == 0) {
        handler->lowest_free_index = *((u_int32_t*)value);
    } else {
        if (!handler->read_only) {
            handler->lowest_free_index = 0;
            db_put(handler, "lowest_free_index",
                   strlen("lowest_free_index"),
                   &handler->lowest_free_index,
                   sizeof(handler->lowest_free_index));
        }
    }

    if (db_get(handler, "epoch_list",
               strlen("epoch_list"),
               &value, &value_len) == 0) {
        handler->epoch_list = (u_int32_t*) malloc(value_len);
        memcpy(handler->epoch_list, value , value_len);
    } else {
        if (!handler->read_only) {
            handler->epoch_list = NULL;
        }
    }

    if (db_get(handler, "slot_duration",
               strlen("slot_duration"),
               &value, &value_len) == 0) {
        handler->slot_duration = *((u_int32_t*)value);
    } else {
        if (!handler->read_only) {
            handler->slot_duration = slot_duration;
            db_put(handler, "slot_duration",
                   strlen("slot_duration"),
                   &handler->slot_duration,
                   sizeof(handler->slot_duration));
        }
    }

    if (db_get(handler, "values_per_entry",
               strlen("values_per_entry"),
               &value, &value_len) == 0) {
        *values_per_entry = handler->values_per_entry =
            *((u_int16_t*)value);
    } else {
        if (!handler->read_only) {
            handler->values_per_entry = *values_per_entry;
            db_put(handler, "values_per_entry",
                   strlen("values_per_entry"),
                   &handler->values_per_entry,
                   sizeof(handler->values_per_entry));
        }
    }

    if (db_get(handler, "num_epochs",
                   strlen("num_epochs"),
                   &value, &value_len) == 0) {
            handler->number_of_epochs = *((u_int32_t*)value);
        } else {
            if (!handler->read_only) {
                handler->number_of_epochs = 0;
                db_put(handler, "num_epochs",
                       strlen("num_epochs"),
                       &handler->number_of_epochs,
                       sizeof(handler->number_of_epochs));
            }
        }

    if (db_get(handler, "recent_epoch",
                   strlen("recent_epoch"),
                   &value, &value_len) == 0) {
            handler->most_recent_epoch = *((u_int32_t*)value);
        } else {
            if (!handler->read_only) {
                handler->most_recent_epoch = 0;
                db_put(handler, "recent_epoch",
                       strlen("recent_epoch"),
                       &handler->most_recent_epoch,
                       sizeof(handler->most_recent_epoch));
            }
        }

    handler->values_len = handler->values_per_entry * sizeof(tsdb_value);

    trace_info("lowest_free_index: %u", handler->lowest_free_index);
    trace_info("slot_duration: %u", handler->slot_duration);
    trace_info("values_per_entry: %u", handler->values_per_entry);

    memset(&handler->state_compress, 0, sizeof(handler->state_compress));
    memset(&handler->state_decompress, 0, sizeof(handler->state_decompress));

    handler->alive = 1;

    return 0;
}

void purge_chunk_with_fire(tsdb_handler *db_handler) {
  if (db_handler->chunk.data != NULL) {
      free(db_handler->chunk.data);
  }
  memset(&db_handler->chunk, 0, sizeof(db_handler->chunk));
  db_handler->chunk.epoch = 0;
  db_handler->chunk.data_len = 0;
  db_handler->chunk.new_epoch_flag = 0;
}

static int epoch_list_add(tsdb_handler *handler, u_int32_t epoch) {
  // reallocate memory and add put the epoch at the end
  // counter of epochs is increased as well
  u_int32_t *new_arr_p;
  new_arr_p = (u_int32_t *) realloc(handler->epoch_list, (handler->number_of_epochs + 1) * sizeof(u_int32_t));
  if (new_arr_p == NULL) {
      return -1;
  }
  handler->number_of_epochs++;
  handler->epoch_list = new_arr_p;
  handler->epoch_list[handler->number_of_epochs - 1] = epoch;

  return 0;
}

static void tsdb_flush_chunk(tsdb_handler *handler) {
    char *compressed;
    u_int compressed_len, new_len, num_fragments, i;
    u_int fragment_size;
    char str[32], rv=0;

    if (!handler->chunk.data) {
        purge_chunk_with_fire(handler);
        return;
    }

    if (handler->chunk.new_epoch_flag) {

        if (handler->most_recent_epoch >= handler->chunk.epoch) {
            trace_error("BUG: last epoch in DB %d >= current epoch %d being written \n", handler->most_recent_epoch, handler->chunk.epoch);
        }

        /* The following assertion is fundamental for the logic.
         * It assures that no epochs can be created in the past, and thus inserted
         * into list of epochs in DB at the end. This will violate the assumption,
         * that all epochs in the list are sorted in chronological order.
         * In principle one can cancel this limitation by introducing sorting
         * every time we flush a chunk for a new epoch into DB. However
         * if we have about 1 million of epochs and have to sort them for
         * every DB flush, it can prove being greedy for too much CPU resources */
        assert_true(handler->most_recent_epoch < handler->chunk.epoch);

        rv = epoch_list_add(handler, handler->chunk.epoch);
        if (rv) {
            trace_error("Epoch %lu will not be written, failed to allocate memory. Current chunk will be purged. We keep working.",handler->chunk.epoch );
            purge_chunk_with_fire(handler);
            return;
        }
        //handler->number_of_epochs ++; | It was incremented by epoch_list_add(), if reallocation succeeded

        db_put(handler, "epoch_list",
            strlen("epoch_list"),
            handler->epoch_list,
            handler->number_of_epochs * sizeof(handler->chunk.epoch));
        db_put(handler, "num_epochs",
            strlen("num_epochs"),
            &handler->number_of_epochs,
            sizeof(handler->number_of_epochs));

        if (handler->chunk.epoch > handler->most_recent_epoch) { //must always be true given the assertion above
            handler->most_recent_epoch = handler->chunk.epoch;
            db_put(handler, "recent_epoch", strlen("recent_epoch"),
                   &handler->most_recent_epoch, sizeof(handler->most_recent_epoch));
        }
    }

    fragment_size = handler->values_len * CHUNK_GROWTH;
    new_len = handler->chunk.data_len + CHUNK_LEN_PADDING;
    compressed = (char*)malloc(new_len);
    if (!compressed) {
        trace_error("Not enough memory (%u bytes)", new_len);
        return;
    }

    // Split chunks on the DB
    num_fragments = 1 + (handler->chunk.data_len -1) / fragment_size; //to avoid use of ceil() function

    for (i=0; i < num_fragments; i++) {
        u_int offset;

        if ((!handler->read_only) && handler->chunk.fragment_changed[i]) {
            offset = i * fragment_size;

            compressed_len = qlz_compress(&handler->chunk.data[offset], //src, dst, init_len
                                          compressed, fragment_size,
                                          &handler->state_compress);

            trace_info("Compression %u -> %u [fragment %u] [%.1f %%]",
                       fragment_size, compressed_len, i,
                       ((float)(compressed_len*100))/((float)fragment_size));

            snprintf(str, sizeof(str), "%u-%u", handler->chunk.epoch, i);

            db_put(handler, str, strlen(str), compressed, compressed_len);
        } else {
            trace_info("Skipping fragment %u (unchanged)", i);
        }
    }

    free(compressed);
    /* Invoke the callback (if any) to allow manipulation
     * on the handler->chunk.data before emptying it  */
    if (handler->reportChunkDataCB.cb != NULL && handler->reportChunkDataCB.external_data != NULL) {
        if (handler->reportChunkDataCB.cb(handler, handler->reportChunkDataCB.external_data)) {
            trace_warning("CallBack call failed, no or incorrect data will be written into consolidated TSDBs.");
        }
    }
    /******/
    free(handler->chunk.data);
    memset(&handler->chunk, 0, sizeof(handler->chunk));
    handler->chunk.data = NULL;
    handler->chunk.epoch = 0;
    handler->chunk.data_len = 0;
    handler->chunk.new_epoch_flag = 0;
}

void tsdb_close(tsdb_handler *handler) {

    if (!handler->alive) {
        return;
    }

    tsdb_flush_chunk(handler);

    if (!handler->read_only) {
        trace_info("Flushing database changes...");
    }

    free(handler->epoch_list);
    handler->db->close(handler->db, 0);

    handler->alive = 0;
}

void normalize_epoch(tsdb_handler *handler, u_int32_t *epoch) {
    *epoch -= *epoch % handler->slot_duration;
//    *epoch += timezone - daylight * 3600; <-- Legacy code, it used to recalculate local time into UTC (in a wrong way, btw)
}

int tsdb_get_key_index(tsdb_handler *handler, char *key, u_int32_t *index) {
/*get index by key*/
    void *ptr;
    u_int32_t len;
    char str[32] = { 0 };

    snprintf(str, sizeof(str), "key-%s", key);

    if (db_get(handler, str, strlen(str), &ptr, &len) == 0) {
        *index = *(u_int32_t*)ptr;
        return 0;
    }
    return -1;
}

static void set_key_index(tsdb_handler *handler, char *key, u_int32_t index) {
    char str[32];

    snprintf(str, sizeof(str), "key-%s", key); // strlen(key) <= 31 - 4 = 27

    db_put(handler, str, strlen(str), &index, sizeof(index));

    trace_info("[NEW_SET] Mapping %s -> %u", key, index);
}

int tsdb_goto_epoch(tsdb_handler *handler,
                    u_int32_t epoch,
                    u_int8_t fail_if_missing,
                    u_int8_t growable) {
  //if epoch exists, it loads it with all fragments
  //otherwise, if permitted, a new empty epoch is created
    int rc;
    void *value;
    u_int32_t value_len, fragment = 0;
    char str[32];

    if (handler == NULL) {
        trace_error("DB handler is NULL pointer");
        return -1;
    }

    normalize_epoch(handler, &epoch);
    if (handler->chunk.epoch == epoch) {
        //by returning we effectively prevent extra disk writes (code
        //does not reach the tsdb_flush_chunk() line)
        return 0;
    }

    tsdb_flush_chunk(handler);

    //normalize_epoch(handler, &epoch);
    snprintf(str, sizeof(str), "%u-%u", epoch, fragment);

    rc = db_get(handler, str, strlen(str), &value, &value_len);

    if (rc == -1 && fail_if_missing) {
        return -1;
    }

    if (rc == -1 ) {
        if (handler->most_recent_epoch > epoch) {
            // intended epoch recored is in the past and does not exist
            trace_warning("Attempt to access a non-existing epoch in the past. In the current implementation this is PROHIBITED, as it breaks program logic");
            return -1;
        }
        // Having implemented the above condition, we make sure an every non-existent epoch we are about to write (thus creating it) is the most recent one and we don't change "the past"
        handler->chunk.new_epoch_flag = 1;
    }

    handler->chunk.epoch = epoch;
    handler->chunk.growable = growable;
    //handler->chunk.data = NULL is set after flushing

    if (rc == 0) {
        //ATTENTION! All fragments must exist consecutively, i.e., we cant have only fragments 3, 7 and 90
        //Fragments existing in the DB for every epoch must be [0,1,...,k], where k <= MAX_NUM_FRAGMENTS
        //Otherwise we cannot guarantee that if k+1 th fragment does not exists - there are no more fragments for
        //this epoch. This leads to a constraint on the way we write and save data in the DB - even if we have previously written
        //only indices, e.g., 7000 and 45000, which corresponds to fragments 0 and 4, eventually the fragments 0,1,2,3,4 must be written
        //for that epoch, even though the fragments 1,2,3 will be empty (have only zeros)

        u_int32_t new_decompr_chunk_len, offset = 0;
        u_int8_t *cur_data = NULL, *new_data = NULL;

        trace_info("Loading epoch %u", epoch);

        while (1) {
            cur_data = new_data;
            new_decompr_chunk_len = qlz_size_decompressed(value);
            new_data = (u_int8_t*) realloc(cur_data, handler->chunk.data_len + new_decompr_chunk_len);
            if (new_data == NULL) {
                trace_error("Not enough memory (%u bytes)",
                              handler->chunk.data_len+new_decompr_chunk_len);
                free(value);
                free(cur_data);
                return -2;
            }
            new_decompr_chunk_len = qlz_decompress(value, &new_data[offset], &handler->state_decompress);
            handler->chunk.data_len += new_decompr_chunk_len;
            fragment++;
            offset = handler->chunk.data_len;

            snprintf(str, sizeof(str), "%u-%u", epoch, fragment);
            if (db_get(handler, str, strlen(str), &value, &value_len) == -1) {
                break; // No more fragments
            }
        }

        if (new_data == NULL) {
            trace_error("Smth went wrong and epoch data was not read out into memory");
            return -2;
        }

        handler->chunk.data = new_data;
    }

    return 0;
}

int tsdb_epoch_exists(tsdb_handler *handler,
                    u_int32_t epoch) {

  if (!handler->alive) {
      return -1;
  }

  u_int32_t fragment = 0;
  char str[32];

  normalize_epoch(handler, &epoch);

  snprintf(str, sizeof(str), "%u-%u", epoch, fragment);

  if (db_key_exists(handler, str, strlen(str))) {
      return 1;
  } else {
      return 0;
  }
}

static int ensure_key_index(tsdb_handler *handler, char *key,
                            u_int32_t *index, u_int8_t for_write) {
    if (tsdb_get_key_index(handler, key, index) == 0) {
        trace_info("Key %s mapped to index %u", key, *index);
        return 0;
    }

    if (!for_write) {
        trace_info("Unable to find key %s", key);
        return -1;
    }

    *index = handler->lowest_free_index++;
    set_key_index(handler, key, *index);

    /* CallBack time! We report a new key discovery */
    if (handler->reportNewMetricCB.cb != NULL && handler->reportNewMetricCB.external_data != NULL) {
        if (handler->reportNewMetricCB.cb(key, handler->reportNewMetricCB.external_data)) {
            trace_warning("CallBack call failed, consolidated TSDBs will have keys missing. Data loss in those DBs possible.");
        }
    }
    /******/

    db_put(handler,
           "lowest_free_index", strlen("lowest_free_index"),
           &handler->lowest_free_index,
           sizeof(handler->lowest_free_index));

    return 0;
}

static int prepare_offset_by_index(tsdb_handler *handler, u_int32_t *index,
                                   u_int64_t *offset, u_int8_t for_write) {
  /*index - absolute value. This func loads a respective fragment of the current epoch,
   *decompresses it and put in the chunk struct (memory gets allocated internally) */

    if (!handler->chunk.data) { // empty chunk.data assumes that the whole epoch is new,
                                // because otherwise it would be filled with data of the epoch
                                // by tsdb_goto_epoch
        if (!for_write) {
            return -1;
        }

        char str[32];
        void *value;
        u_int8_t *old_data_ptr = NULL, *new_data_ptr = NULL;
        u_int32_t fragment = *index / CHUNK_GROWTH, value_len;
        size_t new_size;

        if (fragment) {
            old_data_ptr = (u_int8_t*) malloc(fragment * CHUNK_GROWTH * handler->values_len); //allocate memory for all prev fragments
            if (old_data_ptr == NULL) {
                trace_error("Not enough memory (%u bytes)", fragment * CHUNK_GROWTH * handler->values_len);
                return -2;
            }
        }
        // Load the epoch handler->chunk.epoch/fragment


        snprintf(str, sizeof(str), "%u-%u", handler->chunk.epoch, fragment);
        if (db_get(handler, str, strlen(str), &value, &value_len) == -1) {
            //requested fragment does not exist
            new_size = (fragment+1) * CHUNK_GROWTH * handler->values_len;
            new_data_ptr = (u_int8_t*) realloc(old_data_ptr, new_size);
            if (new_data_ptr == NULL) {
                trace_error("Not enough memory (%u bytes)", new_size);
                return -2;
            }
            handler->chunk.data_len = new_size;
            handler->chunk.data = new_data_ptr;
            memset(handler->chunk.data,
                   handler->unknown_value,
                   handler->chunk.data_len);
//            u_int32_t mem_len = handler->values_len * CHUNK_GROWTH;
//            handler->chunk.data_len = mem_len;
//            handler->chunk.data = (u_int8_t*)malloc(mem_len);
//            if (handler->chunk.data == NULL) {
//                trace_error("Not enough memory (%u bytes)", mem_len);
//                return -2;
//            }
//            memset(handler->chunk.data,
//                   handler->unknown_value,
//                   handler->chunk.data_len);
        } else {
            size_t old_size_as_offset = fragment * CHUNK_GROWTH * handler->values_len;
            new_size = old_size_as_offset + qlz_size_decompressed(value);
            new_data_ptr = (u_int8_t*) realloc(old_data_ptr, new_size);
            if (new_data_ptr == NULL) {
                trace_error("Not enough memory (%u bytes)", new_size);
                return -2;
            }
            handler->chunk.data_len = new_size;
            handler->chunk.data = new_data_ptr;
            memset(handler->chunk.data,
                   handler->unknown_value,
                   handler->chunk.data_len);
            qlz_decompress(value, &handler->chunk.data[old_size_as_offset],
                                       &handler->state_decompress);
            free(value);

//            handler->chunk.data_len = qlz_size_decompressed(value);
//            handler->chunk.data = (u_int8_t*)malloc(handler->chunk.data_len);
//            if (handler->chunk.data == NULL) {
//                trace_error("Not enough memory (%u bytes)",
//                            handler->chunk.data_len);
//                return -2;
//            }
//            qlz_decompress(value, handler->chunk.data,
//                           &handler->state_decompress);
        }
        //absolute index of a first element in the given fragment
//        handler->chunk.base_index = fragment * CHUNK_GROWTH;
        //relative index to that fragment
//        *index -= handler->chunk.base_index;
    } else {
        get_offset:

            if (*index >= (handler->chunk.data_len / handler->values_len)) {
                if (!for_write || !handler->chunk.growable) {
                    return -1;
                }

                u_int32_t to_add = CHUNK_GROWTH * handler->values_len;
                u_int32_t new_len = handler->chunk.data_len + to_add;
                u_int8_t *ptr = malloc(new_len);

                if (!ptr) {
                    trace_error("Not enough memory (%u bytes): unable to grow "
                                "table", new_len);
                    return -2;
                }

                memcpy(ptr, handler->chunk.data, handler->chunk.data_len);
                free(handler->chunk.data);
                memset(&ptr[handler->chunk.data_len],
                       handler->unknown_value, to_add);
                handler->chunk.data = ptr;
                handler->chunk.data_len = new_len;

                trace_warning("Epoch grown to %u", new_len);

                goto get_offset;
            }
    }

    //relative index within current fragment(chunk), offset is in bytes, index in elements (tsdb_value * values_per_entry)
    *offset = handler->values_len * *index;

    if (*offset >= handler->chunk.data_len) {
        trace_error("INTERNAL ERROR [Id: %u][Offset: %u/%u]",
                    *index, *offset, handler->chunk.data_len);
    }

    return 0;
}

static int prepare_offset_by_key(tsdb_handler *handler, char *key,
                                 u_int64_t *offset, u_int8_t for_write) {
    u_int32_t index; //absolute value, not relative to a fragment

    if (!handler->chunk.epoch) {
        return -1;
    }

    if (ensure_key_index(handler, key, &index, for_write) == -1) {
        trace_info("Unable to find index %s", key);
        return -1;
    }

    trace_info("%s mapped to idx %u", key, index);

    return prepare_offset_by_index(handler, &index, offset, for_write);
}

int tsdb_set_with_index(tsdb_handler *handler, char *key,
                        tsdb_value *value, u_int32_t *index) {
  /* Obsolete and useless. Use tsdb_set_by_index instead. */
    tsdb_value *chunk_ptr;
    u_int64_t offset;
    int rc, i;
    unsigned char just_created = 0;

    if (!handler->alive) {
        return -1;
    }

    if (!handler->chunk.epoch) {
        trace_error("Missing epoch");
        return -2;
    }

    if (handler->chunk.data == NULL) {
        just_created = 1;
    }

    rc = prepare_offset_by_key(handler, key, &offset, 1);
    if (rc == 0) {
        chunk_ptr = (tsdb_value*)(&handler->chunk.data[offset]);
        memcpy(chunk_ptr, value, handler->values_len);

        // Mark a fragment as changed
        *index = offset / handler->values_len;
        int fragment = *index / CHUNK_GROWTH;
        if (fragment > MAX_NUM_FRAGMENTS - 1) {
            trace_error("Internal error [%u > %u]",
                        fragment, MAX_NUM_FRAGMENTS);
            if (just_created) {
                free(handler->chunk.data);
                handler->chunk.data = NULL;
            }
        } else {
            handler->chunk.fragment_changed[fragment] = 1;
            if (just_created) {
                for (i = 0; i < fragment; ++i) {
                    handler->chunk.fragment_changed[i] = 1;
                }
            }
        }

    }

    return rc;
}

int tsdb_set_by_index(tsdb_handler *handler, tsdb_value *value, u_int32_t *index) {

  tsdb_value *chunk_ptr;
  u_int64_t offset;
  int rc, i;
  unsigned char just_created = 0;

  if (!handler->alive) {
      return -1;
  }

  if (!handler->chunk.epoch) {
      trace_error("Missing epoch");
      return -2;
  }

  if (handler->chunk.data == NULL) {
      just_created = 1;
  }

  //rc = prepare_offset_by_key(handler, key, &offset, 1);
  if (*index >= handler->lowest_free_index) {
      trace_error("Index %ld was not mapped yet to a key, hence we refuse setting by it. Use tsdb_set with provided key name instead to create mapping key-index automatically.",*index);
      return -1;
  }
  rc = prepare_offset_by_index(handler, index, &offset, 1);
  if (rc == 0) {
      chunk_ptr = (tsdb_value*)(&handler->chunk.data[offset]);
      memcpy(chunk_ptr, value, handler->values_len);

      // Mark a fragment as changed
      *index = offset / handler->values_len;
      int fragment = *index / CHUNK_GROWTH;
      if (fragment > MAX_NUM_FRAGMENTS - 1) {
          trace_error("Internal error [%u > %u]",
              fragment, MAX_NUM_FRAGMENTS);
          if (just_created) {
              free(handler->chunk.data);
              handler->chunk.data = NULL;
          }
      } else {
          handler->chunk.fragment_changed[fragment] = 1;
          if (just_created) {
              for (i = 0; i < fragment; ++i) {
                  handler->chunk.fragment_changed[i] = 1;
              }
          }
      }

  }

  return rc;
}

int tsdb_set(tsdb_handler *handler, char *key, tsdb_value *value) {
    u_int32_t index; //relative to current chunk
    return tsdb_set_with_index(handler, key, value, &index);
}

int tsdb_get_by_key(tsdb_handler *handler, char *key, tsdb_value **value) {
    u_int64_t offset;
    int rc;

    if (!handler->alive || !handler->chunk.data) {
        return -1;
    }

    rc = prepare_offset_by_key(handler, key, &offset, 0);
    if (rc == 0) {
        *value = (tsdb_value*)(handler->chunk.data + offset);
    }

    return rc ;
}

int tsdb_get_by_index(tsdb_handler *handler, u_int32_t *index,
                      tsdb_value **value) {
    u_int64_t offset;
    int rc;

    if (!handler->alive || !handler->chunk.data) {
        return -1;
    }

    rc = prepare_offset_by_index(handler, index, &offset, 0);
    if (rc == 0) {
        *value = (tsdb_value*)(handler->chunk.data + offset);
    }

    return rc ;
}

void tsdb_flush(tsdb_handler *handler) {
    if (!handler->alive || handler->read_only) {
        return;
    }
    trace_info("Flushing database changes");
    tsdb_flush_chunk(handler);
    handler->db->sync(handler->db, 0);
}

static int load_tag_array(tsdb_handler *handler, char *name,
                          tsdb_tag *tag) {
  //find a tag structure named "name" and load it into "tag"
    void *ptr;
    u_int32_t len;
    char str[255] = { 0 }; //empty string

    snprintf(str, sizeof(str), "tag-%s", name);

    if (db_get(handler, str, strlen(str), &ptr, &len) == 0) {
        u_int32_t *array;
        array = (u_int32_t*)malloc(len);
        if (array == NULL) {
            //memory allocation failed
            free(ptr);
            return -2;
        }
        memcpy(array, ptr, len);
        tag->array = array;
        tag->array_len = len;
        return 0;
    }

    return -1;
}

static int allocate_tag_array(tsdb_tag *tag) {
   // u_int32_t array_len = CHUNK_GROWTH / sizeof(u_int32_t);
    /*it will only contain indices enough for one chunk,
     * better is CHUNK_GROWTH*MAX_NUM_FRAGMENTS / BITS_PER_WORD + 1 */
    u_int32_t array_len = 1 + CHUNK_GROWTH*MAX_NUM_FRAGMENTS / BITS_PER_WORD;
    u_int32_t* array = malloc(array_len);
    if (!array) {
        return -1;
    }

    memset(array, 0, array_len);

    tag->array = array;
    tag->array_len = array_len;

    return 0;
}

static void set_tag(tsdb_handler *handler, char *name, tsdb_tag *tag) {
  //tag->array must be allocated!
    char str[255];

    snprintf(str, sizeof(str), "tag-%s", name);

    db_put(handler, str, strlen(str), tag->array, tag->array_len);
}

static int ensure_tag_array(tsdb_handler *handler, char *name, tsdb_tag *tag) {
  // if tag exists in DF - load it, otherwise allocates empty one of size CHUNK_GROWTH/size of uint32
    if (load_tag_array(handler, name, tag) == 0) {
        return 0;
    }

    if (allocate_tag_array(tag) == 0) {
        return 0;
    }

    return -1;
}

int tsdb_tag_key(tsdb_handler *handler, char *key, char *tag_name) {
  //map key to tag_name. tag_name may be mapped to an arbitrary number of keys (indices respectively)
    u_int32_t index;

    if (tsdb_get_key_index(handler, key, &index) == -1) {
        // returned index is an absolute value
        return -1;
    }

    tsdb_tag tag;
    if (ensure_tag_array(handler, tag_name, &tag)) {
        return -1;
    }

    set_bit(tag.array, index); // set index-th bit of tag.array to 1
    set_tag(handler, tag_name, &tag);

    free(tag.array);

    return 0;
}

void scan_tag_indexes(tsdb_tag *tag, u_int32_t *indexes,
                      u_int32_t max_index, u_int32_t *count) {
    u_int32_t i, j, index;
    u_int32_t max_word = max_index / BITS_PER_WORD;

    *count = 0;

    for (i = 0; i <= max_word; i++) {
        if (tag->array[i] == 0) {
            continue;
        }
        for (j = 0; j < BITS_PER_WORD; j++) {
            index = i * BITS_PER_WORD + j;
            if (index > max_index) {
                break;
            }
            if (get_bit(tag->array, index)) {
                indexes[(*count)++] = index;
            }
        }
    }
}

static u_int32_t max_tag_index(tsdb_handler *handler, u_int32_t max_len) {
    if (handler->lowest_free_index < max_len) {
        return handler->lowest_free_index - 1;
    } else {
        return max_len - 1;
    }
}

int tsdb_get_tag_indexes(tsdb_handler *handler, char *tag_name,
                         u_int32_t *indexes, u_int32_t indexes_len,
                         u_int32_t *count) {
    tsdb_tag tag;
    if (load_tag_array(handler, tag_name, &tag) == 0) {
        u_int32_t max_index = max_tag_index(handler, indexes_len);
        scan_tag_indexes(&tag, indexes, max_index, count);
        free(tag.array);
        return 0;
    }

    return -1;
}

int tsdb_get_consolidated_tag_indexes(tsdb_handler *handler,
                                      char **tag_names,
                                      u_int16_t tag_names_len,
                                      int consolidator,
                                      u_int32_t *indexes,
                                      u_int32_t indexes_len, // up to how many indices to consider
                                      u_int32_t *count) {
  /*This function will set aggregated indices to "indexes" and its number to "count"*/
    u_int32_t i, j, max_index, max_word, extra_bits, nullify_mask;
    tsdb_tag consolidated, current;

    consolidated.array = NULL;
    consolidated.array_len = 0;
    max_index = max_tag_index(handler, indexes_len);
    max_word = max_index / BITS_PER_WORD;
    extra_bits = max_index % BITS_PER_WORD;
    nullify_mask = 0; nullify_mask = ~nullify_mask;
    nullify_mask = nullify_mask >> (BITS_PER_WORD - extra_bits); //Logical shift for unsinged integers - padding with zeros MSB positions

    *count = 0;

    for (i = 0; i < tag_names_len; i++) {
        if (load_tag_array(handler, tag_names[i], &current) == 0) {
            if (consolidated.array) {
                for (j = 0; j <= max_word; j++) {
                    switch (consolidator) {
                    case TSDB_AND:
                        if (j == max_word){
                            consolidated.array[j] &= current.array[j] & nullify_mask;
                            break; //DEBUG THIS!
                        }
                        consolidated.array[j] &= current.array[j];
                        break;
                    case TSDB_OR:
                        if (j == max_word){
                            consolidated.array[j] &= current.array[j] & nullify_mask;
                            break; //DEBUG THIS!
                        }
                        consolidated.array[j] |= current.array[j];
                        break;
                    default:
                        if (j == max_word){
                            consolidated.array[j] &= current.array[j] & nullify_mask;
                            break; //DEBUG THIS!
                        }
                        consolidated.array[j] = current.array[j];
                    }
                }
                free(current.array);
            } else {
                consolidated.array = current.array;
                consolidated.array_len = current.array_len;
            }
        }
    }

    if (consolidated.array) {
        scan_tag_indexes(&consolidated, indexes, max_index, count);
        free(consolidated.array);
    }

    return 0;
}
