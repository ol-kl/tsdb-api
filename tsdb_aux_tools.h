/*
 * tsdb_aux_tools.h
 *
 *  Created on: Dec 5, 2013
 *   Author(s): Oleg Klyudt
 */

#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

#ifndef TSDB_AUX_TOOLS_H_
#define TSDB_AUX_TOOLS_H_

#define BYTE(X) ((unsigned char *)(X))

typedef struct darray DArray;

struct darray{
  void **data;
  void *__fill_val;
  void *__col_p;
  size_t coln;
  size_t rown;
  u_int8_t __data_allocated;
  size_t __elem_size;
  void (* destroy)(DArray *self); //destructor for the structure
  void (* wipe_data)(DArray *self); //delete and deallocate only internal data, preserving the same filling element and the size of type
  int (*add_col)(DArray *self, u_int32_t n, u_int8_t to_end); //add n columns at the end (if to_end is true) of the array and fill them with default value. If to_end is false - add cols at the beginning.
  int (*app_col)(DArray *self, void *cdata, size_t cdata_num_elem); //append one given column at the end
  int (*prp_col)(DArray *self, void *cdata, size_t cdata_num_elem); //prepend one given column at the beginning
  int (*add_row)(DArray *self, u_int32_t n); //add n rows at the end of the array and fill them with default value
  void* (*get_col)(DArray *self, u_int32_t n); //get deep copy of the n-th indexed column
  void* (*get_row)(DArray *self, u_int32_t n); //get deep copy of the n-th indexed row
};

/* Create a new DArray struct. *fillval is deep copied. */
DArray * new_darray(size_t rown, size_t coln, size_t elem_size, void *fillval );

/* check file existence using process real uid, gid */
int fexist(const char* fname);

/* remove file fname */
int fremove(const char* fname);

/* Calculate time difference  */
int timeval_subtract (struct timeval *result,
                      struct timeval *x,
                      struct timeval *y);

/* Convert timeval struct into float number as seconds.microseconds */
float timeval2float (struct timeval *timestamp);

/* Write time into provided string formatting it as %T %d-%m-%Y*/
void time2str(u_int32_t *tmr, char* str, size_t strsize);

/* Random permutation of an arbitrary array using Fisher–Yates algorithm */
void shuffle(void *obj, size_t nmemb, size_t size);

/* Memory allocation for a double indexed array */
void** malloc_darray(size_t nrows, size_t ncols, size_t elem_size);
void** calloc_darray(size_t nrows, size_t ncols, size_t elem_size);
void** realloc_darray(void **old_arr, size_t nrows, size_t ncols, size_t old_nrows, size_t elem_size);

/* Memory deallocation for a double indexed array*/
void free_darray(size_t nrows, void **arr);

int fill_darray(void **darr, size_t row_from, size_t row_to, size_t col_from, size_t col_to, void *val, size_t len);

/* Prepend UTC to the formated string and prints it out into the given file stream */
/* Return value: negative on errors, non-negative to indicate the number of characters written */
int tfprintf(FILE *fout, char *msg, ...);

/* Append string *scr to the string *dest with internal reallocation. Thus *dest must be allocated on heap. It gets deallocated internally */
/* Pointer to the new concatenated string is returned. In case of an error NULL pointer is returned */
char * strapp(char *dest, char *scr);

#endif /* TSDB_AUX_TOOLS_H_ */
