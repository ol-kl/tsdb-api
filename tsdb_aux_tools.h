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

/* Memory deallocation for a double indexed array*/
void free_darray(size_t nrows, void **arr);

/* Prepend UTC to the formated string and prints it out into the given file stream */
/* Return value: negative on errors, non-negative to indicate the number of characters written */
int tfprintf(FILE *fout, char *msg, ...);

#endif /* TSDB_AUX_TOOLS_H_ */
