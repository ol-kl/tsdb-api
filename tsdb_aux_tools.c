/*
 * tsdb_aux_tools.c
 *
 *  Created on: Dec 5, 2013
 *      Author(s): Oleg Klyudt
 */

#include "tsdb_aux_tools.h"

int fremove(const char* fname) {

  if (!strlen(fname)){
      return (-1);
  }
  if (access(fname,F_OK) == -1) { //can't access the file
      if (errno != ENOENT){ //error is not of a file non existence type
          return (-1);
      }
  } else {//file exists, remove it
      if (unlink(fname)) {
          return (-1);
      }
  }

  return 0;
}

int timeval_subtract (struct timeval *result,
                      struct timeval *x,
                      struct timeval *y)
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

static int rrand(int m)
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

void time2str(u_int32_t *tmr, char* str, size_t strsize) {
  time_t temp_buf = *tmr; //we need it because alignment of types u_int32_t and time_t may not coincide
  struct tm *tmr_struct = gmtime(&temp_buf);
  strftime(str,strsize,"%T %d-%m-%Y",tmr_struct);
}

void free_darray(size_t nrows, void **arr) {
  size_t i;
  for(i = 0; i < nrows; ++i) free(arr[i]);
  free(arr);
}

void** malloc_darray(size_t nrows, size_t ncols, size_t elem_size) {
  size_t i, j;
  void **tmp;

  if ((tmp = malloc(nrows * sizeof *tmp)) == NULL) return NULL;

  for(i = 0; i < nrows; ++i) {
      tmp[i] = malloc(ncols * elem_size);
      if (tmp[i] == NULL) {
          for(j = 0; j < i; ++j) free(tmp[j]);
          return NULL;
      }
  }
  return tmp;
}


