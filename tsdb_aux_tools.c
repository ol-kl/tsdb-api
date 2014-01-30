/*
 * tsdb_aux_tools.c
 *
 *  Created on: Dec 5, 2013
 *      Author(s): Oleg Klyudt
 */

#include "tsdb_aux_tools.h"

int fexist(const char* fname) {
  if (access(fname,F_OK) != 0) return 0;
  return 1;
}

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

  if (nrows == 0 || ncols == 0) return NULL;

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

void** calloc_darray(size_t nrows, size_t ncols, size_t elem_size) {
  size_t i, j;
  void **tmp;

  if (nrows == 0 || ncols == 0) return NULL;

  if ((tmp = malloc(nrows * sizeof *tmp)) == NULL) return NULL;

  for(i = 0; i < nrows; ++i) {
      tmp[i] = calloc(ncols, elem_size);
      if (tmp[i] == NULL) {
          for(j = 0; j < i; ++j) free(tmp[j]);
          return NULL;
      }
  }
  return tmp;
}

void** realloc_darray(void **old_arr, size_t nrows, size_t ncols, size_t old_nrows, size_t elem_size) {
  void **tmp;
  if (old_arr == NULL) {tmp = malloc_darray(nrows, ncols, elem_size); return tmp;}

  if (nrows == 0 || ncols == 0) {
      free_darray(old_nrows, old_arr);
      return NULL;
  }

  size_t i, j;
  if (old_nrows > nrows) {
      for (i = 0; i < old_nrows - nrows; ++i) free(old_arr[old_nrows - 1 - i]);
  }

  tmp = realloc(old_arr, nrows * sizeof(void*)); if (tmp == NULL) return NULL;
  old_arr = tmp;

  /* set added pointers to NULL to trigger malloc-like behaviour when realloc operates on them*/
  if (nrows > old_nrows) {
      for (i = old_nrows; i < nrows; ++i) old_arr[i] = NULL;
  }

  for(i = 0; i < nrows; ++i) {
      tmp[i] = realloc(old_arr[i], ncols * elem_size);
      if (tmp[i] == NULL) {
          for(j = 0; j < i; ++j) free(tmp[i]);
          return NULL;
      }
  }
  return tmp;
}

int tfprintf(FILE *fout, char *msg, ...) {
  char buffer[256], timestr[32], *format;
  int rv;
  va_list args;
  u_int32_t ctime = time(NULL);

  time2str(&ctime, timestr, 32);
  format = (char *) malloc((strlen(timestr) + strlen(msg) + 3 + 1) * sizeof(char)); //+3 for [, ], space and +1 for /0
  if (format == NULL) return -1;
  if (sprintf(format, "[%s] %s", timestr, msg) < 0) goto err_cln;
  va_start (args, msg);
  if (vsnprintf (buffer,256, format, args) < 0) goto err_cln;
  va_end (args);
  free(format);

  rv = fprintf(fout, buffer);
  if (ferror(fout) || rv < 0) goto err_cln;
  fflush(fout);
  return rv;

  err_cln:
  free(format);
  return -1;
}

char * strapp(char *dest, char *scr) {
  if (dest == NULL || scr == NULL) return NULL;

  size_t buf_s = strlen(dest) + strlen(scr) + 1; // +1 for /0 character
  char *result = (char *) realloc(dest, buf_s);
  if (result == NULL) return NULL;
  memcpy(&result[strlen(result)], scr, strlen(scr));
  result[buf_s -1] = '\0';
  return result;
}

int fill_darray(void **darr, size_t row_from, size_t row_to, size_t col_from, size_t col_to, void *val, size_t len) {
  if (row_from == 0 || col_from == 0) return -1;
  if (row_from > row_to || col_from > col_to) return -2;
  if (val == NULL) return -3;
  size_t i,j;
  for (i=row_from -1 ; i < row_to; ++i) {
      for (j=col_from -1; j < col_to; ++j) {
          //memcpy(&darr[i][j], val, len); - WRONG WAY!!! one cannot index void arrays, as the value of shift for the second index is determined by the type of array and for void it is unknown. Shift for the first index is standard and irrelevant of underlying type, as it is pointer. Size of pointer is the same for all the types the pointer may refer to.
          memcpy(*(darr + i) + j*len, val, len);
      }
  }
  return 0;
}

static void __DArray_destroy(DArray *self) {
  if (self == NULL) return;
  free(self->__fill_val);
  free(self->__col_p);
  if (self->__data_allocated) free_darray(self->rown, self->data);
  free(self);
}

static void __DArray_wipedata(DArray *self) {
  if (self == NULL) return;
  free(self->__col_p);
  if (self->__data_allocated) free_darray(self->rown, self->data);
  self->rown = 0;
  self->coln = 0;
  self->data = NULL;
  self->__data_allocated = 0;
}

static void* __DArray_getrow(DArray *self, u_int32_t n) {
  /* Start counting since 0, performing deep copy of the column for external transparency */
  if (n >= self->rown) return NULL;

  return self->data[n];
}

static void* __DArray_getcol(DArray *self, u_int32_t n) {
  /* Start counting since 0, performing deep copy of the column for external transparency */
  if (n >= self->coln) return NULL;

  size_t i;
  void *new_col_p = realloc(self->__col_p, self->rown * self->__elem_size);
  if (new_col_p == NULL) return NULL;
  self->__col_p = new_col_p;

  for (i = 0; i < self->rown; ++i) {
      memcpy(self->__col_p + i * self->__elem_size,
          self->data[i] + n * self->__elem_size,
          self->__elem_size);
  }

  return self->__col_p;
}

static int __DArray_addcol(DArray *self, u_int32_t n, u_int8_t to_end) {

  int rv;
  size_t i;
  void **rdata;
  if (n == 0) return 0;

  /* Reallocate */
  if (self->rown == 0) {
      rdata = realloc_darray(self->data, self->rown + 1, self->coln + n, self->rown, self->__elem_size);
      self->rown += 1;
  }
  else rdata = realloc_darray(self->data, self->rown, self->coln + n, self->rown, self->__elem_size);
  if (rdata == NULL) return -1;

  /* Shift, if to_end is false */
  if (!to_end) {
      for (i = 0; i < self->rown; ++i) memmove(rdata[i]+n*self->__elem_size, rdata[i], self->coln * self->__elem_size);
  }

  /* Fill with default values */
  if (self->__fill_val == NULL) { // if not specified, fill with zeros
      char *tmp_val = (char *) malloc(self->__elem_size); if (tmp_val == NULL) return -2;
      memset(tmp_val, 0, self->__elem_size);
      if (!to_end) {
          rv = fill_darray(rdata, 1,  self->rown, 1, n, tmp_val, self->__elem_size);
      } else rv = fill_darray(rdata, 1,  self->rown, self->coln + 1, self->coln + n, tmp_val, self->__elem_size);
      free(tmp_val);
  }
  else {
      if (!to_end) {
          rv = fill_darray(rdata, 1,  self->rown, 1, n, self->__fill_val, self->__elem_size);
      } else rv = fill_darray(rdata, 1,  self->rown, self->coln + 1, self->coln + n, self->__fill_val, self->__elem_size);
  }

  if (rv < 0) return -3;

  /* Assign */
  self->data = rdata;
  if (!self->__data_allocated) self->__data_allocated = 1;
  self->coln += n;

  return 0;
}

static int __DArray_appcol(DArray *self, void *cdata, size_t cdata_num_elem) {
  /* Responsibility that *cdata is of the same type as self->data is on the programmer */
  if (self->rown == 0) self->rown = cdata_num_elem;
  if (self->rown != cdata_num_elem) return -1;
  self->add_col(self, 1, 1);

  size_t i;
  for (i = 0; i < self->rown; ++i) {
      memcpy(self->data[i] + (self->coln -1) * self->__elem_size,
          cdata + i * self->__elem_size,
          self->__elem_size);
  }
  return 0;
}

static int __DArray_prpcol(DArray *self, void *cdata, size_t cdata_num_elem) {
  /* Responsibility that *cdata is of the same type as self->data is on the programmer */
  if (self->rown != cdata_num_elem ) return -1;
  self->add_col(self, 1, 0); //prepend a column

  size_t i;
  for (i = 0; i < self->rown; ++i) {
      memcpy(self->data[i],
          cdata + i * self->__elem_size,
          self->__elem_size);
  }
  return 0;
}

static int __DArray_addrow(DArray *self, u_int32_t n) {

  int rv;
  void **rdata;
  if (n == 0) return 0;

  if (self->coln == 0) {
      rdata = realloc_darray(self->data, self->rown + n, self->coln + 1, self->rown, self->__elem_size);
      self->coln += 1;
  }
  else rdata = realloc_darray(self->data, self->rown + n, self->coln, self->rown, self->__elem_size);
  if (rdata == NULL) return -1;

  self->data = rdata;
  if (!self->__data_allocated) self->__data_allocated = 1;
  if (self->__fill_val == NULL) { // if not specified, fill with zeros
        char *tmp_val = (char *) malloc(self->__elem_size); if (tmp_val == NULL) return -2;
        memset(tmp_val, 0, self->__elem_size);
        rv = fill_darray(self->data, self->rown + 1,  self->rown + n, 1, self->coln, tmp_val, self->__elem_size);
        free(tmp_val);
    }
    else rv = fill_darray(self->data, self->rown + 1,  self->rown + n, 1, self->coln, self->__fill_val, self->__elem_size);

  if (rv < 0) return -3;
  self->rown += n;
  return 0;
}

DArray * new_darray(size_t rown, size_t coln, size_t elem_size, void *fillval ) {

  if (elem_size == 0) return NULL;

  DArray *darr = malloc(sizeof(DArray));
  if (darr == NULL) return NULL;

  /* basic init */
  darr->__fill_val = NULL;
  darr->data = NULL;

  darr->coln = coln;
  darr->rown = rown;

  if (fillval == NULL) {
      darr->__fill_val = NULL;
  } else {
      darr->__fill_val = malloc(elem_size);
      if (darr->__fill_val == NULL) goto err_clnup;
      memcpy(darr->__fill_val, fillval, elem_size);
  }

  if (coln != 0 && rown != 0) {
      if (darr->__fill_val == NULL) {
          darr->data = calloc_darray(darr->rown, darr->coln, elem_size); // fill with zeros by default
          if (darr->data == NULL) goto err_clnup;
      } else {
          darr->data = malloc_darray(darr->rown, darr->coln, elem_size);
          if (darr->data == NULL) goto err_clnup;
          if (fill_darray(darr->data, 1,  darr->rown, 1, darr->coln, fillval, elem_size) < 0) goto err_clnup;
      }

      darr->__data_allocated = 1;
  } else {
      darr->__data_allocated = 0;
  }


  darr->__elem_size = elem_size;
  darr->__col_p = NULL;
  darr->destroy = __DArray_destroy;
  darr->add_col = __DArray_addcol;
  darr->add_row = __DArray_addrow;
  darr->wipe_data = __DArray_wipedata;
  darr->app_col = __DArray_appcol;
  darr->prp_col = __DArray_prpcol;
  darr->get_col = __DArray_getcol;
  darr->get_row = __DArray_getrow;
  return darr;

  err_clnup:
  free(darr->__fill_val);
  if (darr->data) free_darray(rown, darr->data);
  free(darr);
  return NULL;
}
