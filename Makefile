CC           = gcc -g
CFLAGS       = -Wall -I. -DSEATEST_EXIT_ON_FAIL
LDFLAGS      = -L /opt/local/lib
SYSLIBS      = -ldb

TSDB_LIB     = libtsdb.a
TSDB_LIB_O   = tsdb_api.o tsdb_trace.o tsdb_bitmap.o quicklz.o tsdb_wrapper_api.o tsdb_aux_tools.o

TEST_LIBS    = $(TSDB_LIB) test_core.o seatest.o

TARGETS      = $(TSDB_LIB) \
				test-queryTime \
				test-data_generator

all: $(TARGETS)

%.o: %.c %.h
	${CC} ${CFLAGS} ${INCLUDE} -c $< -o $@

$(TSDB_LIB): $(TSDB_LIB_O)
	ar rs $@ ${TSDB_LIB_O}
	ranlib $@

tsdb-%: tsdb_%.o $(TSDB_LIB)
	$(CC) $(LDFLAGS) tsdb_$*.o $(TSDB_LIB) $(SYSLIBS) -o $@

test-%: test_%.o $(TEST_LIBS)
	$(CC) $(LDFLAGS) test_$*.o $(TEST_LIBS) $(SYSLIBS) -o $@

clean:
	rm -f ${TARGETS} *.o *~ test-* tsdb-*

.SECONDARY: $(TEST_LIBS)
