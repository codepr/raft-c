CC=gcc
CFLAGS=-Wall                                  \
       -Werror                                \
       -pedantic                              \
       -ggdb                                  \
       -std=c2x                               \
       -fsanitize=address                     \
       -fsanitize=undefined                   \
       -fno-omit-frame-pointer                \
       -pg                                    \
       -Wno-gnu-zero-variadic-macro-arguments \
       -Ilib
LDFLAGS = -L. -lraft

RAFT_C_SRC = src/timeutil.c    \
             src/iomux.c       \
             src/parser.c      \
             src/config.c      \
             src/binary.c      \
             src/storage.c     \
             src/encoding.c    \
             src/cluster.c     \
             src/raft.c        \
             src/hash.c        \
             src/network.c     \
             src/timeseries.c  \
             src/partition.c   \
             src/index.c       \
             src/commitlog.c   \
             src/wal.c         \
             src/server.c
RAFT_C_OBJ = $(RAFT_C_SRC:.c=.o)
RAFT_C_EXEC = raft-c

RAFT_LIB_SOURCES = src/binary.c   \
                   src/storage.c  \
                   src/encoding.c \
                   src/raft.c
RAFT_LIB_OBJECTS = $(RAFT_LIB_SOURCES:.c=.o)

CLI_SRC = src/client.c      \
          src/raftcli.c     \
          src/network.c     \
          src/encoding.c    \
          src/binary.c      \
          src/parser.c      \
          src/timeutil.c
CLI_OBJ = $(CLI_SRC:.c=.o)
CLI_EXEC = raft-cli

TEST_SRC = tests/tests.c           \
           tests/timeseries_test.c \
           src/timeseries.c        \
           src/timeutil.c          \
           src/wal.c               \
           src/storage.c           \
           src/partition.c         \
           src/binary.c            \
           src/commitlog.c         \
           src/index.c
TEST_OBJ = $(TEST_SRC:.c=.o)
TEST_EXEC = raft-c-tests

all: $(RAFT_C_EXEC) $(CLI_EXEC) $(TEST_EXEC)

$(RAFT_C_EXEC): $(RAFT_C_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

libraft.so: $(LIB_OBJECTS)
	$(CC) -shared -o $@ $(LIB_OBJECTS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(CLI_EXEC): $(CLI_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

$(TEST_EXEC): $(TEST_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(RAFT_C_OBJ) $(RAFT_C_EXEC) libraft.so
	rm -f $(CLI_OBJ) ($(CLI_EXEC)

.PHONY: all clean

