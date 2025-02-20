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

CLIENT_SRC = src/client.c \
             src/network.c     \
             src/encoding.c    \
             src/binary.c      \
             src/timeutil.c
CLIENT_OBJ = $(CLIENT_SRC:.c=.o)
CLIENT_EXEC = client

all: $(RAFT_C_EXEC)

$(RAFT_C_EXEC): $(RAFT_C_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

libraft.so: $(LIB_OBJECTS)
	$(CC) -shared -o $@ $(LIB_OBJECTS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(CLIENT_EXEC): $(CLIENT_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(RAFT_C_OBJ) $(RAFT_C_EXEC) libraft.so
	rm -f $(CLIENT_OBJ) ($(CLIENT_EXEC)

.PHONY: all clean

