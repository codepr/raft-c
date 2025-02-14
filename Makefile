CC=gcc
CFLAGS=-Wall					\
	   -Werror					\
	   -pedantic				\
	   -ggdb					\
	   -std=c2x					\
	   -fsanitize=address		\
	   -fsanitize=undefined		\
	   -fno-omit-frame-pointer	\
	   -pg						\
	   -Wno-gnu-zero-variadic-macro-arguments
LDFLAGS = -L. -lraft

RAFT_C_SRC = src/time_util.c \
			 src/config.c	 \
			 src/binary.c	 \
			 src/storage.c	 \
			 src/encoding.c	 \
			 src/cluster.c	 \
			 src/raft.c		 \
			 src/app_server.c
RAFT_C_OBJ = $(RAFT_C_SRC:.c=.o)
RAFT_C_EXEC = raft-c

LIB_SOURCES = src/binary.c	 \
			  src/storage.c	 \
			  src/encoding.c \
			  src/raft.c
LIB_OBJECTS = $(LIB_SOURCES:.c=.o)

all: $(RAFT_C_EXEC)

$(RAFT_C_EXEC): $(RAFT_C_OBJ)
	$(CC) $(CFLAGS) -o $@ $^


libraft.so: $(LIB_OBJECTS)
	$(CC) -shared -o $@ $(LIB_OBJECTS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(RAFT_C_OBJ) $(RAFT_C_EXEC) libraft.so

.PHONY: all clean

