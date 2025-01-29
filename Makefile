CC=gcc
CFLAGS=-Wall -Werror -pedantic -ggdb -std=c2x -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer -pg -Wno-gnu-zero-variadic-macro-arguments

RAFT_C_SRC = raft.c app_server.c
RAFT_C_OBJ = $(RAFT_C_SRC:.c=.o)
RAFT_C_EXEC = raft-c

all: $(RAFT_C_EXEC)

$(RAFT_C_EXEC): $(RAFT_C_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(RAFT_C_EXEC)

.PHONY: all clean

