CC=gcc
CFLAGS=-Wall -Werror -pedantic -ggdb -std=c11 -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer -pg

RAFT_C_SRC = main.c
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

