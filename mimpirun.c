/**
 * This file is for implementation of mimpirun program.
 * */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include "mimpi_common.h"
#include "channel.h"

// Create pipes for each pair of processes
// and set their descriptor numbers.
// pipe for i -> j communication has descriptors
// write (i): from get_pipe_write_fd(i, j, n), something like 2*(100 + i * n + j)
// read (j): from get_pipe_read_fd(i, j, n), something like 2*(100 + i * n + j) + 1
// instead of pipe, use channel.c functions and remember about dup2
// Disclaimer: pipe i -> i is not created.
static void create_channels(int n) {
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) if (i != j) {
            dbg prt("Creating channel from %d to %d\n", i, j);
            int pipefd[2];
            ASSERT_SYS_OK(channel(pipefd));

            int write_fd = pipefd[1];
            int read_fd = pipefd[0];
            dbg prt("read_fd=%d write_fd = %d\n", read_fd, write_fd);

            int write_fd_num = get_pipe_write_fd(i, j, n); // 2 * (100 + i * n + j);
            int read_fd_num = get_pipe_read_fd(i, j, n); // 2 * (100 + i * n + j) + 1;
            dbg prt("read_fd_num=%d write_fd_num = %d\n", read_fd_num, write_fd_num);
            assert(20 <= read_fd_num && read_fd_num <= 1023);
            assert(20 <= write_fd_num && write_fd_num <= 1023);

            ASSERT_SYS_OK(dup2(write_fd, write_fd_num));
            ASSERT_SYS_OK(dup2(read_fd, read_fd_num));

            ASSERT_SYS_OK(close(write_fd));
            ASSERT_SYS_OK(close(read_fd));
        }
    }
}

static void close_unnecessary_channels(int rank, int n) {
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) if (i != j) {
            int write_fd_num = get_pipe_write_fd(i, j, n);
            int read_fd_num = get_pipe_read_fd(i, j, n);
            if (i != rank && j != rank) {
                // Pipe: i -> j and i, j != rank,
                // so process rank doesn't read nor write from/to this pipe
                dbg prt("Closing write_fd_num = %d\n", write_fd_num);
                dbg prt("Closing read_fd_num = %d\n", read_fd_num);
                ASSERT_SYS_OK(close(write_fd_num));
                ASSERT_SYS_OK(close(read_fd_num));
            } else if (i == rank) {
                // Pipe: rank -> j
                // process rank only wants to write, not read
                dbg prt("Closing read_fd_num = %d\n", read_fd_num);
                ASSERT_SYS_OK(close(read_fd_num));
            } else if (j == rank) {
                // Pipe: i -> rank
                // process rank only wants to read, not write
                dbg prt("Closing write_fd_num = %d\n", write_fd_num);
                ASSERT_SYS_OK(close(write_fd_num));
            }
        }
    }
}

static void save_pid_to_rank_in_env(pid_t pid, int rank) {
    char env_var_name[32]; // MIMPI_RANK_[pid]
    get_mimpi_rank_for_pid_envariable_name(env_var_name, pid);

    char rank_str[16];
    ASSERT_SPRINTF_OK(sprintf(rank_str, "%d", rank));

    dbg prt("env_var_name = %s\n", env_var_name);
    dbg prt("rank_str = %s\n", rank_str);

    ASSERT_SYS_OK(setenv(env_var_name, rank_str, true));

    dbg prt("Testing: getenv(\"%s\") = %s\n", env_var_name, getenv(env_var_name));
}

static void save_n_to_env(int n) {
    char n_str[10 + 1];
    ASSERT_SPRINTF_OK(sprintf(n_str, "%d", n));
    ASSERT_SYS_OK(setenv("MIMPI_N", n_str, true));
    dbg prt("Printing saved envariable: getenv(\"MIMPI_N\") = %s\n", getenv("MIMPI_N"));
}

int main(int argc, char** argv) {
    dbg {
        for (int i = 0; i < argc; i++) {
            printf("argv[%d] = %s\n", i, argv[i]);
        }
        fflush(stdout);
    }

    // Get n - number of child processes.
    int n = atoi(argv[1]); dbg prt("n = %d\n", n);
    save_n_to_env(n);

    create_channels(n);
    dbg print_open_descriptors(n);

    for (int i = 0; i < n; i++) {
        pid_t pid; ASSERT_SYS_OK(pid = fork());
        if (pid == 0) {
            // Child: save child's pid to environment variable
            // to make it accessible from the child even *after* calling execvp.
            save_pid_to_rank_in_env(getpid(), i);

            // Close unnecessary channels.
            close_unnecessary_channels(i, n);

            // Execute given program with execvp (this will overwrite the child's code)
            ASSERT_SYS_OK(execvp(argv[2], argv + 2));
            return 0; // so this part is actually unreachable
        }
    }

    // Parent: wait for each child.
    for (int i = 0; i < n; ++i)
        ASSERT_SYS_OK(wait(NULL));

}