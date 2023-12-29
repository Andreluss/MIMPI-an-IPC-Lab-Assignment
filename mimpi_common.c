/**
 * This file is for implementation of common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#include "mimpi_common.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

_Noreturn void syserr(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);
    fprintf(stderr, " (%d; %s)\n", errno, strerror(errno));
    exit(1);
}

_Noreturn void fatal(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);

    fprintf(stderr, "\n");
    exit(1);
}

/////////////////////////////////////////////////
// Put your implementation here
#include <dirent.h>

void print_open_descriptors(int n) {
    const int MAX_PATH_LENGTH = 1024;
    const char *path = "/proc/self/fd";

    // Iterate over all symlinks in `path`.
    // They represent open file descriptors of our process.
    DIR *dr = opendir(path);
    if (dr == NULL)
        fatal("Could not open dir: %s", path);

    struct dirent *entry;
    while ((entry = readdir(dr)) != NULL) {
        if (entry->d_type != DT_LNK)
            continue;

        // Make a c-string with the full path of the entry.
        char subpath[MAX_PATH_LENGTH];
        int ret = snprintf(subpath, sizeof(subpath), "%s/%s", path, entry->d_name);
        if (ret < 0 || ret >= (int) sizeof(subpath))
            fatal("Error in snprintf");

        // Read what the symlink points to.
        char symlink_target[MAX_PATH_LENGTH];
        ssize_t ret2 = readlink(subpath, symlink_target, sizeof(symlink_target) - 1);
        ASSERT_SYS_OK(ret2);
        symlink_target[ret2] = '\0';

        // Skip an additional open descriptor to `path` that we have until closedir().
        if (strncmp(symlink_target, "/proc", 5) == 0)
            continue;

        int desc_num = atoi(entry->d_name);
        if (20 <= desc_num && desc_num <= 1023) {
            int i = (desc_num / 2 - 100) / n;
            int j = (desc_num / 2 - 100) % n;
            char *type = (desc_num % 2 == 0) ? "write" : "read";
            fprintf(stderr, "Pid %d file descriptor %3d -> %s (%d -> %d, %s)\n",
                    getpid(), desc_num, symlink_target, i, j, type);
        }
        else {
            fprintf(stderr, "Pid %d file descriptor %3s -> %s\n",
                    getpid(), entry->d_name, symlink_target);
        }
    }
    closedir(dr);
}

// Return fd for the (i-th process') writing end of the i -> j pipe.
int get_pipe_write_fd(int i, int j, int n) {
    return 2 * (100 + i * n + j);
}

// Return fd for the (j-th process') reading end of the i -> j pipe.
int get_pipe_read_fd(int i, int j, int n) {
    return 2 * (100 + i * n + j) + 1;
}

void get_mimpi_rank_for_pid_envariable_name(char *buf, int pid) {
    ASSERT_SPRINTF_OK(sprintf(buf, "MIMPI_RANK_%d", pid)); // TODO error check?
}

