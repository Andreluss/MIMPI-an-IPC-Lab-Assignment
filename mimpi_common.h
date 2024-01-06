/**
 * This file is for declarations of  common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#ifndef MIMPI_COMMON_H
#define MIMPI_COMMON_H

#include <assert.h>
#include <stdbool.h>
#include <stdnoreturn.h>

/*
    Assert that expression doesn't evaluate to -1 (as almost every system function does in case of error).

    Use as a function, with a semicolon, like: ASSERT_SYS_OK(close(fd));
    (This is implemented with a 'do { ... } while(0)' block so that it can be used between if () and else.)
*/
#define ASSERT_SYS_OK(expr)                                                                \
    do {                                                                                   \
        if ((expr) == -1)                                                                  \
            syserr(                                                                        \
                "system command failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ", \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Assert that expression evaluates to zero (otherwise use result as error number, as in pthreads). */
#define ASSERT_ZERO(expr)                                                                  \
    do {                                                                                   \
        int const _errno = (expr);                                                         \
        if (_errno != 0)                                                                   \
            syserr(                                                                        \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",                \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Prints with information about system error (errno) and quits. */
_Noreturn extern void syserr(const char* fmt, ...);

/* Prints (like printf) and quits. */
_Noreturn extern void fatal(const char* fmt, ...);

#define TODO fatal("UNIMPLEMENTED function %s", __PRETTY_FUNCTION__);


/////////////////////////////////////////////
// Put your declarations here

/* -------------- Helper functions ---------------- */
int get_pipe_write_fd(int i, int j, int n);
int get_pipe_read_fd(int i, int j, int n);
void get_mimpi_rank_for_pid_envariable_name(char *buf, int pid);

/* ------------------ Helper macros ----------------- */

#define ASSERT_SPRINTF_OK(expr)                                                             \
    do {                                                                                   \
        int const _res = (expr);                                                            \
        if (_res < 0)                                                                       \
            syserr(                                                                        \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",                \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)




/* -------------- Debug functions ---------------- */

// Print (to stderr) information about all open descriptors in current process.
void print_open_descriptors(int);
void get_pipe_fd_to_string(int fd, int n, char *buf, int buf_size);


/* ------------------ Debug macros ----------------- */
// Different levels of debug:
#define dbg if (false)
#define d2g if (false)
#define d3g if (false)

#include <stdio.h>
#define prt(...) \
    do { \
        char const *colors[] = {"\033[31m",  "\033[32m", "\033[33m", "\033[34m", "\033[35m", "\033[36m"}; \
        int const colors_len = sizeof(colors) / sizeof(colors[0]);        \
        int const color = rand() % colors_len; \
        fprintf(stderr, "[%s][%d]: %s", __FILE__, __LINE__, colors[color]);         \
        fprintf(stderr, __VA_ARGS__);                                     \
        fprintf(stderr, "\033[0m"); \
        fflush(stderr); \
    } while (0)

/*
#define prt2(...) \
    do { \
        char const *colors[] = {RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN}; \
        int const colors_len = sizeof(colors) / sizeof(colors[0]);        \
        int const color = rand() % colors_len; \
        fprintf(stderr, "%s", colors[color]);                             \
        fprintf(stderr, "[debug]: ");         \
        fprintf(stderr, RESET); \
        fprintf(stderr, __VA_ARGS__);                                     \
        fflush(stderr); \
    } while (0)

#define prt3(...) \
    do { \
        char const *colors[] = {RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN}; \
        int const colors_len = sizeof(colors) / sizeof(colors[0]);        \
        int const color = rand() % colors_len;                            \
        fprintf(stderr, "%s", colors[color]);                             \
        fprintf(stderr, __VA_ARGS__);                                     \
        fprintf(stderr, RESET);                                           \
        fflush(stderr); \
    } while (0)
*/

#endif // MIMPI_COMMON_H