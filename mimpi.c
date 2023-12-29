/**
 * This file is for implementation of MIMPI library.
 * */

#include <stdlib.h>
#include <unistd.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

static struct {
    bool enable_deadlock_detection;
    int n;
    int rank;
} mimpi;

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    mimpi.enable_deadlock_detection = enable_deadlock_detection; // TODO

    // Get world size from env
    assert(getenv("MIMPI_N") != NULL);
    mimpi.n = atoi(getenv("MIMPI_N"));
    dbg prt("Pid %d has mimpi.n = %d\n", getpid(), mimpi.n);

    // Get world rank from env
    pid_t pid = getpid();
    char envariable_name[32];
    get_mimpi_rank_for_pid_envariable_name(envariable_name, pid);
    assert(getenv(envariable_name) != NULL);
    mimpi.rank = atoi(getenv(envariable_name));
    dbg prt("Pid %d has mimpi.rank = %d\n", getpid(), mimpi.rank);
    dbg print_open_descriptors(mimpi.n);

    TODO
}

void MIMPI_Finalize() {
    TODO

    channels_finalize();
}

int MIMPI_World_size() {
    TODO
}

int MIMPI_World_rank() {
    TODO
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}