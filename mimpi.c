/**
 * This file is for implementation of MIMPI library.
 * */

#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <memory.h>
#include <bits/stdint-uintn.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

// These tags should have distinct values:
static const int MIMPI_BARRIER_WAIT_TAG = -1;
static const int MIMPI_BARRIER_NOTIFY_TAG = -2;
static const int MIMPI_BCAST_WAIT_TAG = -3;
static const int MIMPI_BCAST_NOTIFY_TAG = -4;
static const int MIMPI_REDUCE_NOTIFY_TAG = -5;
static const int MIMPI_REDUCE_WAIT_PROD_TAG = -10;
static const int MIMPI_REDUCE_WAIT_SUM_TAG = -11;
static const int MIMPI_REDUCE_WAIT_MIN_TAG = -12;
static const int MIMPI_REDUCE_WAIT_MAX_TAG = -13;

int get_reduce_wait_tag(MIMPI_Op op) {
    switch (op) {
        case MIMPI_PROD: return MIMPI_REDUCE_WAIT_PROD_TAG;
        case MIMPI_SUM: return MIMPI_REDUCE_WAIT_SUM_TAG;
        case MIMPI_MIN: return MIMPI_REDUCE_WAIT_MIN_TAG;
        case MIMPI_MAX: return MIMPI_REDUCE_WAIT_MAX_TAG;
        default: assert(false);
    }
}

uint8_t perform_operation(MIMPI_Op op, uint8_t a, uint8_t b) {
    switch (op) {
        case MIMPI_PROD: return a * b;
        case MIMPI_SUM: return a + b;
        case MIMPI_MIN: return a < b ? a : b;
        case MIMPI_MAX: return a > b ? a : b;
        default: assert(false);
    }
}

typedef struct {
    int count;
    int source;
    int tag;
} mimpi_metadata_t;

typedef struct {
    mimpi_metadata_t metadata;
    void* data;
} received_message_t;
static void received_message_destroy(received_message_t* message) {
    if (message == NULL) return;
    free(message->data);
    free(message);
}
static void received_message_init(received_message_t* message, mimpi_metadata_t metadata, void* data) {
    if (message == NULL) return;
    message->metadata = metadata;
    message->data = data;
}

struct received_messages_node_t {
    received_message_t* message;
    struct received_messages_node_t* next;
};
typedef struct received_messages_node_t received_messages_node_t;

static received_messages_node_t* received_messages_list_head = NULL;
static received_messages_node_t* received_messages_list_tail = NULL;

/// @brief Thread function that continuously reads from pipe _source -> mimpi.rank.
static void* receiver_thread(void* _source);

enum mimpi_state_t {
    MIMPI_STATE_RUN,
    MIMPI_STATE_WAIT,
};
typedef enum mimpi_state_t mimpi_state_t;

static struct {
    bool enable_deadlock_detection;
    int n;
    int rank;

    // This array contains helper threads that continuously read from 'input' channels (pipes).
    pthread_t* receiver_threads;

    // <list of messages received from other processes is defined above>

    // This mutex is used to synchronize the main thread and the receiver threads
    // when accessing commonly used data from here.
    pthread_mutex_t mutex;

    bool is_waiting_on_semaphore;
    int group_synced_count;
    // This semaphore is used to make the MIMPI_Recv wait until the message arrives.
    sem_t semaphore; // destroy in MIMPI_Finalize()
    int recv_source;
    int recv_tag;
    int recv_count;
    received_message_t* received_message;
    mimpi_state_t state;
    int open_channels;
    bool* dead; // dead[i] = true iff process with rank i has already escaped MPI block.
} mimpi;

static void mimpi_init(int n, int rank, bool enable_deadlock_detection) {
    mimpi.n = n;
    mimpi.rank = rank;
    mimpi.enable_deadlock_detection = enable_deadlock_detection;
    ASSERT_ZERO(pthread_mutex_init(&mimpi.mutex, NULL));
    ASSERT_ZERO(sem_init(&mimpi.semaphore, 0, 0)); // semaphore shared between processes
    mimpi.received_message = NULL;
    mimpi.receiver_threads = malloc(mimpi.n * sizeof(pthread_t));
    mimpi.is_waiting_on_semaphore = false;
    mimpi.group_synced_count = 0;
    mimpi.state = MIMPI_STATE_RUN;

    mimpi.open_channels = mimpi.n - 1;
    mimpi.dead = calloc(mimpi.n, sizeof(bool)); // all false

    for (int source_rank = 0; source_rank < mimpi.n; source_rank++) {
        if (source_rank == mimpi.rank)
            continue;

        int* thread_arg = malloc(sizeof(int));
        *thread_arg = source_rank;

        ASSERT_ZERO(pthread_create(&mimpi.receiver_threads[source_rank], NULL, receiver_thread, thread_arg));
    }
}
static void mimpi_destroy() {
    free(mimpi.receiver_threads);
    ASSERT_ZERO(pthread_mutex_destroy(&mimpi.mutex));
    ASSERT_ZERO(sem_destroy(&mimpi.semaphore));
    received_message_destroy(mimpi.received_message);

    // Destroy the list
    for (received_messages_node_t* curr = received_messages_list_head; curr != NULL;) {
        received_messages_node_t* next = curr->next;
        received_message_destroy(curr->message);
        free(curr);
        curr = next;
    }
    free(mimpi.dead);
}

/// @brief Returns true if the given metadata matches the source, tag and count (tag can be ANY_TAG).
static bool metadata_matches_params(mimpi_metadata_t *metadata, int source, int tag, int count) {
    return metadata &&
           metadata->count == count && metadata->source == source &&
           (metadata->tag == tag || tag == MIMPI_ANY_TAG);
}

/// @brief Pushes a message to the back of the list.
/// @param message The message to be pushed.
/// Notice: the function take the ownership of the message pointer.
static void message_list_push(received_message_t* message) {
    received_messages_node_t* new_node = malloc(sizeof(received_messages_node_t));
    new_node->message = message;
    new_node->next = NULL;
    if (received_messages_list_head == NULL) {
        received_messages_list_head = new_node;
    } else {
        received_messages_list_tail->next = new_node;
    }
    received_messages_list_tail = new_node;
}

/// @brief Finds a message in the list that matches the given parameters and pops it from the list.
/// Notice: the function returns the ownership of the message pointer to the caller.
/// @return The message that matches the given parameters, or NULL if no such message exists.
static received_message_t* message_list_find_and_pop(int count, int source, int tag) {
    received_messages_node_t* prev = NULL;
    received_messages_node_t* curr = received_messages_list_head;
    while (curr != NULL) {
        if (metadata_matches_params(&curr->message->metadata, source, tag, count)) {
            // Found the message.
            if (prev == NULL) {
                // The message is at the head of the list.
                received_messages_list_head = curr->next;
            } else {
                // The message is in the middle of the list.
                prev->next = curr->next;
            }
            if (curr == received_messages_list_tail) {
                // The message is at the tail of the list.
                received_messages_list_tail = prev;
            }
            received_message_t* message = curr->message;
            free(curr);
            return message;
        }
        prev = curr;
        curr = curr->next;
    }
    return NULL;
}

/// @brief Sends all @ref bytes_to_write bytes of @ref data to the file descriptor @ref fd.
/// @return MIMPI return code:
///         - `MIMPI_SUCCESS` if operation ended successfully.
///         - `MIMPI_ERROR_REMOTE_FINISHED` if the process with rank
///           @ref destination has already escaped _MPI block_ (closed the pipe).
static MIMPI_Retcode complete_chsend(int fd, const void *data, size_t bytes_to_write);

/// @brief Receives all @ref bytes_to_read bytes of data from the file descriptor @ref fd.
/// Warning: this function assumes that @ref data IS ALREADY ALLOCATED to hold @ref bytes_to_read bytes.
/// @return MIMPI return code:
///         - `MIMPI_SUCCESS` if operation ended successfully.
///         - `MIMPI_ERROR_REMOTE_FINISHED` if the process with rank
///           @ref source has already escaped _MPI block_ (closed the pipe).
static MIMPI_Retcode complete_chrecv(int fd, void* data, size_t bytes_to_read);

static void set_wait_state(int source, int tag, int count);

static void set_run_state_with_message(received_message_t *msg);

void clear_wait_state();

int get_parent(int msg_root);

int get_left_child(int msg_root);

int get_right_child(int msg_root);

static void set_received_message(received_message_t *message);

static MIMPI_Retcode complete_chsend(int fd, const void *data, size_t bytes_to_write) {
    while (bytes_to_write > 0) {
        int bytes_sent = chsend(fd, data, bytes_to_write);
        dbg {
            char fd_description[32];
            get_pipe_fd_to_string(fd, mimpi.n, fd_description, sizeof(fd_description));
            dbg prt ("Pid %d wrote %d bytes to write_fd %d (%s)\n",
                     getpid(), bytes_sent, fd, fd_description);
        }

        // Case 1: There was an error (probably pipe was closed for reading).
        if (bytes_sent == -1) {
            dbg prt("////////// Pid %d had error writing to fd %d\n", getpid(), fd);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // Case 2: Some data was written.
        bytes_to_write -= bytes_sent; assert(bytes_sent != 0);
        data += bytes_sent;
    }
    return MIMPI_SUCCESS;
}

static MIMPI_Retcode complete_chrecv(int fd, void* data, size_t bytes_to_read) {
    assert(bytes_to_read > 0);
    while (bytes_to_read > 0) {
        int bytes_read = chrecv(fd, data, bytes_to_read);
        dbg {
            char fd_description[32];
            get_pipe_fd_to_string(fd, mimpi.n, fd_description, sizeof(fd_description));
            dbg prt ("Pid %d read %d bytes from read_fd %d (%s)\n",
                     getpid(), bytes_read, fd, fd_description);
        }

        // Case 1: There was an error with the pipe (-1)
        // or the pipe has been closed for writing (0).
        if (bytes_read <= 0) {
            dbg prt("////////// Pid %d had error reading from fd %d\n", getpid(), fd);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // Case 2: Some data was read.
        bytes_to_read -= bytes_read;
        data += bytes_read;
    }

    return MIMPI_SUCCESS;
}

static void merge_data_inplace(void* main_data,
                               void* other_data,
                               int count, MIMPI_Op op) {
    assert(main_data != NULL);
    assert(other_data != NULL);
    for (int i = 0; i < count; i++) {
        uint8_t main_value = ((uint8_t*) main_data)[i];
        uint8_t other_value = ((uint8_t*) other_data)[i];
        ((uint8_t*) main_data)[i] = perform_operation(op, main_value, other_value);
    }
}

received_message_t* try_read_message(int read_fd) {
    mimpi_metadata_t metadata;
    MIMPI_Retcode metadata_read_result = complete_chrecv(read_fd, &metadata, sizeof(mimpi_metadata_t));
    if (metadata_read_result != MIMPI_SUCCESS) return NULL;

    void* data = NULL;
    if (metadata.count > 0) {
        data = malloc(metadata.count);
        MIMPI_Retcode data_read_result = complete_chrecv(read_fd, data, metadata.count);
        if (data_read_result != MIMPI_SUCCESS) {
            free(data);
            return NULL;
        }
    }

    received_message_t* message = malloc(sizeof(received_message_t));
    received_message_init(message, metadata, data);
    return message;
}

void clear_wait_state() { // DON't use
//    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
    mimpi.state = MIMPI_STATE_RUN;
    mimpi.is_waiting_on_semaphore = false;
//    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
}

void set_wait_state(int source, int tag, int count) {
//    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
    mimpi.state = MIMPI_STATE_WAIT;
    received_message_destroy(mimpi.received_message);
    mimpi.received_message = NULL;
//    mimpi.is_waiting_on_semaphore = true;
    mimpi.recv_source = source;
    mimpi.recv_tag = tag; // possibly <= 0
    mimpi.recv_count = count;
//    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
}

static void set_run_state_with_message(received_message_t *msg) {
    mimpi.state = MIMPI_STATE_RUN;
    set_received_message(msg);
}

/// @brief Thread function that continuously reads from pipe _source -> mimpi.rank.
static void* receiver_thread(void* _source) {
    int source_rank = *((int*) _source); free(_source);
    int read_fd = get_pipe_read_fd(source_rank, mimpi.rank, mimpi.n);

    while (true) {
        received_message_t* new_msg = try_read_message(read_fd);
        if (new_msg == NULL) {
            d3g prt("Rank %d: READ ERROR from %d\n", mimpi.rank, source_rank);
            break;
        }
        d3g prt("Rank %d: received new_msg (%d, %d, %d)\n", mimpi.rank, new_msg->metadata.count, new_msg->metadata.source, new_msg->metadata.tag);

        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        bool matches_wait_params = metadata_matches_params(&new_msg->metadata,
                                                           mimpi.recv_source,
                                                           mimpi.recv_tag,
                                                           mimpi.recv_count);
        if (mimpi.state == MIMPI_STATE_WAIT && matches_wait_params) {
            d3g prt("Rank %d: new_msg (%d, %d, %d) matches wait params (%d, %d, %d)\n", mimpi.rank,
                   new_msg->metadata.count, new_msg->metadata.source, new_msg->metadata.tag,
                   mimpi.recv_count, mimpi.recv_source, mimpi.recv_tag);
            set_run_state_with_message(new_msg);
            ASSERT_ZERO(sem_post(&mimpi.semaphore));
        }
        else {
            d3g prt("Rank %d: new_msg (%d, %d, %d) doesn't match wait params (%d, %d, %d)\n", mimpi.rank,
                   new_msg->metadata.count, new_msg->metadata.source, new_msg->metadata.tag,
                   mimpi.recv_count, mimpi.recv_source, mimpi.recv_tag);
            message_list_push(new_msg);
        }
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
    }

    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
    mimpi.dead[source_rank] = true; // this info will prevent *future* MIMPI_Recv from waiting for source_rank.
    if (mimpi.state == MIMPI_STATE_WAIT && mimpi.recv_source == source_rank) {
        d3g prt("Rank %d: ERROR [REMOTE_FINISHED] after wait for msg from %d\n", mimpi.rank, source_rank);
        set_run_state_with_message(NULL);
        ASSERT_ZERO(sem_post(&mimpi.semaphore));
    }
    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

    return NULL;
}

static void set_received_message(received_message_t *message) {
    d2g prt("Setting received message (rank %d)\n", mimpi.rank);
    if (mimpi.received_message != NULL) {
        received_message_destroy(mimpi.received_message);
    }
    mimpi.received_message = message;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    d3g prt("Rank %d: *started* initializing\n", mimpi.rank);
    channels_init();

    // (1) Get world size from env
    assert(getenv("MIMPI_N") != NULL);
    int n = atoi(getenv("MIMPI_N"));
    d3g prt("Rank %d: n = %d\n", mimpi.rank, n);

    // (2) Get world rank from env
    pid_t pid = getpid();
    char envariable_name[32];
    get_mimpi_rank_for_pid_envariable_name(envariable_name, pid);
    assert(getenv(envariable_name) != NULL);
    int rank = atoi(getenv(envariable_name));
    d3g prt("Rank %d sTIll HERE pid=%d\n", rank, getpid());

    // (3) Create helper threads and structures.
    mimpi_init(n, rank, enable_deadlock_detection);

    d3g prt("Rank %d: successfully INITIALIZED\n", mimpi.rank);
}

void MIMPI_Finalize() {
    d2g prt("Rank %d: started finalizing\n", mimpi.rank);
    // MAYBE_TODO send message informing of death?

    // (1) Close all channels (pipes) related to this process (this should stop the receiver threads).
    for (int i = 0; i < mimpi.n; i++) {
        if (i == mimpi.rank)
            continue;
        int read_fd = get_pipe_read_fd(i, mimpi.rank, mimpi.n);
        int write_fd = get_pipe_write_fd(mimpi.rank, i, mimpi.n);
        ASSERT_ZERO(close(read_fd));
        ASSERT_ZERO(close(write_fd));
    }
    d2g {
        prt("Process with rank %d closed all channels\n", mimpi.rank);
//        print_open_descriptors(mimpi.n);
    };

    // (2) Now wait for the helper threads to finish
    // (they should exit, because the pipes are closed).
    for (int i = 0; i < mimpi.n; i++) {
        if (i == mimpi.rank)
            continue;
        d2g prt("........ Rank %d joining thread rf %d ...\n", mimpi.rank, i);
        ASSERT_ZERO(pthread_join(mimpi.receiver_threads[i], NULL));
        d2g prt("........ Rank %d joined thread rf %d\n", mimpi.rank, i);
    }

    // (3) Free all memory allocated in MIMPI_Init().
    mimpi_destroy();
    channels_finalize();
    d2g prt("Rank %d: successfully FINALIZED\n", mimpi.rank);
}

int MIMPI_World_size() {
    return mimpi.n;
}

int MIMPI_World_rank() {
    return mimpi.rank;
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    if (destination == mimpi.rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (destination < 0 || destination >= mimpi.n) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    // Get write fd for the pipe mimpi.rank -> destination.
    int write_fd = get_pipe_write_fd(mimpi.rank, destination, mimpi.n);

    // (1) Send a metadata message to the destination.
    // The message will contain the size of the data to be sent and the tag.
    mimpi_metadata_t metadata = {
        .count = count,
        .source = mimpi.rank,
        .tag = tag
    };
    MIMPI_Retcode metadata_write_result = complete_chsend(write_fd, &metadata, sizeof(metadata));
    if (metadata_write_result != MIMPI_SUCCESS) {
        return metadata_write_result;
    }

    // (2) Send data to destination (until all data is written or error occurs).
    int data_write_result = complete_chsend(write_fd, data, count);
    if (data_write_result != MIMPI_SUCCESS) {
        return data_write_result;
    }

    // in *mutex* update sent messages list TODO

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    if (source == mimpi.rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (source < 0 || source >= mimpi.n) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    // (1) Check if the message is already in the list of received messages.
    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
    received_message_t* message = message_list_find_and_pop(count, source, tag);
    if (message) {
        // If the message is already in the list, then we can return it.
        memcpy(data, message->data, count);
        received_message_destroy(message);
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
        return MIMPI_SUCCESS;
    }
    else {
        // Otherwise we should wait for the message to arrive.
        // If source has already escaped MPI block, then we should return an error.
        if (mimpi.dead[source]) {
            dbg prt("Rank %d: Message (%d, src: %d, %d) not received [REMOTE_FINISHED] because source is DEAD.\n", mimpi.rank, count, source, tag);
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        set_wait_state(source, tag, count);
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        // this part is blocking until the message arrives (or an error occurs):
        // -----------------------------------------------------------------
        ASSERT_ZERO(sem_wait(&mimpi.semaphore));    // FUTURE-TODO Monitor.GetMessage(count, source, tag);
        // -----------------------------------------------------------------

        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        assert(mimpi.state == MIMPI_STATE_RUN);
        // Move the received message to local variable.
        message = mimpi.received_message; mimpi.received_message = NULL;
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        // Case 1: An error occurred.
        if (message == NULL) {
            d3g prt("Rank %d: Message (%d, src: %d, %d) was not received [REMOTE_FINISHED].\n", mimpi.rank, count, source, tag);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // Case 2: The message arrived.
        d3g prt("Rank %d: Message (%d, src: %d, %d) was received.\n", mimpi.rank, count, source, tag);
        memcpy(data, message->data, count);
        received_message_destroy(message);

        return MIMPI_SUCCESS;
    }
}


int get_right_child(int msg_root) {
    if (msg_root == 0)
        return mimpi.rank * 2 + 2;

    // create a tree with nodes 0 and msg_root swapped
    int nodes[16] = {0}; // MAX_N = 16
    for (int i = 0; i < mimpi.n; i++) nodes[i] = i;
    nodes[0] = msg_root; nodes[msg_root] = 0;

    // find index of mimpi.rank in tree
    int idx = 0; while (nodes[idx] != mimpi.rank) idx++;

    // take the right child (if exists)
    int right_child_idx = idx * 2 + 2;
    if (right_child_idx >= mimpi.n) return mimpi.n + 44;
    return nodes[right_child_idx];
}

int get_left_child(int msg_root) {
    if (msg_root == 0)
        return mimpi.rank * 2 + 1;

    // create a tree with nodes 0 and msg_root swapped
    int nodes[16] = {0}; // MAX_N = 16
    for (int i = 0; i < mimpi.n; i++) nodes[i] = i;
    nodes[0] = msg_root; nodes[msg_root] = 0;

    // find index of mimpi.rank in tree
    int idx = 0; while (nodes[idx] != mimpi.rank) idx++;

    // take the left child (if exists)
    int left_child_idx = idx * 2 + 1;
    if (left_child_idx >= mimpi.n) return mimpi.n + 42;
    return nodes[left_child_idx];
}

int get_parent(int msg_root) {
    if (mimpi.rank == msg_root) return -1;
    if (msg_root == 0)
        return (mimpi.rank - 1) / 2;

    int nodes[16] = {0}; // MAX_N = 16
    for (int i = 0; i < mimpi.n; i++) nodes[i] = i;
    nodes[0] = msg_root;
    nodes[msg_root] = 0;

    int idx = 0; while (nodes[idx] != mimpi.rank) idx++;
    if (idx == 0) {
//        d3g prt("+++++++Called parent on root %d rank %d", msg_root, mimpi.rank);
        return -1;
    }

    return nodes[(idx - 1) / 2];
}

bool is_ancestor(int msg_root, int other_rank) {
    int nodes[16] = {0}; // MAX_N = 16
    for (int i = 0; i < mimpi.n; i++) nodes[i] = i;
    nodes[0] = msg_root;
    nodes[msg_root] = 0;

    int idx_other = 0; while (nodes[idx_other] != other_rank) idx_other++;

    // go through all the ancestors of other_rank and check if any of them is mimpi.rank
    while(idx_other != 0 && nodes[idx_other] != mimpi.rank) {
        idx_other = (idx_other - 1) / 2;
    }

    return nodes[idx_other] == mimpi.rank;
}

bool exists(int rank) {
    return rank >= 0 && rank < mimpi.n;
}

MIMPI_Retcode MIMPI_Barrier() {
    if (mimpi.n == 1) return MIMPI_SUCCESS;

    // Wait for BARRIER_WAIT message from children.
    if (exists(get_left_child(0))) {
        MIMPI_Retcode ret = MIMPI_Recv(NULL, 0, get_left_child(0), MIMPI_BARRIER_WAIT_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }
    if (exists(get_right_child(0))) {
        MIMPI_Retcode ret = MIMPI_Recv(NULL, 0, get_right_child(0), MIMPI_BARRIER_WAIT_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }

    // Send BARRIER_WAIT and wait for BARRIER_NOTIFY from parent (if there is one).
    if (exists(get_parent(0))) {
        MIMPI_Retcode ret = MIMPI_Send(NULL, 0, get_parent(0), MIMPI_BARRIER_WAIT_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
        MIMPI_Retcode ret2 = MIMPI_Recv(NULL, 0, get_parent(0), MIMPI_BARRIER_NOTIFY_TAG);
        if (ret2 != MIMPI_SUCCESS) return ret2;
    }

    // Send BARRIER_NOTIFY to children (if any).
    if (exists(get_left_child(0))) {
        MIMPI_Retcode ret = MIMPI_Send(NULL, 0, get_left_child(0), MIMPI_BARRIER_NOTIFY_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }
    if (exists(get_right_child(0))) {
        MIMPI_Retcode ret = MIMPI_Send(NULL, 0, get_right_child(0), MIMPI_BARRIER_NOTIFY_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    if (root < 0 || root >= mimpi.n) return MIMPI_ERROR_NO_SUCH_RANK;
    if (mimpi.n == 1) return MIMPI_SUCCESS;

    // Wait for BCAST_WAIT message from children (if any).
    if (exists(get_left_child(root))) {
        MIMPI_Retcode ret = MIMPI_Recv(NULL, 0, get_left_child(root), MIMPI_BCAST_WAIT_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }
    if (exists(get_right_child(root))) {
        MIMPI_Retcode ret = MIMPI_Recv(NULL, 0, get_right_child(root), MIMPI_BCAST_WAIT_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }

    // Send BCAST_WAIT and wait for BCAST_NOTIFY + the message with count bytes from parent (if there is one).
    if (exists(get_parent(root))) {
        MIMPI_Retcode ret = MIMPI_Send(NULL, 0, get_parent(root), MIMPI_BCAST_WAIT_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
        MIMPI_Retcode ret2 = MIMPI_Recv(data, count, get_parent(root), MIMPI_BCAST_NOTIFY_TAG);
        if (ret2 != MIMPI_SUCCESS) return ret2;
    }

    // Send BCAST_NOTIFY to children (if any).
    if (exists(get_left_child(root))) {
        MIMPI_Retcode ret = MIMPI_Send(data, count, get_left_child(root), MIMPI_BCAST_NOTIFY_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }
    if (exists(get_right_child(root))) {
        MIMPI_Retcode ret = MIMPI_Send(data, count, get_right_child(root), MIMPI_BCAST_NOTIFY_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *reccv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    if (root < 0 || root >= mimpi.n) return MIMPI_ERROR_NO_SUCH_RANK;
    if (mimpi.n == 1) {
        memcpy(reccv_data, send_data, count);
        return MIMPI_SUCCESS;
    }

    int reduce_wait_tag = get_reduce_wait_tag(op);
    void* merged_data = malloc(count); // for merging data from this node and children
    memcpy(merged_data, send_data, count);
    void* tmp_data = malloc(count); // for reading data from children

    // Wait for REDUCE_WAIT message + data of size count from children (if any).
    if (exists(get_left_child(root))) {
        MIMPI_Retcode ret = MIMPI_Recv(tmp_data, count, get_left_child(root), reduce_wait_tag);
        if (ret != MIMPI_SUCCESS) {
            free(merged_data);
            free(tmp_data);
            return ret;
        }
        merge_data_inplace(merged_data, tmp_data, count, op);
    }
    if (exists(get_right_child(root))) {
        MIMPI_Retcode ret = MIMPI_Recv(tmp_data, count, get_right_child(root), reduce_wait_tag);
        if (ret != MIMPI_SUCCESS) {
            free(merged_data);
            free(tmp_data);
            return ret;
        }
        merge_data_inplace(merged_data, tmp_data, count, op);
    }
    free(tmp_data);
    d3g prt("Rank %d: after merge_data_inplace\n", mimpi.rank);

    // Send REDUCE_WAIT + merged data and wait for REDUCE_NOTIFY from parent (if there is one).
    if (exists(get_parent(root))) {
        MIMPI_Retcode ret = MIMPI_Send(merged_data, count, get_parent(root), reduce_wait_tag);
        if (ret != MIMPI_SUCCESS) {
            free(merged_data);
            return ret;
        }

        MIMPI_Retcode ret2 = MIMPI_Recv(NULL, 0, get_parent(root), MIMPI_REDUCE_NOTIFY_TAG);
        if (ret2 != MIMPI_SUCCESS) {
            free(merged_data);
            return ret2;
        }
    }
    else {
        d3g prt("Rank %d: root received reduce data\n", mimpi.rank);
        memcpy(reccv_data, merged_data, count); // Copy result to reccv_data only in the root!
    }
    free(merged_data);

    // Send REDUCE_NOTIFY to children (if any).
    if (exists(get_left_child(root))) {
        MIMPI_Retcode ret = MIMPI_Send(NULL, 0, get_left_child(root), MIMPI_REDUCE_NOTIFY_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }
    if (exists(get_right_child(root))) {
        MIMPI_Retcode ret = MIMPI_Send(NULL, 0, get_right_child(root), MIMPI_REDUCE_NOTIFY_TAG);
        if (ret != MIMPI_SUCCESS) return ret;
    }

    return MIMPI_SUCCESS;
}





























