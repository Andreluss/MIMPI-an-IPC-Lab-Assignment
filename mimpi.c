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
static const int MIMPI_DEADLOCK_DETECTED_TAG = -42;

static const int DEADLOCK_MANAGER = -99;

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
} message_t;
static void message_destroy_and_free(message_t* message) {
    if (message == NULL) return;
    free(message->data);
    free(message);
}
static void message_init(message_t* message, mimpi_metadata_t metadata, void* data) {
    if (message == NULL) return;
    message->metadata = metadata;
    message->data = data;
}

// ---------------------------- message list interface ----------------------------------------------
typedef struct message_list_node message_list_node;
struct message_list_node {
    message_t* message;
    message_list_node* next;
};

typedef struct message_list message_list;
struct message_list {
    message_list_node* head;
    message_list_node* tail;
};
static void message_list_init(message_list* list) {
    list->head = NULL;
    list->tail = NULL;
}
static void message_list_destroy(message_list* list) {
    for (message_list_node* curr = list->head; curr != NULL;) {
        message_list_node* next = curr->next;
        free(curr->message);
        free(curr);
        curr = next;
    }
    list->head = list->tail = NULL;
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
static void message_list_push(message_list* list, message_t* message) {
    message_list_node* new_node = malloc(sizeof(message_list_node));
    new_node->message = message;
    new_node->next = NULL;
    if (list->head == NULL) {
        list->head = new_node;
    } else {
        list->tail->next = new_node;
    }
    list->tail = new_node;
}

/// @brief Finds a message in the list that matches the given parameters and pops it from the list.
/// Notice: the function returns the ownership of the message pointer to the caller.
/// @return The message that matches the given parameters, or NULL if no such message exists.
static message_t* message_list_find_and_pop(message_list* list, int count, int source, int tag) {
    message_list_node* prev = NULL;
    message_list_node* curr = list->head;
    while (curr != NULL) {
        if (metadata_matches_params(&curr->message->metadata, source, tag, count)) {
            // Found the message.
            if (prev == NULL) {
                // The message is at the head of the list.
                list->head = curr->next;
            } else {
                // The message is in the middle of the list.
                prev->next = curr->next;
            }
            if (curr == list->tail) {
                // The message is at the tail of the list.
                list->tail = prev;
            }
            message_t* message = curr->message;
            free(curr);
            return message;
        }
        prev = curr;
        curr = curr->next;
    }
    return NULL;
}
// ----------------------------------------------------------------------------------------------

/// @brief Thread function that continuously reads from pipe _source -> mimpi.rank.
static void* receiver_thread(void* _source);

typedef enum mimpi_state_t mimpi_state_t;
enum mimpi_state_t {
    MIMPI_STATE_RUN,
    MIMPI_STATE_WAIT,
};

static struct {
    bool deadlock_detection;
    int n;
    int rank;

    // This array contains helper threads that continuously read from 'input' channels (pipes).
    pthread_t* receiver_threads;

    // list of messages received from other processes is defined above
    message_list ml; // ml for message list :P

    // This mutex is used to synchronize the main thread and the receiver threads
    // when accessing commonly used data from here.
    pthread_mutex_t mutex;

    // This semaphore is used to make the MIMPI_Recv wait until the message arrives.
    sem_t semaphore; // destroy in MIMPI_Finalize()
    int recv_source;
    int recv_tag;
    int recv_count;
    message_t* received_message;
    mimpi_state_t state;
    bool* dead; // dead[i] = true iff process with rank i has already escaped MPI block.

    // ----- deadlock detection -----
    // for each other process create a list of messages sent to it
    message_list* sent_messages; // sent_messages[i] = list of messages sent to process with rank i (default: empty list)
    message_t** requested_message; // requested_message[i] = message currently requested by other process with rank i (default: NULL)
    // and message currently requested by it (default: NULL)

    // ------------------------------

} mimpi;

static void mimpi_init(int n, int rank, bool enable_deadlock_detection) {
    mimpi.n = n;
    mimpi.rank = rank;
    mimpi.deadlock_detection = enable_deadlock_detection;
    ASSERT_ZERO(pthread_mutex_init(&mimpi.mutex, NULL));
    ASSERT_ZERO(sem_init(&mimpi.semaphore, 0, 0)); // semaphore shared between processes
    mimpi.received_message = NULL;
    mimpi.receiver_threads = malloc(mimpi.n * sizeof(pthread_t));
    mimpi.state = MIMPI_STATE_RUN;

    message_list_init(&mimpi.ml);

    mimpi.dead = calloc(mimpi.n, sizeof(bool)); // all false

    if (mimpi.deadlock_detection) {
        // for each other process create a list of messages sent to it
        // and message currently requested by it (default: NULL)
        mimpi.sent_messages = malloc(mimpi.n * sizeof(message_list));
        mimpi.requested_message = malloc(mimpi.n * sizeof(message_t*));

        for (int i = 0; i < mimpi.n; i++) {
            message_list_init(&mimpi.sent_messages[i]);
            mimpi.requested_message[i] = NULL;
        }
    }

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
    message_destroy_and_free(mimpi.received_message);

    // Destroy the list
    message_list_destroy(&mimpi.ml);
    free(mimpi.dead);

    if (mimpi.deadlock_detection) {
        // Destroy the lists
        for (int i = 0; i < mimpi.n; i++) {
            message_list_destroy(&mimpi.sent_messages[i]);
        }
        free(mimpi.sent_messages);
        for (int i = 0; i < mimpi.n; i++) {
            message_destroy_and_free(mimpi.requested_message[i]);
        }
        free(mimpi.requested_message);
    }
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

static void set_run_state_with_message(message_t *msg);

int get_parent(int msg_root);

int get_left_child(int msg_root);

int get_right_child(int msg_root);

static void set_received_message(message_t *message);

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

message_t* try_read_message(int read_fd) {
    mimpi_metadata_t metadata;
    MIMPI_Retcode metadata_read_result = complete_chrecv(read_fd, &metadata, sizeof(mimpi_metadata_t));
    if (metadata_read_result != MIMPI_SUCCESS) return NULL;

    void* data = NULL;
    if (metadata.count > 0 && metadata.source != DEADLOCK_MANAGER) {
        data = malloc(metadata.count);
        MIMPI_Retcode data_read_result = complete_chrecv(read_fd, data, metadata.count);
        if (data_read_result != MIMPI_SUCCESS) {
            free(data);
            return NULL;
        }
    }

    message_t* message = malloc(sizeof(message_t));
    message_init(message, metadata, data);
    return message;
}

void set_wait_state(int source, int tag, int count) {
    mimpi.state = MIMPI_STATE_WAIT;
    message_destroy_and_free(mimpi.received_message);
    mimpi.received_message = NULL;
    mimpi.recv_source = source;
    mimpi.recv_tag = tag; // possibly <= 0
    mimpi.recv_count = count;
}

static void set_run_state_with_message(message_t *msg) {
    mimpi.state = MIMPI_STATE_RUN;
    set_received_message(msg);
}

/// @brief Thread function that continuously reads from pipe _source -> mimpi.rank.
static void* receiver_thread(void* _source) {
    int source_rank = *((int*) _source); free(_source);
    int read_fd = get_pipe_read_fd(source_rank, mimpi.rank, mimpi.n);

    while (true) {
        message_t* new_msg = try_read_message(read_fd);
        if (new_msg == NULL) {
            d3g prt("Rank %d: READ ERROR from %d\n", mimpi.rank, source_rank);
            break;
        }
        d3g prt("Rank %d: received new_msg (%d, %d, %d)\n", mimpi.rank, new_msg->metadata.count, new_msg->metadata.source, new_msg->metadata.tag);

        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        if (mimpi.deadlock_detection && new_msg->metadata.source == DEADLOCK_MANAGER) {
            // We just got an info that the other process was waiting for us (count, tag).
            // Let's set the requested message first:
            assert(mimpi.requested_message[source_rank] == NULL);
//            message_destroy_and_free(mimpi.requested_message[source_rank]);
            mimpi.requested_message[source_rank] = new_msg;

            assert(new_msg->metadata.source == DEADLOCK_MANAGER);
            message_t* matching_sent_message = message_list_find_and_pop(&mimpi.sent_messages[source_rank],
                                                                         new_msg->metadata.count,
                                                                         new_msg->metadata.source, // !!!!!
                                                                         new_msg->metadata.tag);
            if (matching_sent_message != NULL) {
                d4g prt("Rank %d: matching_sent_message (%d, %d, %d)\n", mimpi.rank,
                       matching_sent_message->metadata.count,
                       matching_sent_message->metadata.source,
                       matching_sent_message->metadata.tag);
                message_destroy_and_free(matching_sent_message);
                message_destroy_and_free(mimpi.requested_message[source_rank]);
                mimpi.requested_message[source_rank] = NULL;
            }
            else {
//                d4g prt("Rank %d: no matching_sent_message (receiver thread) so...\n", mimpi.rank);
                if (mimpi.state == MIMPI_STATE_WAIT && mimpi.recv_source == source_rank) {
                    d4g prt("Rank %d: DEADLOCK in THREAD when waiting for (%d, %d, %d)\n", mimpi.rank,
                           mimpi.recv_count, mimpi.recv_source, mimpi.recv_tag);
                    message_t* deadlock_message = malloc(sizeof(message_t));
                    message_init(deadlock_message, (mimpi_metadata_t) {
                        .count = 0,
                        .source = DEADLOCK_MANAGER,
                        .tag = MIMPI_DEADLOCK_DETECTED_TAG
                    }, NULL);

                    message_destroy_and_free(mimpi.requested_message[source_rank]);
                    mimpi.requested_message[source_rank] = NULL;
                    set_run_state_with_message(deadlock_message);

                    ASSERT_ZERO(sem_post(&mimpi.semaphore));
                }
                else {
                    d4g prt("Rank %d: RECEIVED deadlock message, but we're not waiting for %d yet!\n", mimpi.rank, source_rank);
                }
            }
        }
        else if (mimpi.state == MIMPI_STATE_WAIT && metadata_matches_params(&new_msg->metadata,
                                                                            mimpi.recv_source,
                                                                            mimpi.recv_tag,
                                                                            mimpi.recv_count)) {
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
            message_list_push(&mimpi.ml, new_msg);
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

static void set_received_message(message_t *message) {
    d2g prt("Setting received message (rank %d)\n", mimpi.rank);
    if (mimpi.received_message != NULL) {
        message_destroy_and_free(mimpi.received_message);
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

    d4g prt("Rank %d: SEND (%d, %d) to %d\n", mimpi.rank, count, tag, destination);

    // Get write fd for the pipe mimpi.rank -> destination.
    int write_fd = get_pipe_write_fd(mimpi.rank, destination, mimpi.n);

    // Send metadata + data in a 'single stream' of bytes.
    void* metadata_and_data = malloc(sizeof(mimpi_metadata_t) + count);
    memcpy(metadata_and_data, &(mimpi_metadata_t) {
        .count = count,
        .source = mimpi.rank,
        .tag = tag
    }, sizeof(mimpi_metadata_t));
    memcpy(metadata_and_data + sizeof(mimpi_metadata_t), data, count);

    MIMPI_Retcode metadata_and_data_write_result = complete_chsend(write_fd, metadata_and_data, sizeof(mimpi_metadata_t) + count);
    free(metadata_and_data);

    if (metadata_and_data_write_result != MIMPI_SUCCESS) {
        return metadata_and_data_write_result;
    }

    // in *mutex* update sent messages list
    if (mimpi.deadlock_detection) {
        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        message_t* message = malloc(sizeof(message_t));
        message_init(message, (mimpi_metadata_t) {
            .count = count,
            .source = DEADLOCK_MANAGER,
            .tag = tag
        }, NULL);
        // if this message clears the requested message
        // then we can clear the requested message
        d4g prt("Rank %d: Sent message (%d, src: %d, %d)\n", mimpi.rank, count, mimpi.rank, tag);
        if (mimpi.requested_message[destination] != NULL &&
            metadata_matches_params(&mimpi.requested_message[destination]->metadata, DEADLOCK_MANAGER, tag, count))
        {
//            d4g prt("Rank %d: Sent message (%d, src: %d, %d) clears requested message\n", mimpi.rank, count, mimpi.rank, tag);
            message_destroy_and_free(mimpi.requested_message[destination]);
            mimpi.requested_message[destination] = NULL;
        }
        else {
//            d4g prt("Rank %d: Sent message (%d, src: %d, %d) doesn't clear requested message\n", mimpi.rank, count, mimpi.rank, tag);
            message_list_push(&mimpi.sent_messages[destination], message);
        }
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
    }

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

    d4g prt("Rank %d: RECV (%d, %d) from %d\n", mimpi.rank, count, tag, source);

    // (1) Check if the message is already in the list of received messages.
    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
    if (mimpi.deadlock_detection) {
        // send message to source that we are waiting for it (even if it's been already received or the source is dead).
        int write_fd = get_pipe_write_fd(mimpi.rank, source, mimpi.n);
        MIMPI_Retcode ret = complete_chsend(write_fd, &(mimpi_metadata_t) {
                .count = count,
                .source = DEADLOCK_MANAGER,
                .tag = tag
        }, sizeof(mimpi_metadata_t));
        d4g prt("Rank %d: REQUEST NOTIFY (%d, %d) to %d [ret: %d]\n", mimpi.rank, count, tag, source, ret);
    }

    message_t* message = message_list_find_and_pop(&mimpi.ml, count, source, tag);
    if (message) {
//        if (mimpi.deadlock_detection) assert(false);
        // If the message is already in the list, then we can return it.
        memcpy(data, message->data, count);
        message_destroy_and_free(message);
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
        return MIMPI_SUCCESS;
    }
    else {
        // Otherwise we should wait for the message to arrive.
        // If source has already escaped MPI block, then we should return an error.


        if (mimpi.dead[source]) {
            dbg prt("Rank %d: DEAD SOURCE Message (%d, src: %d, %d) not received [REMOTE_FINISHED].\n", mimpi.rank, count, source, tag);
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
//            if (mimpi.deadlock_detection) {
//                bool other_also_waiting = (mimpi.requested_message[source] != NULL);
//                d4g prt("%d asdfasdffffffffffffffsdsas%s", other_also_waiting, "\n");
//                if (other_also_waiting) {
//                    d4g prt("fsdfadfas%s", "\n");
//                    message_destroy_and_free(mimpi.requested_message[source]);
//                    mimpi.requested_message[source] = NULL;
//                    return MIMPI_ERROR_DEADLOCK_DETECTED;
//                }
//            }
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if (mimpi.deadlock_detection) {
            bool other_also_waiting = (mimpi.requested_message[source] != NULL);
            if (other_also_waiting) {
                // deadlock detected
                d4g prt("Rank %d: DEADLOCK in RECV (before WAIT) Message (%d, src: %d, %d) not received - source is also waiting.\n", mimpi.rank, count, source, tag);

                assert(mimpi.state == MIMPI_STATE_RUN);
                message_destroy_and_free(mimpi.requested_message[source]);
                mimpi.requested_message[source] = NULL;

                ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
                return MIMPI_ERROR_DEADLOCK_DETECTED;
            }
            else {
                d4g prt("Rank %d: Other rank is not yet waiting... I'm waiting...\n", mimpi.rank);
            }
        }

        set_wait_state(source, tag, count);
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
//        d4g prt("Rank %d: Wait...\n", mimpi.rank);

        // this part is blocking until the message arrives (or an error occurs):
        // -----------------------------------------------------------------
        ASSERT_ZERO(sem_wait(&mimpi.semaphore));    // FUTURE-TODO Monitor.GetMessage(count, source, tag);
        // -----------------------------------------------------------------

        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        assert(mimpi.state == MIMPI_STATE_RUN);
        // Move the received message to local variable.
        message = mimpi.received_message; mimpi.received_message = NULL;


        // Case 1: An error occurred.
        if (message == NULL) {
            d3g prt("Rank %d: Message (%d, src: %d, %d) was not received [REMOTE_FINISHED].\n", mimpi.rank, count, source, tag);
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else if (message->metadata.tag == MIMPI_DEADLOCK_DETECTED_TAG) {
            d3g prt("Rank %d: Message (%d, src: %d, %d) was not received [DEADLOCK_DETECTED].\n", mimpi.rank, count, source, tag);
            // clear requested flag at other TODO ?????????????? or maybe just overwrite it
            // or maybe set it to null ??
            d4g prt("Rank %d: DEADLOCK in RECV (after WAIT) Message (%d, src: %d, %d) not received - source is also waiting.\n", mimpi.rank, count, source, tag);

            assert(mimpi.state == MIMPI_STATE_RUN);
//            assert(mimpi.requested_message[source] != NULL); // ? TODO rem
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        // Case 2: The message arrived.
        d3g prt("Rank %d: Message (%d, src: %d, %d) was received.\n", mimpi.rank, count, source, tag);
        memcpy(data, message->data, count);
        message_destroy_and_free(message);

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





























