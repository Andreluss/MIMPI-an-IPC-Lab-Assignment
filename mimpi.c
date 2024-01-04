/**
 * This file is for implementation of MIMPI library.
 * */

#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <memory.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

static const int MIMPI_BARRIER_WAIT_TAG = -1;
static const int MIMPI_BARRIER_NOTIFY_TAG = -2;
static const int MIMPI_BCAST_WAIT_TAG = -3;
__attribute_maybe_unused__ static const int MIMPI_BCAST_NOTIFY_TAG = -4;
static const int MIMPI_REDUCE_WAIT_TAG = -5;
__attribute_maybe_unused__ static const int MIMPI_REDUCE_NOTIFY_TAG = -6;
typedef struct {
//    enum {
//        MIMPI_SEND,
//        MIMPI_BARRIER_WAIT,
//        MIMPI_BARRIER_NOTIFY,
//        MIMPI_BCAST_WAIT,
//        MIMPI_BCAST_NOTIFY,
//        MIMPI_REDUCE_WAIT,
//        MIMPI_REDUCE_NOTIFY,
//    } type;
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
    MIMPI_STATE_IDLE,
    MIMPI_STATE_WAITING,
    MIMPI_STATE_BARRIER_WAITING,
    MIMPI_STATE_GROUP_SYNCING
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
    mimpi.state = MIMPI_STATE_IDLE;

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

void set_wait_state(int source, int tag, int count);

void clear_wait_state();

int get_parent();

int get_left_child();

int get_right_child();

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

/// @brief Thread function that continuously reads from pipe _source -> mimpi.rank.
static void* receiver_thread(void* _source) {
    int source_rank = *((int*) _source);
    free(_source);

    // Get read fd for the pipe source_rank -> mimpi.rank.
    int read_fd = get_pipe_read_fd(source_rank, mimpi.rank, mimpi.n);
    dbg {
        prt("Pid %d created receiver thread for mimpi.rank = %d, read_fd = %d\n",
            getpid(), mimpi.rank, read_fd);
    }

    while (true) {

        // (1) Read metadata message from the sender.
        mimpi_metadata_t metadata;
        MIMPI_Retcode metadata_read_result = complete_chrecv(read_fd, &metadata, sizeof(metadata));
        if (metadata_read_result != MIMPI_SUCCESS) {
            dbg {
                prt("Pid %d: ERROR metadata_read_result = %d\n", getpid(), metadata_read_result);
            }
            break;
        }

        // (2) Read data from the sender.

        void* data = NULL;
        if (metadata.count > 0) {
            data = malloc(metadata.count);
            MIMPI_Retcode data_read_result = complete_chrecv(read_fd, data, metadata.count);

            if (data_read_result != MIMPI_SUCCESS) {
                dbg {
                    prt("Pid %d: ERROR data_read_result = %d\n", getpid(), data_read_result);
                }
                free(data);
                break;
            }
        }
        dbg {
            prt("Helper thread in process with rank %d \n "
                "received message from %d with tag %d (of size %d)",
                mimpi.rank, source_rank, metadata.tag, metadata.count);
        };

        // (3) Create message object.
        received_message_t* message = malloc(sizeof(received_message_t));
        message->metadata = metadata;
        message->data = data;

// -----------------------------------------------------------------------------------
        // Handle different types of messages.
        if (metadata.tag == MIMPI_BARRIER_WAIT_TAG) {
            // Sync/info message (doesn't unblock the main thread).
            d2g prt("Pid %d: Received BARRIER_WAIT from %d\n", getpid(), source_rank);
            ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
            mimpi.group_synced_count++;
            bool sync_completed = (mimpi.group_synced_count == mimpi.n);
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

            if (sync_completed) {
                assert(mimpi.state == MIMPI_STATE_GROUP_SYNCING);
                assert(mimpi.is_waiting_on_semaphore);
                ASSERT_ZERO(sem_post(&mimpi.semaphore));
            }

        }
        else if (metadata.tag == MIMPI_BCAST_WAIT_TAG){
            // Sync/info message (doesn't unblock the main thread).
            TODO
        }
        else if (metadata.tag == MIMPI_REDUCE_WAIT_TAG){
            // Sync/info message (doesn't unblock the main thread).
            TODO
        }
        else {
            // Unblocking message (on which the main thread could be waiting).

            // (4) Check if the main thread is waiting on MIMPI_Recv() for this kind of message.
            ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
            if (mimpi.is_waiting_on_semaphore && metadata_matches_params
                    (&message->metadata, mimpi.recv_source, mimpi.recv_tag, mimpi.recv_count)) {
                // If the message is what the main thread is waiting for, we should wake it up.
                set_received_message(message);
                mimpi.is_waiting_on_semaphore = false;
                ASSERT_ZERO(sem_post(&mimpi.semaphore));
            }
            else {
                // Otherwise, we should push the message to the list of received messages.
                message_list_push(message);
            }
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
        }
        dbg
        {
            prt("Helper thread in process with rank %d \n will now"
                " try to receive message from %d=%d with tag %d (of size %d)",
                mimpi.rank, source_rank, metadata.source, metadata.tag, metadata.count);
        }

// -----------------------------------------------------------------------------------


    }

    // Thread is about to end.
    // If the main process is waiting on *anything* from source_rank,
    // then we should wake it up.
    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
    dbg prt("Helper thread for %d -> %d is about to end\n", source_rank, mimpi.rank);
    if (mimpi.is_waiting_on_semaphore && mimpi.recv_source == source_rank) {
        mimpi.received_message = NULL;
        mimpi.is_waiting_on_semaphore = false;
        ASSERT_ZERO(sem_post(&mimpi.semaphore));
    }
    else if (mimpi.is_waiting_on_semaphore && mimpi.state == MIMPI_STATE_GROUP_SYNCING) {
        // cancel synchronization
        assert(mimpi.group_synced_count < mimpi.n);
        ASSERT_ZERO(sem_post(&mimpi.semaphore));
    }
    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
    dbg prt("Helper thread for %d -> %d ended\n", source_rank, mimpi.rank);
    return NULL;
}

static void set_received_message(received_message_t *message) {
    if (mimpi.received_message != NULL) {
        received_message_destroy(mimpi.received_message);
    }
    mimpi.received_message = message;
//    mimpi.is_waiting_on_semaphore = false; // MAYBE?
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    // (1) Get world size from env
    assert(getenv("MIMPI_N") != NULL);
    int n = atoi(getenv("MIMPI_N"));
    dbg {
        prt("Pid %d has mimpi.n = %d\n", getpid(), mimpi.n);
    }

    // (2) Get world rank from env
    pid_t pid = getpid();
    char envariable_name[32];
    get_mimpi_rank_for_pid_envariable_name(envariable_name, pid);
    assert(getenv(envariable_name) != NULL);
    int rank = atoi(getenv(envariable_name));
    dbg {
        prt("Pid %d has mimpi.rank = %d\n", getpid(), mimpi.rank);
        print_open_descriptors(mimpi.n);
    };

    // (3) Create helper threads and structures.
    mimpi_init(n, rank, enable_deadlock_detection);
}

void MIMPI_Finalize() {
    dbg prt("Process with rank %d is finalizing\n", mimpi.rank);
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
    dbg {
        prt("Process with rank %d closed all channels\n", mimpi.rank);
        print_open_descriptors(mimpi.n);
    };

    // (2) Now wait for the helper threads to finish
    // (they should exit, because the pipes are closed).
    for (int i = 0; i < mimpi.n; i++) {
        if (i == mimpi.rank)
            continue;
        dbg {
            prt("Rank %d waiting for helper thread for %d -> %d to finish...\n",
                mimpi.rank, i, mimpi.rank);
        }
        ASSERT_ZERO(pthread_join(mimpi.receiver_threads[i], NULL));
    }

    // (3) Free all memory allocated in MIMPI_Init().
    mimpi_destroy();
    channels_finalize();
    dbg prt("Process with rank %d successfully FINALIZED\n", mimpi.rank);
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
        free(message->data);
        free(message);
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
        return MIMPI_SUCCESS;
    }
    else {
        // Otherwise, we should wait for the message to arrive.
        mimpi.is_waiting_on_semaphore = true;
        mimpi.recv_source = source;
        mimpi.recv_tag = tag;
        mimpi.recv_count = count;
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        // this part is blocking until the message arrives (or an error occurs):
        // -----------------------------------------------------------------
        ASSERT_ZERO(sem_wait(&mimpi.semaphore));
        // -----------------------------------------------------------------

        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        mimpi.is_waiting_on_semaphore = false;
        message = mimpi.received_message;

        // Case 1: An error occurred.
        if (message == NULL) {
            dbg prt("////////// Rank %d Error after waiting to receive message", mimpi.rank);
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // Case 2: The message arrived.
        mimpi.received_message = NULL;
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        memcpy(data, message->data, count);
        free(message->data);
        free(message);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Barrier() {
    if (mimpi.n == 1) {
        return MIMPI_SUCCESS;
    }

    // If this is the synchronization root.
    if (mimpi.rank == 0) {
        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        mimpi.group_synced_count++;
        mimpi.state = MIMPI_STATE_GROUP_SYNCING;
        bool should_wait = (mimpi.group_synced_count < mimpi.n);
        mimpi.is_waiting_on_semaphore = should_wait;
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        if (should_wait) {// Notice: the receiver thread SHOULD post the semaphore.
            ASSERT_ZERO(sem_wait(&mimpi.semaphore));
            mimpi.is_waiting_on_semaphore = false;
        }

        // Case 1: An error occurred.
        if (mimpi.group_synced_count < mimpi.n) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // Case 2: The group is synced.
        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        mimpi.group_synced_count = 0;
        mimpi.state = MIMPI_STATE_IDLE;
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        // Create a notify message to be propagated to children.
        received_message_t* message = malloc(sizeof(received_message_t));
        received_message_init(message, (mimpi_metadata_t) {
                .count = 0,
                .source = mimpi.rank,
                .tag = MIMPI_BARRIER_NOTIFY_TAG
        }, NULL);

        // Propagate the message to children (if any).
        if (get_left_child() < mimpi.n) {
            complete_chsend(get_pipe_write_fd(mimpi.rank, get_left_child(), mimpi.n),
                            message, sizeof(mimpi_metadata_t));
        }
        if (get_right_child() < mimpi.n) {
            complete_chsend(get_pipe_write_fd(mimpi.rank, get_right_child(), mimpi.n),
                            message, sizeof(mimpi_metadata_t));
        }
    }
    // Otherwise, we need to send a message to the synchronization root (rank 0).
    else {
        // (1) Send metadata message (BARRIER_WAIT) to the synchronization root.
        set_wait_state(get_parent(), MIMPI_BARRIER_NOTIFY_TAG, 0);

        MIMPI_Retcode metadata_write_result =
            complete_chsend(get_pipe_write_fd(mimpi.rank, 0, mimpi.n), &(mimpi_metadata_t) {
                    .count = 0,
                    .source = mimpi.rank,
                    .tag = MIMPI_BARRIER_WAIT_TAG
            }, sizeof(mimpi_metadata_t));
        if (metadata_write_result != MIMPI_SUCCESS) return metadata_write_result;

        // (2) Wait for the response (BARRIER_NOTIFY) - from (mimpi.rank-1)/2.
        ASSERT_ZERO(sem_wait(&mimpi.semaphore)); // Notice: the receiver thread SHOULD post the semaphore.
        // Clear the waiting for barrier state.
        clear_wait_state();

        // Case 1: An error occurred.
        if (mimpi.received_message == NULL) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        // Case 2: The message arrived. Then:

        // - swap the sender in metadata
        mimpi.received_message->metadata.source = mimpi.rank;

        // - and propagate the message to children (if any).
        if (get_left_child() < mimpi.n) {
            complete_chsend(get_pipe_write_fd(mimpi.rank, get_left_child(), mimpi.n),
                            mimpi.received_message, sizeof(mimpi_metadata_t));
        }
        if (get_right_child() < mimpi.n) {
            complete_chsend(get_pipe_write_fd(mimpi.rank, get_right_child(), mimpi.n),
                            mimpi.received_message, sizeof(mimpi_metadata_t));
        }
    }
    return MIMPI_SUCCESS;
}

int get_right_child() { return mimpi.rank * 2 + 2; }

int get_left_child() { return mimpi.rank * 2 + 1; }

int get_parent() { return (mimpi.rank - 1) / 2; }

void clear_wait_state() {
    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
//    mimpi.state = MIMPI_STATE_IDLE;
    mimpi.is_waiting_on_semaphore = false;
    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
}

void set_wait_state(int source, int tag, int count) {
    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
//    mimpi.state = MIMPI_STATE_WAITING;
    mimpi.is_waiting_on_semaphore = true;
    mimpi.recv_source = source;
    mimpi.recv_tag = tag; // possibly <= 0
    mimpi.recv_count = count;
    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
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