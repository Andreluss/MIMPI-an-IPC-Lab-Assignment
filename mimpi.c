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

typedef struct {
    int count;
    int source;
    int tag;
} mimpi_metadata_t;

typedef struct {
    mimpi_metadata_t metadata;
    void* data;
} received_message_t;

typedef struct received_messages_list_t {
    received_message_t* message;
    struct received_messages_list_t* next;
} received_messages_list_t;

static received_messages_list_t* received_messages_list_head = NULL;
static received_messages_list_t* received_messages_list_tail = NULL;

static struct {
    bool enable_deadlock_detection;
    int n;
    int rank;
    // This array contains helper threads that continuously read from 'input' channels (pipes).
    pthread_t* receiver_threads; // destroy in MIMPI_Finalize()
    // This array is used to communicate between the main thread and the receiver threads.
    bool* receiver_should_receive; // destroy in MIMPI_Finalize()
    /// This array contains mutexes that are used
    /// to synchronize the main thread and the receiver threads.
    pthread_mutex_t* receiver_mutexes; // destroy in MIMPI_Finalize()

    // <list of messages received from other processes is defined above>

    // This mutex is used to synchronize the main thread and the receiver threads
    // when accessing commonly used data from here.
    pthread_mutex_t mutex;

    bool is_waiting_on_recv;
    // This semaphore is used to make the MIMPI_Recv wait until the message arrives.
    sem_t recv_sem; // destroy in MIMPI_Finalize()
    int recv_source;
    int recv_tag;
    int recv_count;
    received_message_t* received_message;
} mimpi;

static bool metadata_matches_params(mimpi_metadata_t *metadata, int source, int tag, int count) {
    return metadata &&
           metadata->count == count && metadata->source == source &&
           (metadata->tag == tag || tag == MIMPI_ANY_TAG);
}

/// @brief Pushes a message to the back of the list.
/// @param message The message to be pushed.
/// Notice: the function take the ownership of the message pointer.
static void push_message_to_list(received_message_t* message) {
    received_messages_list_t* new_node = malloc(sizeof(received_messages_list_t));
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
static received_message_t* find_and_pop_message_from_list(int count, int source, int tag) {
    received_messages_list_t* prev = NULL;
    received_messages_list_t* curr = received_messages_list_head;
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

//static received_message_t* locked_find_and_pop_message_from_list(int count, int source, int tag) {
//    ASSERT_ZERO(pthread_mutex_lock(&mimpi.recv_mutex));
//    received_message_t* message = find_and_pop_message_from_list(count, source, tag);
//    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.recv_mutex));
//}

static void channels_create_receiver_thread(int i);

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


void MIMPI_Init(bool enable_deadlock_detection) {

    channels_init();
    mimpi.enable_deadlock_detection = enable_deadlock_detection;

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

    // Create mutexes for receiver threads
    mimpi.receiver_mutexes = malloc(mimpi.n * sizeof(pthread_mutex_t));
    for (int i = 0; i < mimpi.n; i++) {
        ASSERT_ZERO(pthread_mutex_init(&mimpi.receiver_mutexes[i], NULL));
    }

    // Create array of flags for receiver threads
    mimpi.receiver_should_receive = malloc(mimpi.n * sizeof(bool));
    for (int i = 0; i < mimpi.n; i++) {
        mimpi.receiver_should_receive[i] = true;
    }

    // Create and run helper receiver threads
    mimpi.receiver_threads = malloc(mimpi.n * sizeof(pthread_t));
    for (int i = 0; i < mimpi.n; i++) {
        if (i == mimpi.rank)
            continue;
        channels_create_receiver_thread(i);
    }

    mimpi.is_waiting_on_recv = false;
    ASSERT_ZERO(pthread_mutex_init(&mimpi.mutex, NULL));
    ASSERT_ZERO(sem_init(&mimpi.recv_sem, 0, 0)); // semaphore shared between processes
    mimpi.received_message = NULL;
}

/// @brief Thread function that continuously reads from pipe _from_rank -> mimpi.rank.
static void* channels_receiver_thread(void* _from_rank) {
    int from_rank = *((int*) _from_rank);
    free(_from_rank);

    // Get read fd for the pipe from_rank -> mimpi.rank.
    int read_fd = get_pipe_read_fd(from_rank, mimpi.rank, mimpi.n);
    dbg {
        prt("Pid %d created receiver thread for mimpi.rank = %d, read_fd = %d\n",
            getpid(), mimpi.rank, read_fd);
    }

    while (true) {
        ASSERT_ZERO(pthread_mutex_lock(&mimpi.receiver_mutexes[from_rank]));
        if (!mimpi.receiver_should_receive[from_rank]) {
            ASSERT_ZERO(pthread_mutex_unlock(&mimpi.receiver_mutexes[from_rank]));
            break;
        }
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.receiver_mutexes[from_rank]));

        // (1) Read metadata message from the sender.
        mimpi_metadata_t metadata;
        MIMPI_Retcode metadata_read_result = complete_chrecv(read_fd, &metadata, sizeof(metadata));
        if (metadata_read_result != MIMPI_SUCCESS) {
            dbg {
                prt("Pid %d: ERROR metadata_read_result = %d\n", getpid(), metadata_read_result);
            }
            break;
        }

        dbg {
            prt("Helper thread in process with rank %d \n will now"
                  " try to receive message from %d=%d with tag %d (of size %d)",
                  mimpi.rank, from_rank, metadata.source, metadata.tag, metadata.count);
        };

        // (2) Read data from the sender.
        void* data = malloc(metadata.count);
        MIMPI_Retcode data_read_result = complete_chrecv(read_fd, data, metadata.count);
        if (data_read_result != MIMPI_SUCCESS) {
            dbg {
                prt("Pid %d: ERROR data_read_result = %d\n", getpid(), data_read_result);
            }
            free(data);
            break;
        }

        dbg {
            prt("Helper thread in process with rank %d \n "
                "received message from %d with tag %d (of size %d)",
                mimpi.rank, from_rank, metadata.tag, metadata.count);
        };

        // (3) Create message object.
        received_message_t* message = malloc(sizeof(received_message_t));
        message->metadata = metadata;
        message->data = data;

        // (4) Check if the main thread is waiting on MIMPI_Recv() for this kind of message.
        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        if (mimpi.is_waiting_on_recv && metadata_matches_params(&message->metadata,
            mimpi.recv_source, mimpi.recv_tag, mimpi.recv_count)) {
            // If the message is what the main thread is waiting for,
            // then we should wake it up.
            mimpi.received_message = message;
            mimpi.is_waiting_on_recv = false;
            ASSERT_ZERO(sem_post(&mimpi.recv_sem));
        }
        else {
            // Otherwise, we should push the message to the list of received messages.
            push_message_to_list(message);
        }
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
    }

    // Thread is about to end. If the main process is waiting on MIMPI_Recv(),
    // then we should wake it up.
    ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
    dbg prt("Helper thread for %d -> %d is about to end\n", from_rank, mimpi.rank);
    if (mimpi.is_waiting_on_recv) {
        mimpi.received_message = NULL;
        mimpi.is_waiting_on_recv = false;
        ASSERT_ZERO(sem_post(&mimpi.recv_sem));
    }
    ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));
    dbg prt("Helper thread for %d -> %d ended\n", from_rank, mimpi.rank);
    return NULL;
}

/// @brief Creates a pthread that will continuously read from pipe i -> mimpi.rank.
static void channels_create_receiver_thread(int i) {
    assert(i != mimpi.rank);
    pthread_t thread;
    int* thread_arg_from_rank = malloc(sizeof(int));
    *thread_arg_from_rank = i;

    ASSERT_ZERO(pthread_create(&thread, NULL, channels_receiver_thread, thread_arg_from_rank));

    mimpi.receiver_threads[i] = thread;
}

void MIMPI_Finalize() {
    dbg prt("Process with rank %d is finalizing\n", mimpi.rank);
    // TODO send message informing of death
    // (this should stop the receiver threads even in the middle of reading!)

    // Tell all helper threads to stop receiving new messages.
    for (int i = 0; i < mimpi.n; i++) {
        if (i == mimpi.rank)
            continue;
        ASSERT_ZERO(pthread_mutex_lock(&mimpi.receiver_mutexes[i]));
        mimpi.receiver_should_receive[i] = false;
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.receiver_mutexes[i]));
    }

    // Close all channels (pipes) related to this process.
    for (int i = 0; i < mimpi.n; i++) {
        if (i == mimpi.rank)
            continue;
        int read_fd = get_pipe_read_fd(i, mimpi.rank, mimpi.n);
        int write_fd = get_pipe_write_fd(mimpi.rank, i, mimpi.n);
        ASSERT_ZERO(close(read_fd));
        ASSERT_ZERO(close(write_fd));
    }
    dbg prt("Process with rank %d closed all channels\n", mimpi.rank);
    dbg print_open_descriptors(mimpi.n);

    // Now wait for the helper threads to finish
    // (they should exit, because the pipes are closed).
    for (int i = 0; i < mimpi.n; i++) {
        if (i == mimpi.rank)
            continue;
        dbg prt("Rank %d waiting for helper thread for %d -> %d to finish...\n",
                mimpi.rank, i, mimpi.rank);
        ASSERT_ZERO(pthread_join(mimpi.receiver_threads[i], NULL));
    }

    // Free all memory allocated in MIMPI_Init().
    free(mimpi.receiver_threads);
    free(mimpi.receiver_should_receive);
    for (int i = 0; i < mimpi.n; i++) {
        ASSERT_ZERO(pthread_mutex_destroy(&mimpi.receiver_mutexes[i]));
    }
    free(mimpi.receiver_mutexes);
    ASSERT_ZERO(pthread_mutex_destroy(&mimpi.mutex));
    ASSERT_ZERO(sem_destroy(&mimpi.recv_sem));
    channels_finalize();
    dbg prt("Process with rank %d finished finalizing\n", mimpi.rank);
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

//    if (mimpi.enable_deadlock_detection) {
//
//    }
    return MIMPI_SUCCESS;
}

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

/// @brief Receives all bytes_to_read bytes of data from the file descriptor fd.
/// Warning: this function assumes that data IS ALREADY ALLOCATED to hold bytes_to_read bytes.
/// @return MIMPI return code:
///         - `MIMPI_SUCCESS` if operation ended successfully.
///         - `MIMPI_ERROR_REMOTE_FINISHED` if the process with rank
///           @ref source has already escaped _MPI block_ (closed the pipe).
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
    received_message_t* message = find_and_pop_message_from_list(count, source, tag);
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
        mimpi.is_waiting_on_recv = true;
        mimpi.recv_source = source;
        mimpi.recv_tag = tag;
        mimpi.recv_count = count;
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi.mutex));

        // this part is blocking until the message arrives (or an error occurs):
        // -----------------------------------------------------------------
        ASSERT_ZERO(sem_wait(&mimpi.recv_sem));
        // -----------------------------------------------------------------

        ASSERT_ZERO(pthread_mutex_lock(&mimpi.mutex));
        message = mimpi.received_message;

        // Case 1: An error occurred.
        if (message == NULL) {
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