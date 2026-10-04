#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include <ctype.h>

#define PORT 11634
#define BACKLOG 10
#define BUFFER_SIZE 4096
#define MAX_USERNAME 50
#define MAX_ROOMNAME 50
#define NID_TAG " NID:6956"
#define LOG_FILE "netmsg_IT23695634.log"
#define STORAGE_BASE "./storage/IT23695634/"

// ---------- DATA STRUCTURES ----------
typedef struct Client {
    int sockfd;
    char username[MAX_USERNAME];
    int is_registered;
    struct Client *next;
} Client;

typedef struct Room {
    char name[MAX_ROOMNAME];
    char members[10][MAX_USERNAME]; // Max 10 members per room for simplicity
    int member_count;
    struct Room *next;
} Room;

// Global lists and mutexes for thread safety
Client *clients_head = NULL;
Room *rooms_head = NULL;
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t rooms_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

// ---------- UTILITY FUNCTIONS ----------

// Log events to the personalised log file
void log_event(const char *format, ...) {
    pthread_mutex_lock(&log_mutex);
    FILE *fp = fopen(LOG_FILE, "a");
    if (fp) {
        time_t now = time(NULL);
        char *time_str = ctime(&now);
        time_str[strlen(time_str) - 1] = '\0'; // Remove newline
        fprintf(fp, "[%s] ", time_str);
        
        va_list args;
        va_start(args, format);
        vfprintf(fp, format, args);
        va_end(args);
        
        fprintf(fp, "\n");
        fclose(fp);
    }
    pthread_mutex_unlock(&log_mutex);
}

// Send a formatted response with the NID tag
void send_response(int sockfd, const char *status, const char *msg) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "%s %s%s\n", status, msg, NID_TAG);
    send(sockfd, buffer, strlen(buffer), 0);
}

// Find a client by username
Client* find_client_by_username(const char *username) {
    Client *curr = clients_head;
    while (curr) {
        if (strcmp(curr->username, username) == 0) return curr;
        curr = curr->next;
    }
    return NULL;
}

// Find a client by socket fd
Client* find_client_by_sockfd(int sockfd) {
    Client *curr = clients_head;
    while (curr) {
        if (curr->sockfd == sockfd) return curr;
        curr = curr->next;
    }
    return NULL;
}

// Find or create a room
Room* find_room(const char *roomname) {
    Room *curr = rooms_head;
    while (curr) {
        if (strcmp(curr->name, roomname) == 0) return curr;
        curr = curr->next;
    }
    // Create new room
    Room *new_room = (Room *)malloc(sizeof(Room));
    strncpy(new_room->name, roomname, MAX_ROOMNAME - 1);
    new_room->member_count = 0;
    new_room->next = rooms_head;
    rooms_head = new_room;
    return new_room;
}

// Remove a client from all rooms (used on disconnect)
void remove_client_from_rooms(const char *username) {
    pthread_mutex_lock(&rooms_mutex);
    Room *curr = rooms_head;
    while (curr) {
        for (int i = 0; i < curr->member_count; i++) {
            if (strcmp(curr->members[i], username) == 0) {
                // Shift remaining members
                for (int j = i; j < curr->member_count - 1; j++) {
                    strcpy(curr->members[j], curr->members[j+1]);
                }
                curr->member_count--;
                break;
            }
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&rooms_mutex);
}

// Remove a client from the global list and free memory
void remove_client(int sockfd) {
    pthread_mutex_lock(&clients_mutex);
    Client *curr = clients_head;
    Client *prev = NULL;
    while (curr) {
        if (curr->sockfd == sockfd) {
            if (prev) prev->next = curr->next;
            else clients_head = curr->next;
            
            if (curr->is_registered) {
                remove_client_from_rooms(curr->username);
                log_event("DISCONNECT: User '%s' disconnected.", curr->username);
            }
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&clients_mutex);
}

// ---------- COMMAND HANDLERS ----------

void handle_register(int sockfd, const char *username) {
    if (strlen(username) == 0) {
        send_response(sockfd, "ERR", "001 INVALID_USERNAME");
        return;
    }
    
    pthread_mutex_lock(&clients_mutex);
    if (find_client_by_username(username)) {
        pthread_mutex_unlock(&clients_mutex);
        send_response(sockfd, "ERR", "001 USERNAME_TAKEN");
        return;
    }
    
    Client *client = find_client_by_sockfd(sockfd);
    if (client) {
        strncpy(client->username, username, MAX_USERNAME - 1);
        client->is_registered = 1;
        send_response(sockfd, "OK", "REGISTERED");
        log_event("REGISTER: User '%s' registered.", username);
    }
    pthread_mutex_unlock(&clients_mutex);
}

void handle_bcast(int sockfd, const char *message) {
    Client *sender = find_client_by_sockfd(sockfd);
    if (!sender || !sender->is_registered) return;
    
    char out_msg[BUFFER_SIZE];
    snprintf(out_msg, sizeof(out_msg), "MSG BCAST %s %s\n", sender->username, message);
    
    pthread_mutex_lock(&clients_mutex);
    Client *curr = clients_head;
    while (curr) {
        if (curr->sockfd != sockfd && curr->is_registered) {
            send(curr->sockfd, out_msg, strlen(out_msg), 0);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&clients_mutex);
    
    send_response(sockfd, "OK", "SENT");
    log_event("BCAST: %s -> '%s'", sender->username, message);
}

void handle_pmsg(int sockfd, const char *target, const char *message) {
    Client *sender = find_client_by_sockfd(sockfd);
    if (!sender || !sender->is_registered) return;
    
    pthread_mutex_lock(&clients_mutex);
    Client *target_client = find_client_by_username(target);
    if (!target_client) {
        pthread_mutex_unlock(&clients_mutex);
        send_response(sockfd, "ERR", "002 USER_NOT_FOUND");
        return;
    }
    
    char out_msg[BUFFER_SIZE];
    snprintf(out_msg, sizeof(out_msg), "MSG PRIV %s %s\n", sender->username, message);
    send(target_client->sockfd, out_msg, strlen(out_msg), 0);
    pthread_mutex_unlock(&clients_mutex);
    
    send_response(sockfd, "OK", "SENT");
    log_event("PMSG: %s -> %s: '%s'", sender->username, target, message);
}

void handle_join(int sockfd, const char *roomname) {
    Client *client = find_client_by_sockfd(sockfd);
    if (!client || !client->is_registered) return;
    
    pthread_mutex_lock(&rooms_mutex);
    Room *room = find_room(roomname);
    if (room->member_count >= 10) {
        pthread_mutex_unlock(&rooms_mutex);
        send_response(sockfd, "ERR", "005 ROOM_FULL");
        return;
    }
    
    // Check if already in room
    for (int i = 0; i < room->member_count; i++) {
        if (strcmp(room->members[i], client->username) == 0) {
            pthread_mutex_unlock(&rooms_mutex);
            send_response(sockfd, "OK", "JOINED"); // Already in room
            return;
        }
    }
    
    strcpy(room->members[room->member_count++], client->username);
    pthread_mutex_unlock(&rooms_mutex);
    
    send_response(sockfd, "OK", "JOINED");
    log_event("JOIN: %s joined room '%s'", client->username, roomname);
}

void handle_leave(int sockfd, const char *roomname) {
    Client *client = find_client_by_sockfd(sockfd);
    if (!client || !client->is_registered) return;
    
    pthread_mutex_lock(&rooms_mutex);
    Room *room = find_room(roomname);
    int found = 0;
    for (int i = 0; i < room->member_count; i++) {
        if (strcmp(room->members[i], client->username) == 0) {
            for (int j = i; j < room->member_count - 1; j++) {
                strcpy(room->members[j], room->members[j+1]);
            }
            room->member_count--;
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&rooms_mutex);
    
    if (found) {
        send_response(sockfd, "OK", "LEFT");
        log_event("LEAVE: %s left room '%s'", client->username, roomname);
    } else {
        send_response(sockfd, "ERR", "003 ROOM_NOT_FOUND");
    }
}

void handle_rooms(int sockfd) {
    char room_list[BUFFER_SIZE] = "";
    pthread_mutex_lock(&rooms_mutex);
    Room *curr = rooms_head;
    while (curr) {
        if (curr->member_count > 0) {
            strcat(room_list, curr->name);
            strcat(room_list, ",");
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&rooms_mutex);
    
    if (strlen(room_list) > 0) room_list[strlen(room_list)-1] = '\0'; // Remove trailing comma
    send_response(sockfd, "OK", room_list);
}

void handle_rmsg(int sockfd, const char *roomname, const char *message) {
    Client *sender = find_client_by_sockfd(sockfd);
    if (!sender || !sender->is_registered) return;
    
    pthread_mutex_lock(&rooms_mutex);
    Room *room = find_room(roomname);
    if (room->member_count == 0) {
        pthread_mutex_unlock(&rooms_mutex);
        send_response(sockfd, "ERR", "003 ROOM_NOT_FOUND");
        return;
    }
    
    char out_msg[BUFFER_SIZE];
    snprintf(out_msg, sizeof(out_msg), "MSG ROOM %s %s %s\n", roomname, sender->username, message);
    
    for (int i = 0; i < room->member_count; i++) {
        Client *target = find_client_by_username(room->members[i]);
        if (target && target->sockfd != sockfd) {
            send(target->sockfd, out_msg, strlen(out_msg), 0);
        }
    }
    pthread_mutex_unlock(&rooms_mutex);
    
    send_response(sockfd, "OK", "SENT");
    log_event("RMSG: %s to room '%s': '%s'", sender->username, roomname, message);
}

void handle_sendfile(int sockfd, const char *target, const char *filename, int filesize) {
    Client *sender = find_client_by_sockfd(sockfd);
    if (!sender || !sender->is_registered) return;
    
    // 1. Create user's storage directory if it doesn't exist
    char dir_path[256];
    snprintf(dir_path, sizeof(dir_path), "%s%s/", STORAGE_BASE, sender->username);
    mkdir(STORAGE_BASE, 0777);
    mkdir(dir_path, 0777);
    
    // 2. Save file to disk
    char filepath[256];
    snprintf(filepath, sizeof(filepath), "%s%s", dir_path, filename);
    FILE *fp = fopen(filepath, "wb");
    if (!fp) {
        send_response(sockfd, "ERR", "006 FILE_WRITE_ERROR");
        return;
    }
    
    char buffer[BUFFER_SIZE];
    int remaining = filesize;
    while (remaining > 0) {
        int to_read = (remaining < BUFFER_SIZE) ? remaining : BUFFER_SIZE;
        int bytes_read = recv(sockfd, buffer, to_read, 0);
        if (bytes_read <= 0) break; // Connection lost
        fwrite(buffer, 1, bytes_read, fp);
        remaining -= bytes_read;
    }
    fclose(fp);
    
    // 3. Deliver the file to the target (if it's a user)
    Client *target_client = find_client_by_username(target);
    if (target_client) {
        char out_msg[BUFFER_SIZE];
        snprintf(out_msg, sizeof(out_msg), "MSG FILE %s %s %d\n", sender->username, filename, filesize);
        send(target_client->sockfd, out_msg, strlen(out_msg), 0);
    }
    
    send_response(sockfd, "OK", "FILE_RECEIVED"); 
    log_event("SENDFILE: %s sent '%s' (%d bytes) to %s", sender->username, filename, filesize, target);
}

// ---------- MAIN CLIENT HANDLER THREAD ----------
void *handle_client(void *arg) {
    int sockfd = *(int *)arg;
    free(arg);
    
    // Register client in global list
    pthread_mutex_lock(&clients_mutex);
    Client *new_client = (Client *)malloc(sizeof(Client));
    new_client->sockfd = sockfd;
    new_client->is_registered = 0;
    new_client->next = clients_head;
    clients_head = new_client;
    pthread_mutex_unlock(&clients_mutex);
    
    log_event("CONNECT: New connection on socket %d", sockfd);
    
    char buffer[BUFFER_SIZE];
    char line_buffer[BUFFER_SIZE];
    int line_len = 0;
    
    while (1) {
        int bytes_read = recv(sockfd, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_read <= 0) break; // Disconnect or error
        
        buffer[bytes_read] = '\0';
        
        // Process byte by byte to handle partial lines and multiple commands
        for (int i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                line_buffer[line_len] = '\0';
                
                // Parse command (FIXED: arg3 declared, sscanf updated)
                char command[20], arg1[256], arg2[BUFFER_SIZE], arg3[256];
                int args = sscanf(line_buffer, "%s %s %s %s", command, arg1, arg2, arg3);
                
                if (strcmp(command, "REGISTER") == 0 && args >= 2) {
                    handle_register(sockfd, arg1);
                } 
                else if (strcmp(command, "BCAST") == 0 && args >= 2) {
                    handle_bcast(sockfd, arg1); 
                }
                else if (strcmp(command, "PMSG") == 0 && args >= 3) {
                    handle_pmsg(sockfd, arg1, arg2);
                }
                else if (strcmp(command, "JOIN") == 0 && args >= 2) {
                    handle_join(sockfd, arg1);
                }
                else if (strcmp(command, "LEAVE") == 0 && args >= 2) {
                    handle_leave(sockfd, arg1);
                }
                else if (strcmp(command, "ROOMS") == 0) {
                    handle_rooms(sockfd);
                }
                else if (strcmp(command, "RMSG") == 0 && args >= 3) {
                    handle_rmsg(sockfd, arg1, arg2);
                }
                else if (strcmp(command, "SENDFILE") == 0 && args >= 4) {
                    int filesize = atoi(arg3);
                    handle_sendfile(sockfd, arg1, arg2, filesize);
                }
                else if (strcmp(command, "QUIT") == 0) {
                    send_response(sockfd, "OK", "BYE");
                    goto cleanup;
                }
                else {
                    send_response(sockfd, "ERR", "007 UNKNOWN_COMMAND");
                }
                
                line_len = 0; // Reset for next line
            } else {
                line_buffer[line_len++] = buffer[i];
                if (line_len >= BUFFER_SIZE - 1) line_len = 0; // Prevent overflow
            }
        }
    }

cleanup:
    remove_client(sockfd);
    close(sockfd);
    return NULL;
}

// ---------- MAIN FUNCTION ----------
int main() {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, BACKLOG) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }

    printf("NetMessenger Server started on port %d\n", PORT);
    printf("Node ID: NID:6956\n");
    log_event("SERVER START: Port %d", PORT);

    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept failed");
            continue;
        }
        pthread_t thread_id;
        int *client_sock = malloc(sizeof(int));
        *client_sock = new_socket;
        if (pthread_create(&thread_id, NULL, handle_client, (void *)client_sock) != 0) {
            perror("pthread_create failed");
            close(new_socket);
            free(client_sock);
        }
        pthread_detach(thread_id);
    }
    return 0;
}
