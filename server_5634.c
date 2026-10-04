#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>

#define PORT 11634
#define BACKLOG 10

// Function to handle each client (we will expand this in Phase 3)
void *handle_client(void *arg) {
    int client_fd = *(int *)arg;
    free(arg);
    printf("Client connected on socket fd: %d\n", client_fd);
    
    // For now, just close the connection
    close(client_fd);
    printf("Client disconnected.\n");
    return NULL;
}

int main() {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    // 1. Create socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 2. Set socket options to reuse address and port
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }

    // 3. Bind socket to the personalised port
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // 4. Listen for incoming connections
    if (listen(server_fd, BACKLOG) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }

    printf("NetMessenger Server started on port %d\n", PORT);
    printf("Node ID: NID:6956\n");

    // 5. Accept connections in a loop
    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept failed");
            continue;
        }

        printf("New connection accepted: socket fd = %d, IP = %s, port = %d\n",
               new_socket, inet_ntoa(address.sin_addr), ntohs(address.sin_port));

        // Create a thread to handle this client
        pthread_t thread_id;
        int *client_sock = malloc(sizeof(int));
        *client_sock = new_socket;
        
        if (pthread_create(&thread_id, NULL, handle_client, (void *)client_sock) != 0) {
            perror("pthread_create failed");
            close(new_socket);
            free(client_sock);
        }
        
        pthread_detach(thread_id); // Detach thread so it cleans itself up
    }

    return 0;
}
