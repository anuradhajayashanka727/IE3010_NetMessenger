#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

// --- PERSONALISED VALUES FOR IT23695634 ---
#define SERVER_IP "127.0.0.1"
#define PORT 11634
#define NID_TAG "NID:6956"
#define REGNO "IT23695634"
#define DOWNLOAD_DIR "./downloads_IT23695634"
#define MAX_LINE 4096
#define MAX_USERNAME 32
#define MAX_FILENAME 256
#define MAX_FILE_SIZE (10ULL * 1024ULL * 1024ULL)

static int sockfd = -1;
static volatile int running = 1;
static pthread_mutex_t send_mu = PTHREAD_MUTEX_INITIALIZER;
static char my_username[64] = {0};

// ---------- UTILITIES ----------
static int send_all(int fd, const void *data, size_t len) {
    const char *p = data;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) return -1;
        p += n; len -= (size_t)n;
    }
    return 0;
}

static int send_line(const char *line) {
    pthread_mutex_lock(&send_mu);
    int rc = send_all(sockfd, line, strlen(line));
    pthread_mutex_unlock(&send_mu);
    return rc;
}

static void mkdir_if_missing(const char *path) {
    if (mkdir(path, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "Warning: could not create directory %s\n", path);
    }
}

// ---------- BUFFERED EXACT RECEIVE ----------
// TCP is a byte stream. The same recv() may contain the MSG FILE header
// and some of the file bytes. Those bytes remain in rxbuf, so the file
// receiver must consume buffered bytes before reading from the socket again.
static int recv_exact_buffered(char *rxbuf,
                               size_t *rx_used,
                               void *buffer,
                               size_t len) {
    unsigned char *out = (unsigned char *)buffer;

    while (len > 0) {
        // First consume bytes already buffered after the header.
        if (*rx_used > 0) {
            size_t take = (*rx_used < len) ? *rx_used : len;

            memcpy(out, rxbuf, take);
            memmove(rxbuf, rxbuf + take, *rx_used - take);

            *rx_used -= take;
            out += take;
            len -= take;
            continue;
        }

        // Nothing buffered: receive directly from the TCP socket.
        ssize_t n = recv(sockfd, out, len, 0);

        if (n == 0)
            return -1;

        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        out += n;
        len -= (size_t)n;
    }

    return 0;
}

static int discard_exact_buffered(char *rxbuf,
                                  size_t *rx_used,
                                  unsigned long long len) {
    unsigned char temp[4096];

    while (len > 0) {
        size_t want = (len > sizeof(temp)) ? sizeof(temp) : (size_t)len;

        if (recv_exact_buffered(rxbuf, rx_used, temp, want) < 0)
            return -1;

        len -= (unsigned long long)want;
    }

    return 0;
}

// ---------- FILE RECEIVE HANDLER ----------
// Called from the receiver thread when "MSG FILE <sender> <filename> <size>" is seen.
static void receive_file(const char *sender,
                         const char *filename,
                         unsigned long long filesize,
                         char *rxbuf,
                         size_t *rx_used) {
    // Validate filename
    if (strlen(filename) == 0 || strlen(filename) >= MAX_FILENAME ||
        strcmp(filename, ".") == 0 || strcmp(filename, "..") == 0 ||
        strchr(filename, '/') || strchr(filename, '\\')) {
        fprintf(stderr, "\n[!] Rejecting file with invalid name.\n");

        if (discard_exact_buffered(rxbuf, rx_used, filesize) < 0)
            running = 0;

        return;
    }

    if (filesize > MAX_FILE_SIZE) {
        fprintf(stderr, "\n[!] Rejecting file larger than 10 MB.\n");

        if (discard_exact_buffered(rxbuf, rx_used, filesize) < 0)
            running = 0;

        return;
    }

    mkdir_if_missing(DOWNLOAD_DIR);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", DOWNLOAD_DIR, filename);

    FILE *fp = fopen(path, "wb");
    if (!fp) {
        fprintf(stderr, "\n[!] Could not open %s for writing.\n", path);

        if (discard_exact_buffered(rxbuf, rx_used, filesize) < 0)
            running = 0;

        return;
    }

    unsigned char buf[4096];
    unsigned long long left = filesize;

    while (left > 0) {
        size_t want = left > sizeof(buf) ? sizeof(buf) : (size_t)left;

        if (recv_exact_buffered(rxbuf, rx_used, buf, want) < 0) {
            fprintf(stderr, "\n[!] File receive failed.\n");
            fclose(fp);
            remove(path);
            running = 0;
            return;
        }

        if (fwrite(buf, 1, want, fp) != want) {
            fprintf(stderr, "\n[!] File write error.\n");
            fclose(fp);
            remove(path);
            running = 0;
            return;
        }

        left -= (unsigned long long)want;
    }

    fclose(fp);
    printf("\n[+] File received from %s: %s (%llu bytes) -> %s\n> ",
           sender, filename, filesize, path);
    fflush(stdout);
}

// ---------- RECEIVER THREAD ----------
// Reads the incoming TCP stream line by line, but switches to raw-byte mode
// when it detects "MSG FILE ..." in order to consume exactly <filesize> bytes.
static void *receiver_thread(void *arg) {
    (void)arg;
    char rxbuf[MAX_LINE * 2];
    size_t rx_used = 0;

    while (running) {
        // If a newline is already in the buffer, process it first
        char *nl = memchr(rxbuf, '\n', rx_used);
        if (nl) {
            size_t line_len = nl - rxbuf;
            char line[MAX_LINE];
            if (line_len >= sizeof(line)) line_len = sizeof(line) - 1;
            memcpy(line, rxbuf, line_len);
            line[line_len] = '\0';
            // Remove the consumed line from the buffer
            size_t consumed = (nl - rxbuf) + 1;
            memmove(rxbuf, rxbuf + consumed, rx_used - consumed);
            rx_used -= consumed;

            // Strip trailing \r if present
            size_t ll = strlen(line);
            if (ll > 0 && line[ll - 1] == '\r') line[ll - 1] = '\0';

            // Check for MSG FILE
            if (strncmp(line, "MSG FILE ", 9) == 0) {
                char sender[64], filename[MAX_FILENAME];
                unsigned long long filesize;
                if (sscanf(line + 9, "%63s %255s %llu", sender, filename, &filesize) == 3) {
                    receive_file(sender,
                                 filename,
                                 filesize,
                                 rxbuf,
                                 &rx_used);
                }
                continue;
            }

            // Normal message
            printf("\n<< %s\n> ", line);
            fflush(stdout);
            continue;
        }

        // Buffer is full but no newline — this shouldn't happen in practice
        if (rx_used >= sizeof(rxbuf)) {
            fprintf(stderr, "\n[!] Receive buffer overflow. Disconnecting.\n");
            running = 0;
            break;
        }

        // Read more data
        ssize_t n = recv(sockfd, rxbuf + rx_used, sizeof(rxbuf) - rx_used, 0);
        if (n == 0) {
            printf("\n[*] Server closed the connection.\n");
            running = 0;
            break;
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv");
            running = 0;
            break;
        }
        rx_used += (size_t)n;
    }
    return NULL;
}

// ---------- MENU / USER COMMAND HANDLING ----------
static void print_menu(void) {
    printf("\n=========================================================\n");
    printf("           NetMessenger Client - IT23695634\n");
    printf("=========================================================\n");
    printf("Commands:\n");
    printf("  /register <username>            - Register with the server\n");
    printf("  /list                           - List connected users\n");
    printf("  /bcast <message>                - Broadcast to all users\n");
    printf("  /pmsg <user> <message>          - Send private message\n");
    printf("  /join <room>                    - Join or create a room\n");
    printf("  /leave <room>                   - Leave a room\n");
    printf("  /rooms                          - List rooms\n");
    printf("  /rmsg <room> <message>          - Send message to a room\n");
    printf("  /sendfile <user|room> <path>    - Send a file to a user or room\n");
    printf("  /quit                           - Disconnect and exit\n");
    printf("=========================================================\n");
}

static int handle_local_command(char *line) {
    // Strip newline
    size_t n = strlen(line);
    while (n > 0 && (line[n-1] == '\n' || line[n-1] == '\r')) line[--n] = '\0';
    if (n == 0) return 0;

    if (strcmp(line, "/quit") == 0) {
        send_line("QUIT\n");
        running = 0;
        return 1;
    }

    if (strcmp(line, "/help") == 0) {
        print_menu();
        return 1;
    }

    if (strcmp(line, "/list") == 0) {
        send_line("LIST\n");
        return 1;
    }

    if (strcmp(line, "/rooms") == 0) {
        send_line("ROOMS\n");
        return 1;
    }

    if (strncmp(line, "/register ", 10) == 0) {
        const char *user = line + 10;
        // Skip spaces
        while (*user == ' ') user++;
        if (*user == '\0') {
            printf("[!] Usage: /register <username>\n");
            return 1;
        }

        if (strlen(user) >= MAX_USERNAME) {
            printf("[!] Username too long. Maximum is %d characters.\n",
                   MAX_USERNAME - 1);
            return 1;
        }

        char cmd[MAX_LINE];
        snprintf(cmd, sizeof(cmd), "REGISTER %s\n", user);

        if (send_line(cmd) < 0) {
            printf("[!] Failed to send REGISTER command.\n");
            return 1;
        }

        memcpy(my_username, user, strlen(user) + 1);
        return 1;
    }

    if (strncmp(line, "/bcast ", 7) == 0) {
        char cmd[MAX_LINE];
        snprintf(cmd, sizeof(cmd), "BCAST %s\n", line + 7);
        send_line(cmd);
        return 1;
    }

    if (strncmp(line, "/pmsg ", 6) == 0) {
        char cmd[MAX_LINE];
        snprintf(cmd, sizeof(cmd), "PMSG %s\n", line + 6);
        send_line(cmd);
        return 1;
    }

    if (strncmp(line, "/join ", 6) == 0) {
        char cmd[MAX_LINE];
        snprintf(cmd, sizeof(cmd), "JOIN %s\n", line + 6);
        send_line(cmd);
        return 1;
    }

    if (strncmp(line, "/leave ", 7) == 0) {
        char cmd[MAX_LINE];
        snprintf(cmd, sizeof(cmd), "LEAVE %s\n", line + 7);
        send_line(cmd);
        return 1;
    }

    if (strncmp(line, "/rmsg ", 6) == 0) {
        char cmd[MAX_LINE];
        snprintf(cmd, sizeof(cmd), "RMSG %s\n", line + 6);
        send_line(cmd);
        return 1;
    }

    if (strncmp(line, "/sendfile ", 10) == 0) {
        // Format: /sendfile <target> <local_path>
        char target[64], path[512];
        if (sscanf(line + 10, "%63s %511[^\n]", target, path) != 2) {
            printf("[!] Usage: /sendfile <user|room> <local_path>\n");
            return 1;
        }

        FILE *fp = fopen(path, "rb");
        if (!fp) {
            printf("[!] Cannot open file: %s\n", path);
            return 1;
        }

        // Get file size
        fseek(fp, 0, SEEK_END);
        long size = ftell(fp);
        fseek(fp, 0, SEEK_SET);
        if (size < 0) {
            printf("[!] Could not determine file size.\n");
            fclose(fp);
            return 1;
        }
        if ((unsigned long long)size > MAX_FILE_SIZE) {
            printf("[!] File too large (max 10 MB).\n");
            fclose(fp);
            return 1;
        }

        // Extract just the basename from the path
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;

        char header[MAX_LINE];
        snprintf(header, sizeof(header), "SENDFILE %s %s %ld\n", target, base, size);

        pthread_mutex_lock(&send_mu);
        send_all(sockfd, header, strlen(header));

        // Stream the file bytes
        unsigned char buf[8192];
        size_t r;
        while ((r = fread(buf, 1, sizeof(buf), fp)) > 0) {
            if (send_all(sockfd, buf, r) < 0) break;
        }
        pthread_mutex_unlock(&send_mu);

        fclose(fp);
        printf("[+] File sent: %s -> %s (%ld bytes)\n", base, target, size);
        return 1;
    }

    if (line[0] == '/') {
        printf("[!] Unknown command. Type /help to see available commands.\n");
        return 1;
    }

    // Not a command — ignore silently (or you could broadcast raw text)
    printf("[!] Prefix your message with a command (e.g., /bcast, /pmsg). Type /help.\n");
    return 1;
}

// ---------- MAIN ----------
int main(void) {
    signal(SIGPIPE, SIG_IGN);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return 1; }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, SERVER_IP, &addr.sin_addr) <= 0) {
        perror("inet_pton"); return 1;
    }

    if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect"); return 1;
    }

    printf("Connected to NetMessenger server at %s:%d\n", SERVER_IP, PORT);
    printf("Your client is personalised for %s (NID:6956)\n", REGNO);
    print_menu();

    // Start receiver thread
    pthread_t rtid;
    if (pthread_create(&rtid, NULL, receiver_thread, NULL) != 0) {
        perror("pthread_create");
        close(sockfd);
        return 1;
    }

    // Main thread: read user input
    char line[MAX_LINE];
    while (running) {
        printf("> ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;
        handle_local_command(line);
    }

    running = 0;
    shutdown(sockfd, SHUT_RDWR);
    pthread_join(rtid, NULL);
    close(sockfd);
    printf("Disconnected. Goodbye!\n");
    return 0;
}
