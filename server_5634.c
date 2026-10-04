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

// ============================================================
// PERSONALISED VALUES
// Registration Number : IT23695634
// Port                : 11634
// NID                 : 6956
// Server              : server_5634.c
// ============================================================

#define PORT 11634
#define NID_TAG "NID:6956"
#define REGNO "IT23695634"
#define LOG_FILE "netmsg_IT23695634.log"
#define STORAGE_ROOT "storage/IT23695634"

#define MAX_CLIENTS 32
#define MAX_ROOMS 32
#define MAX_ROOM_MEMBERS 32
#define MAX_NAME 32
#define MAX_LINE 4096
#define RXBUF_SIZE 8192
#define MAX_FILENAME 256
#define MAX_FILE_SIZE (10ULL * 1024ULL * 1024ULL) /* 10 MB */

#define ERR_MALFORMED 005
#define ERR_RATE_LIMIT 006
#define ERR_STORAGE 007

typedef struct Client Client;

typedef struct {
    int active;
    char name[MAX_NAME];
    Client *members[MAX_ROOM_MEMBERS];
    int member_count;
} Room;

struct Client {
    int active;
    int registered;
    int fd;
    char username[MAX_NAME];
    pthread_t thread;
    pthread_mutex_t send_mu;
    char rxbuf[RXBUF_SIZE];
    size_t rx_used;
};

static Client clients[MAX_CLIENTS];
static Room rooms[MAX_ROOMS];
static pthread_mutex_t state_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mu = PTHREAD_MUTEX_INITIALIZER;

// ============================================================
// LOGGING
// ============================================================

static void log_event(const char *fmt, ...)
{
    pthread_mutex_lock(&log_mu);

    FILE *fp = fopen(LOG_FILE, "a");

    if (fp)
    {
        char ts[64];
        time_t now = time(NULL);
        struct tm tmv;

        localtime_r(&now, &tmv);
        strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

        fprintf(fp, "[%s] ", ts);

        va_list ap;
        va_start(ap, fmt);
        vfprintf(fp, fmt, ap);
        va_end(ap);

        fputc('\n', fp);
        fclose(fp);
    }

    pthread_mutex_unlock(&log_mu);
}

// ============================================================
// SEND ALL
// ============================================================

static int send_all_fd(int fd, const void *data, size_t len)
{
    const char *p = (const char *)data;

    while (len > 0)
    {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);

        if (n < 0)
        {
            if (errno == EINTR)
                continue;

            return -1;
        }

        if (n == 0)
            return -1;

        p += n;
        len -= (size_t)n;
    }

    return 0;
}

// ============================================================
// SEND A LINE
// ============================================================

static int send_client_line(Client *c, const char *line, int add_nid)
{
    char out[MAX_LINE + 64];

    if (add_nid)
    {
        int n = snprintf(out,
                         sizeof(out),
                         "%s %s\n",
                         line,
                         NID_TAG);

        if (n < 0 || (size_t)n >= sizeof(out))
            return -1;

        pthread_mutex_lock(&c->send_mu);

        int rc = send_all_fd(c->fd,
                             out,
                             (size_t)n);

        pthread_mutex_unlock(&c->send_mu);

        return rc;
    }

    int n = snprintf(out,
                     sizeof(out),
                     "%s\n",
                     line);

    if (n < 0 || (size_t)n >= sizeof(out))
        return -1;

    pthread_mutex_lock(&c->send_mu);

    int rc = send_all_fd(c->fd,
                         out,
                         (size_t)n);

    pthread_mutex_unlock(&c->send_mu);

    return rc;
}

// ============================================================
// RESPONSE HELPERS
// ============================================================

static void reply_ok(Client *c, const char *text)
{
    char line[MAX_LINE];

    snprintf(line,
             sizeof(line),
             "OK %s",
             text);

    send_client_line(c, line, 1);
}

static void reply_err(Client *c, int code, const char *reason)
{
    char line[MAX_LINE];

    snprintf(line,
             sizeof(line),
             "ERR %03d %s",
             code,
             reason);

    send_client_line(c, line, 1);
}

// ============================================================
// RECEIVE ONE COMPLETE LINE
//
// Return values:
//   1  = complete line received
//   0  = connection closed
//  -1  = receive error
//  -2  = line/buffer too large
// ============================================================

static int recv_line(Client *c, char *line, size_t cap)
{
    for (;;)
    {
        for (size_t i = 0; i < c->rx_used; ++i)
        {
            if (c->rxbuf[i] == '\n')
            {
                size_t len = i;

                if (len > 0 && c->rxbuf[len - 1] == '\r')
                    len--;

                if (len >= cap)
                    return -2;

                memcpy(line,
                       c->rxbuf,
                       len);

                line[len] = '\0';

                size_t remain =
                    c->rx_used - (i + 1);

                memmove(c->rxbuf,
                        c->rxbuf + i + 1,
                        remain);

                c->rx_used = remain;

                return 1;
            }
        }

        if (c->rx_used == sizeof(c->rxbuf))
            return -2;

        ssize_t n = recv(c->fd,
                         c->rxbuf + c->rx_used,
                         sizeof(c->rxbuf) - c->rx_used,
                         0);

        if (n == 0)
            return 0;

        if (n < 0)
        {
            if (errno == EINTR)
                continue;

            return -1;
        }

        c->rx_used += (size_t)n;
    }
}

// ============================================================
// RECEIVE EXACTLY N BYTES
//
// First consumes bytes already buffered in c->rxbuf, then uses
// recv() until exactly len bytes have been consumed.
// ============================================================

static int recv_exact(Client *c, void *buffer, size_t len)
{
    char *out = (char *)buffer;

    while (len > 0)
    {
        if (c->rx_used > 0)
        {
            size_t take =
                (c->rx_used < len) ? c->rx_used : len;

            memcpy(out,
                   c->rxbuf,
                   take);

            memmove(c->rxbuf,
                    c->rxbuf + take,
                    c->rx_used - take);

            c->rx_used -= take;
            out += take;
            len -= take;

            continue;
        }

        ssize_t n = recv(c->fd,
                         out,
                         len,
                         0);

        if (n == 0)
            return -1;

        if (n < 0)
        {
            if (errno == EINTR)
                continue;

            return -1;
        }

        out += n;
        len -= (size_t)n;
    }

    return 0;
}

// ============================================================
// VALIDATION
// ============================================================

static int valid_name(const char *s, size_t max_len)
{
    size_t n = strlen(s);

    if (n == 0 || n >= max_len)
        return 0;

    for (size_t i = 0; i < n; ++i)
    {
        unsigned char ch = (unsigned char)s[i];

        if (!(isalnum(ch) || ch == '_' || ch == '-'))
            return 0;
    }

    return 1;
}

static int valid_filename(const char *s)
{
    size_t n = strlen(s);

    if (n == 0 || n >= MAX_FILENAME)
        return 0;

    if (strcmp(s, ".") == 0 || strcmp(s, "..") == 0)
        return 0;

    if (strchr(s, '/') || strchr(s, '\\'))
        return 0;

    return 1;
}

// ============================================================
// CLIENT / ROOM LOOKUPS
// ============================================================

static Client *find_client_locked(const char *username)
{
    for (int i = 0; i < MAX_CLIENTS; ++i)
    {
        if (clients[i].active &&
            clients[i].registered &&
            strcmp(clients[i].username, username) == 0)
        {
            return &clients[i];
        }
    }

    return NULL;
}

static Room *find_room_locked(const char *room_name)
{
    for (int i = 0; i < MAX_ROOMS; ++i)
    {
        if (rooms[i].active &&
            strcmp(rooms[i].name, room_name) == 0)
        {
            return &rooms[i];
        }
    }

    return NULL;
}

// ============================================================
// CREATE ROOM
// ============================================================

static Room *create_room_locked(const char *room_name)
{
    Room *r = find_room_locked(room_name);

    if (r)
        return r;

    if (!valid_name(room_name, MAX_NAME))
        return NULL;

    for (int i = 0; i < MAX_ROOMS; ++i)
    {
        if (!rooms[i].active)
        {
            rooms[i].active = 1;
            rooms[i].member_count = 0;

            size_t len = strlen(room_name);

            memcpy(rooms[i].name,
                   room_name,
                   len + 1);

            return &rooms[i];
        }
    }

    return NULL;
}

// ============================================================
// ROOM MEMBERSHIP
// ============================================================

static int room_has_member(const Room *r, const Client *c)
{
    for (int i = 0; i < r->member_count; ++i)
    {
        if (r->members[i] == c)
            return 1;
    }

    return 0;
}

static int room_add_locked(Room *r, Client *c)
{
    if (room_has_member(r, c))
        return 0;

    if (r->member_count >= MAX_ROOM_MEMBERS)
        return -1;

    r->members[r->member_count++] = c;

    return 0;
}

static int room_remove_locked(Room *r, Client *c)
{
    for (int i = 0; i < r->member_count; ++i)
    {
        if (r->members[i] == c)
        {
            r->members[i] =
                r->members[r->member_count - 1];

            r->member_count--;

            return 0;
        }
    }

    return -1;
}

// ============================================================
// PRESENCE BROADCAST
// ============================================================

static void broadcast_presence(Client *exclude,
                               const char *username,
                               const char *event)
{
    Client *targets[MAX_CLIENTS];
    int count = 0;

    pthread_mutex_lock(&state_mu);

    for (int i = 0; i < MAX_CLIENTS; ++i)
    {
        if (clients[i].active &&
            clients[i].registered &&
            &clients[i] != exclude)
        {
            targets[count++] = &clients[i];
        }
    }

    pthread_mutex_unlock(&state_mu);

    char msg[MAX_LINE];

    snprintf(msg,
             sizeof(msg),
             "MSG PRESENCE %s %s",
             username,
             event);

    for (int i = 0; i < count; ++i)
    {
        send_client_line(targets[i], msg, 0);
    }
}

// ============================================================
// STORAGE
// ============================================================

static int mkdir_if_missing(const char *path)
{
    if (mkdir(path, 0755) == 0)
        return 0;

    if (errno == EEXIST)
        return 0;

    return -1;
}

static int make_user_storage(const char *username,
                             char *out,
                             size_t out_sz)
{
    if (mkdir_if_missing("storage") < 0)
        return -1;

    if (mkdir_if_missing(STORAGE_ROOT) < 0)
        return -1;

    int n = snprintf(out,
                     out_sz,
                     "%s/%s",
                     STORAGE_ROOT,
                     username);

    if (n < 0 || (size_t)n >= out_sz)
        return -1;

    if (mkdir_if_missing(out) < 0)
        return -1;

    return 0;
}

// ============================================================
// LIST USERS
// ============================================================

static void send_users(Client *c)
{
    char line[MAX_LINE];

    strcpy(line, "USERS");

    pthread_mutex_lock(&state_mu);

    for (int i = 0; i < MAX_CLIENTS; ++i)
    {
        if (!clients[i].active || !clients[i].registered)
            continue;

        size_t used = strlen(line);

        if (used + 1 + strlen(clients[i].username) >= sizeof(line))
            break;

        snprintf(line + used,
                 sizeof(line) - used,
                 "%s%s",
                 (used == 5 ? " " : ","),
                 clients[i].username);
    }

    pthread_mutex_unlock(&state_mu);

    send_client_line(c, line, 1);
}

// ============================================================
// LIST ROOMS
// ============================================================

static void send_rooms(Client *c)
{
    char line[MAX_LINE];

    strcpy(line, "ROOMS");

    pthread_mutex_lock(&state_mu);

    for (int i = 0; i < MAX_ROOMS; ++i)
    {
        if (!rooms[i].active)
            continue;

        size_t used = strlen(line);

        if (used + 1 + strlen(rooms[i].name) >= sizeof(line))
            break;

        snprintf(line + used,
                 sizeof(line) - used,
                 "%s%s",
                 (used == 5 ? " " : ","),
                 rooms[i].name);
    }

    pthread_mutex_unlock(&state_mu);

    send_client_line(c, line, 1);
}

// ============================================================
// BROADCAST TEXT
// ============================================================

static void broadcast_text(Client *sender,
                           const char *message)
{
    Client *targets[MAX_CLIENTS];
    int count = 0;

    pthread_mutex_lock(&state_mu);

    for (int i = 0; i < MAX_CLIENTS; ++i)
    {
        if (clients[i].active &&
            clients[i].registered &&
            &clients[i] != sender)
        {
            targets[count++] = &clients[i];
        }
    }

    pthread_mutex_unlock(&state_mu);

    // Calculate available space for the message.
    int prefix_len = snprintf(NULL,
                              0,
                              "MSG BCAST %s ",
                              sender->username);

    if (prefix_len < 0 ||
        (size_t)prefix_len >= MAX_LINE)
    {
        reply_err(sender,
                  ERR_MALFORMED,
                  "MESSAGE_TOO_LONG");
        return;
    }

    size_t available =
        MAX_LINE - (size_t)prefix_len - 1;

    size_t message_len = strlen(message);

    if (message_len > available)
    {
        reply_err(sender,
                  ERR_MALFORMED,
                  "MESSAGE_TOO_LONG");
        return;
    }

    char line[MAX_LINE];

    int n = snprintf(line,
                     sizeof(line),
                     "MSG BCAST %s ",
                     sender->username);

    if (n < 0 || (size_t)n >= sizeof(line))
    {
        reply_err(sender, ERR_MALFORMED, "MESSAGE_TOO_LONG");
        return;
    }

    memcpy(line + n, message, message_len);
    line[n + message_len] = '\0';

    for (int i = 0; i < count; ++i)
    {
        send_client_line(targets[i], line, 0);
    }
}

// ============================================================
// PRIVATE MESSAGE
// ============================================================

static void private_text(Client *sender,
                         const char *target_name,
                         const char *message)
{
    Client *target = NULL;

    pthread_mutex_lock(&state_mu);

    target = find_client_locked(target_name);

    pthread_mutex_unlock(&state_mu);

    if (!target)
    {
        reply_err(sender,
                  2,
                  "USER_NOT_FOUND");
        return;
    }

    int prefix_len = snprintf(NULL,
                              0,
                              "MSG PRIV %s ",
                              sender->username);

    if (prefix_len < 0 ||
        (size_t)prefix_len >= MAX_LINE)
    {
        reply_err(sender,
                  ERR_MALFORMED,
                  "MESSAGE_TOO_LONG");
        return;
    }

    size_t available =
        MAX_LINE - (size_t)prefix_len - 1;

    if (strlen(message) > available)
    {
        reply_err(sender,
                  ERR_MALFORMED,
                  "MESSAGE_TOO_LONG");
        return;
    }

    char line[MAX_LINE];

    int n = snprintf(line,
                     sizeof(line),
                     "MSG PRIV %s ",
                     sender->username);

    if (n < 0 || (size_t)n >= sizeof(line))
    {
        reply_err(sender, ERR_MALFORMED, "MESSAGE_TOO_LONG");
        return;
    }

    size_t message_len = strlen(message);

    if (message_len > MAX_LINE - (size_t)n - 1)
    {
        reply_err(sender, ERR_MALFORMED, "MESSAGE_TOO_LONG");
        return;
    }

    memcpy(line + n, message, message_len);
    line[n + message_len] = '\0';

    send_client_line(target,
                     line,
                     0);

    reply_ok(sender, "SENT");
}

// ============================================================
// ROOM MESSAGE
// ============================================================

static void room_text(Client *sender,
                      const char *room_name,
                      const char *message)
{
    Client *targets[MAX_ROOM_MEMBERS];
    int count = 0;
    Room *r = NULL;

    pthread_mutex_lock(&state_mu);

    r = find_room_locked(room_name);

    if (r)
    {
        for (int i = 0; i < r->member_count; ++i)
        {
            targets[count++] = r->members[i];
        }
    }

    pthread_mutex_unlock(&state_mu);

    if (!r)
    {
        reply_err(sender,
                  3,
                  "ROOM_NOT_FOUND");
        return;
    }

    int prefix_len = snprintf(NULL,
                              0,
                              "MSG ROOM %s %s ",
                              room_name,
                              sender->username);

    if (prefix_len < 0 ||
        (size_t)prefix_len >= MAX_LINE)
    {
        reply_err(sender,
                  ERR_MALFORMED,
                  "MESSAGE_TOO_LONG");
        return;
    }

    size_t available =
        MAX_LINE - (size_t)prefix_len - 1;

    if (strlen(message) > available)
    {
        reply_err(sender,
                  ERR_MALFORMED,
                  "MESSAGE_TOO_LONG");
        return;
    }

    char line[MAX_LINE];

    int n = snprintf(line,
                     sizeof(line),
                     "MSG ROOM %s %s ",
                     room_name,
                     sender->username);

    if (n < 0 || (size_t)n >= sizeof(line))
    {
        reply_err(sender, ERR_MALFORMED, "MESSAGE_TOO_LONG");
        return;
    }

    size_t message_len = strlen(message);

    if (message_len > MAX_LINE - (size_t)n - 1)
    {
        reply_err(sender, ERR_MALFORMED, "MESSAGE_TOO_LONG");
        return;
    }

    memcpy(line + n, message, message_len);
    line[n + message_len] = '\0';

    for (int i = 0; i < count; ++i)
    {
        send_client_line(targets[i],
                         line,
                         0);
    }

    reply_ok(sender, "SENT");
}

// ============================================================
// SEND FILE TO ONE CLIENT
// ============================================================

static int send_file_to(Client *target,
                        const char *sender_name,
                        const char *filename,
                        unsigned long long filesize,
                        const char *path)
{
    FILE *fp = fopen(path, "rb");

    if (!fp)
        return -1;

    char header[MAX_LINE];

    int n = snprintf(header,
                     sizeof(header),
                     "MSG FILE %s %s %llu\n",
                     sender_name,
                     filename,
                     filesize);

    if (n < 0 || (size_t)n >= sizeof(header))
    {
        fclose(fp);
        return -1;
    }

    pthread_mutex_lock(&target->send_mu);

    int rc = send_all_fd(target->fd,
                         header,
                         (size_t)n);

    unsigned char buf[8192];

    unsigned long long remaining = filesize;

    while (rc == 0 && remaining > 0)
    {
        size_t want =
            (remaining > sizeof(buf))
                ? sizeof(buf)
                : (size_t)remaining;

        size_t got = fread(buf,
                           1,
                           want,
                           fp);

        if (got == 0)
        {
            rc = -1;
            break;
        }

        if (send_all_fd(target->fd,
                        buf,
                        got) < 0)
        {
            rc = -1;
            break;
        }

        remaining -= got;
    }

    pthread_mutex_unlock(&target->send_mu);

    fclose(fp);

    return rc;
}

// ============================================================
// DRAIN EXACT FILE BYTES
// Used when a file has to be rejected after its header arrived.
// ============================================================

static int drain_file_bytes(Client *c,
                            unsigned long long filesize)
{
    unsigned char buf[8192];

    unsigned long long remaining = filesize;

    while (remaining > 0)
    {
        size_t want =
            (remaining > sizeof(buf))
                ? sizeof(buf)
                : (size_t)remaining;

        if (recv_exact(c,
                       buf,
                       want) < 0)
        {
            return -1;
        }

        remaining -= (unsigned long long)want;
    }

    return 0;
}

// ============================================================
// HANDLE FILE TRANSFER
// ============================================================

static void handle_sendfile(Client *sender,
                            const char *args)
{
    char target[MAX_NAME];
    char filename[MAX_FILENAME];
    unsigned long long filesize;

    if (sscanf(args,
               "%31s %255s %llu",
               target,
               filename,
               &filesize) != 3 ||
        !valid_name(target, MAX_NAME) ||
        !valid_filename(filename))
    {
        reply_err(sender,
                  ERR_MALFORMED,
                  "MALFORMED_SENDFILE");
        return;
    }

    // --------------------------------------------------------
    // The file header has already been received. If we reject
    // based on size, drain the announced bytes to preserve the
    // TCP stream framing.
    // --------------------------------------------------------

    if (filesize > MAX_FILE_SIZE)
    {
        if (drain_file_bytes(sender, filesize) < 0)
        {
            return;
        }

        reply_err(sender,
                  4,
                  "FILE_TOO_LARGE");
        return;
    }

    // --------------------------------------------------------
    // Find target user or room.
    // --------------------------------------------------------

    Client *client_target = NULL;
    Room *room_target = NULL;

    pthread_mutex_lock(&state_mu);

    client_target = find_client_locked(target);

    if (!client_target)
        room_target = find_room_locked(target);

    pthread_mutex_unlock(&state_mu);

    if (!client_target && !room_target)
    {
        if (drain_file_bytes(sender, filesize) < 0)
        {
            return;
        }

        reply_err(sender,
                  2,
                  "USER_NOT_FOUND");
        return;
    }

    // --------------------------------------------------------
    // Create sender storage directory.
    // --------------------------------------------------------

    char user_dir[512];

    if (make_user_storage(sender->username,
                          user_dir,
                          sizeof(user_dir)) < 0)
    {
        if (drain_file_bytes(sender, filesize) < 0)
        {
            return;
        }

        reply_err(sender,
                  ERR_STORAGE,
                  "STORAGE_ERROR");
        return;
    }

    // --------------------------------------------------------
    // Build output path.
    // --------------------------------------------------------

    char path[768];

    int pn = snprintf(path,
                      sizeof(path),
                      "%s/%s",
                      user_dir,
                      filename);

    if (pn < 0 || (size_t)pn >= sizeof(path))
    {
        if (drain_file_bytes(sender, filesize) < 0)
        {
            return;
        }

        reply_err(sender,
                  ERR_STORAGE,
                  "STORAGE_ERROR");
        return;
    }

    // --------------------------------------------------------
    // Open destination file.
    // --------------------------------------------------------

    FILE *fp = fopen(path, "wb");

    if (!fp)
    {
        if (drain_file_bytes(sender, filesize) < 0)
        {
            return;
        }

        reply_err(sender,
                  ERR_STORAGE,
                  "STORAGE_ERROR");
        return;
    }

    // --------------------------------------------------------
    // Receive EXACTLY filesize bytes from sender.
    // --------------------------------------------------------

    unsigned char buf[8192];

    unsigned long long left = filesize;
    int write_ok = 1;

    while (left > 0)
    {
        size_t take =
            (left > sizeof(buf))
                ? sizeof(buf)
                : (size_t)left;

        if (recv_exact(sender,
                       buf,
                       take) < 0)
        {
            fclose(fp);
            unlink(path);
            return;
        }

        if (write_ok &&
            fwrite(buf,
                   1,
                   take,
                   fp) != take)
        {
            write_ok = 0;
        }

        left -= (unsigned long long)take;
    }

    fclose(fp);

    if (!write_ok)
    {
        unlink(path);

        reply_err(sender,
                  ERR_STORAGE,
                  "STORAGE_ERROR");
        return;
    }

    // --------------------------------------------------------
    // Confirm receipt to sender.
    // --------------------------------------------------------

    char response[MAX_LINE];

    snprintf(response,
             sizeof(response),
             "OK FILE_RECEIVED %s",
             filename);

    send_client_line(sender,
                     response,
                     1);

    // --------------------------------------------------------
    // Deliver to a single user.
    // --------------------------------------------------------

    if (client_target)
    {
        send_file_to(client_target,
                     sender->username,
                     filename,
                     filesize,
                     path);
    }

    // --------------------------------------------------------
    // Deliver to every member of a room.
    // --------------------------------------------------------

    else if (room_target)
    {
        Client *targets[MAX_ROOM_MEMBERS];
        int count = 0;

        pthread_mutex_lock(&state_mu);

        for (int i = 0;
             i < room_target->member_count;
             ++i)
        {
            targets[count++] =
                room_target->members[i];
        }

        pthread_mutex_unlock(&state_mu);

        for (int i = 0; i < count; ++i)
        {
            send_file_to(targets[i],
                         sender->username,
                         filename,
                         filesize,
                         path);
        }
    }

    log_event("SENDFILE: %s -> %s: '%s' (%llu bytes)",
              sender->username,
              target,
              filename,
              filesize);
}

// ============================================================
// REGISTER CLIENT
// ============================================================

static int register_client(Client *c,
                           const char *username)
{
    if (!valid_name(username, MAX_NAME))
        return -1;

    pthread_mutex_lock(&state_mu);

    if (find_client_locked(username) != NULL)
    {
        pthread_mutex_unlock(&state_mu);
        return 1;
    }

    c->registered = 1;

    snprintf(c->username,
             sizeof(c->username),
             "%s",
             username);

    pthread_mutex_unlock(&state_mu);

    return 0;
}

// ============================================================
// CLEANUP CLIENT
// ============================================================

static void cleanup_client(Client *c)
{
    char left_name[MAX_NAME];
    int had_registration = 0;

    pthread_mutex_lock(&state_mu);

    if (c->registered)
    {
        had_registration = 1;

        snprintf(left_name,
                 sizeof(left_name),
                 "%s",
                 c->username);

        for (int i = 0; i < MAX_ROOMS; ++i)
        {
            if (!rooms[i].active)
                continue;

            room_remove_locked(&rooms[i], c);
        }
    }

    pthread_mutex_unlock(&state_mu);

    if (had_registration)
    {
        log_event("DISCONNECT: User '%s' disconnected.",
                  left_name);

        broadcast_presence(c,
                           left_name,
                           "LEFT");
    }

    close(c->fd);

    pthread_mutex_lock(&state_mu);

    c->active = 0;
    c->registered = 0;
    c->fd = -1;
    c->username[0] = '\0';
    c->rx_used = 0;

    pthread_mutex_unlock(&state_mu);
}

// ============================================================
// CLIENT THREAD
// ============================================================

static void *client_thread(void *arg)
{
    Client *c = (Client *)arg;

    char line[MAX_LINE];

    // --------------------------------------------------------
    // First command MUST be REGISTER.
    // --------------------------------------------------------

    int rc = recv_line(c,
                       line,
                       sizeof(line));

    if (rc <= 0)
    {
        cleanup_client(c);
        return NULL;
    }

    if (rc == -2)
    {
        reply_err(c,
                  ERR_MALFORMED,
                  "LINE_TOO_LONG");

        cleanup_client(c);
        return NULL;
    }

    char username[MAX_NAME];

    char extra[MAX_NAME];

    int fields = sscanf(line,
                        "REGISTER %31s %31s",
                        username,
                        extra);

    if (fields != 1)
    {
        reply_err(c,
                  ERR_MALFORMED,
                  "REGISTER_REQUIRED");

        cleanup_client(c);
        return NULL;
    }

    int reg_rc = register_client(c,
                                 username);

    if (reg_rc == 1)
    {
        reply_err(c,
                  1,
                  "USERNAME_TAKEN");

        cleanup_client(c);
        return NULL;
    }

    if (reg_rc < 0)
    {
        reply_err(c,
                  ERR_MALFORMED,
                  "INVALID_USERNAME");

        cleanup_client(c);
        return NULL;
    }

    char regmsg[MAX_LINE];

    snprintf(regmsg,
             sizeof(regmsg),
             "OK REGISTERED %s",
             c->username);

    send_client_line(c,
                     regmsg,
                     1);

    log_event("REGISTER: User '%s' registered.",
              c->username);

    broadcast_presence(NULL,
                       c->username,
                       "JOINED");

    // --------------------------------------------------------
    // Process commands.
    // --------------------------------------------------------

    for (;;)
    {
        rc = recv_line(c,
                       line,
                       sizeof(line));

        if (rc == 0)
            break;

        if (rc == -2)
        {
            reply_err(c,
                      ERR_MALFORMED,
                      "LINE_TOO_LONG");
            break;
        }

        if (rc < 0)
        {
            reply_err(c,
                      ERR_MALFORMED,
                      "FRAMING_ERROR");
            break;
        }

        if (line[0] == '\0')
            continue;

        // ----------------------------------------------------
        // LIST USERS
        // ----------------------------------------------------

        if (strcmp(line, "LIST") == 0)
        {
            send_users(c);
        }

        // ----------------------------------------------------
        // BROADCAST
        // ----------------------------------------------------

        else if (strncmp(line,
                         "BCAST ",
                         6) == 0)
        {
            const char *msg = line + 6;

            if (*msg == '\0')
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "EMPTY_MESSAGE");
                continue;
            }

            broadcast_text(c,
                           msg);

            /* If broadcast_text rejects an oversized message it
               sends the error itself; otherwise send OK. */
            int prefix_len = snprintf(NULL,
                                      0,
                                      "MSG BCAST %s ",
                                      c->username);

            size_t available =
                (prefix_len >= 0 &&
                 (size_t)prefix_len < MAX_LINE)
                    ? MAX_LINE - (size_t)prefix_len - 1
                    : 0;

            if (strlen(msg) <= available)
            {
                reply_ok(c, "SENT");

                log_event("BCAST: %s -> '%s'",
                          c->username,
                          msg);
            }
        }

        // ----------------------------------------------------
        // PRIVATE MESSAGE
        // ----------------------------------------------------

        else if (strncmp(line,
                         "PMSG ",
                         5) == 0)
        {
            char target[MAX_NAME];

            const char *rest = line + 5;

            const char *sp = strchr(rest,
                                    ' ');

            if (!sp)
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MALFORMED_PMSG");
                continue;
            }

            size_t n =
                (size_t)(sp - rest);

            if (n == 0 ||
                n >= sizeof(target))
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MALFORMED_PMSG");
                continue;
            }

            memcpy(target,
                   rest,
                   n);

            target[n] = '\0';

            const char *msg = sp + 1;

            if (!valid_name(target,
                            MAX_NAME) ||
                *msg == '\0')
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MALFORMED_PMSG");
                continue;
            }

            int prefix_len = snprintf(NULL,
                                      0,
                                      "MSG PRIV %s ",
                                      c->username);

            size_t available =
                (prefix_len >= 0 &&
                 (size_t)prefix_len < MAX_LINE)
                    ? MAX_LINE - (size_t)prefix_len - 1
                    : 0;

            if (strlen(msg) > available)
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MESSAGE_TOO_LONG");
                continue;
            }

            private_text(c,
                         target,
                         msg);

            log_event("PMSG: %s -> %s: '%s'",
                      c->username,
                      target,
                      msg);
        }

        // ----------------------------------------------------
        // JOIN ROOM
        // ----------------------------------------------------

        else if (strncmp(line,
                         "JOIN ",
                         5) == 0)
        {
            const char *room_arg = line + 5;

            if (!valid_name(room_arg,
                            MAX_NAME))
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "INVALID_ROOM");
                continue;
            }

            char room[MAX_NAME];

            size_t room_len = strlen(room_arg);

            memcpy(room,
                   room_arg,
                   room_len + 1);

            pthread_mutex_lock(&state_mu);

            Room *r = create_room_locked(room);

            int ok = r ? room_add_locked(r, c) : -1;

            pthread_mutex_unlock(&state_mu);

            if (ok < 0)
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "ROOM_FULL");
            }
            else
            {
                char resp[MAX_LINE];

                snprintf(resp,
                         sizeof(resp),
                         "OK JOINED %s",
                         room);

                send_client_line(c,
                                 resp,
                                 1);

                log_event("JOIN: %s joined room '%s'",
                          c->username,
                          room);
            }
        }

        // ----------------------------------------------------
        // LEAVE ROOM
        // ----------------------------------------------------

        else if (strncmp(line,
                         "LEAVE ",
                         6) == 0)
        {
            const char *room_arg = line + 6;

            if (!valid_name(room_arg,
                            MAX_NAME))
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "INVALID_ROOM");
                continue;
            }

            char room[MAX_NAME];

            size_t room_len = strlen(room_arg);

            memcpy(room,
                   room_arg,
                   room_len + 1);

            pthread_mutex_lock(&state_mu);

            Room *r = find_room_locked(room);

            int found = (r != NULL);

            if (r)
                room_remove_locked(r, c);

            pthread_mutex_unlock(&state_mu);

            if (!found)
            {
                reply_err(c,
                          3,
                          "ROOM_NOT_FOUND");
            }
            else
            {
                char resp[MAX_LINE];

                snprintf(resp,
                         sizeof(resp),
                         "OK LEFT %s",
                         room);

                send_client_line(c,
                                 resp,
                                 1);

                log_event("LEAVE: %s left room '%s'",
                          c->username,
                          room);
            }
        }

        // ----------------------------------------------------
        // LIST ROOMS
        // ----------------------------------------------------

        else if (strcmp(line,
                        "ROOMS") == 0)
        {
            send_rooms(c);
        }

        // ----------------------------------------------------
        // ROOM MESSAGE
        // ----------------------------------------------------

        else if (strncmp(line,
                         "RMSG ",
                         5) == 0)
        {
            char room[MAX_NAME];

            const char *rest = line + 5;

            const char *sp = strchr(rest,
                                    ' ');

            if (!sp)
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MALFORMED_RMSG");
                continue;
            }

            size_t n =
                (size_t)(sp - rest);

            if (n == 0 ||
                n >= sizeof(room))
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MALFORMED_RMSG");
                continue;
            }

            memcpy(room,
                   rest,
                   n);

            room[n] = '\0';

            const char *msg = sp + 1;

            if (!valid_name(room,
                            MAX_NAME) ||
                *msg == '\0')
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MALFORMED_RMSG");
                continue;
            }

            int prefix_len = snprintf(NULL,
                                      0,
                                      "MSG ROOM %s %s ",
                                      room,
                                      c->username);

            size_t available =
                (prefix_len >= 0 &&
                 (size_t)prefix_len < MAX_LINE)
                    ? MAX_LINE - (size_t)prefix_len - 1
                    : 0;

            if (strlen(msg) > available)
            {
                reply_err(c,
                          ERR_MALFORMED,
                          "MESSAGE_TOO_LONG");
                continue;
            }

            room_text(c,
                      room,
                      msg);

            log_event("RMSG: %s to room '%s': '%s'",
                      c->username,
                      room,
                      msg);
        }

        // ----------------------------------------------------
        // FILE
        // ----------------------------------------------------

        else if (strncmp(line,
                         "SENDFILE ",
                         9) == 0)
        {
            handle_sendfile(c,
                            line + 9);
        }

        // ----------------------------------------------------
        // QUIT
        // ----------------------------------------------------

        else if (strcmp(line,
                        "QUIT") == 0)
        {
            reply_ok(c,
                     "BYE");

            log_event("QUIT: %s",
                      c->username);

            break;
        }

        // ----------------------------------------------------
        // UNKNOWN COMMAND
        // ----------------------------------------------------

        else
        {
            reply_err(c,
                      ERR_MALFORMED,
                      "UNKNOWN_COMMAND");
        }
    }

    cleanup_client(c);

    return NULL;
}

// ============================================================
// MAIN SERVER
// ============================================================

int main(void)
{
    // Prevent server termination when sending to a closed socket.
    signal(SIGPIPE, SIG_IGN);

    // Initialise client slots.
    for (int i = 0; i < MAX_CLIENTS; ++i)
    {
        clients[i].fd = -1;

        pthread_mutex_init(&clients[i].send_mu,
                           NULL);
    }

    // --------------------------------------------------------
    // Create TCP socket.
    // --------------------------------------------------------

    int server_fd =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (server_fd < 0)
    {
        perror("socket");
        return 1;
    }

    // --------------------------------------------------------
    // Allow immediate reuse of the port after restart.
    // --------------------------------------------------------

    int opt = 1;

    if (setsockopt(server_fd,
                   SOL_SOCKET,
                   SO_REUSEADDR,
                   &opt,
                   sizeof(opt)) < 0)
    {
        perror("setsockopt");

        close(server_fd);

        return 1;
    }

    // --------------------------------------------------------
    // Bind.
    // --------------------------------------------------------

    struct sockaddr_in addr;

    memset(&addr,
           0,
           sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);

    if (bind(server_fd,
             (struct sockaddr *)&addr,
             sizeof(addr)) < 0)
    {
        perror("bind");

        close(server_fd);

        return 1;
    }

    // --------------------------------------------------------
    // Listen.
    // --------------------------------------------------------

    if (listen(server_fd, 16) < 0)
    {
        perror("listen");

        close(server_fd);

        return 1;
    }

    // --------------------------------------------------------
    // Create storage root.
    // --------------------------------------------------------

    if (mkdir_if_missing("storage") < 0 ||
        mkdir_if_missing(STORAGE_ROOT) < 0)
    {
        perror("storage");

        close(server_fd);

        return 1;
    }

    // --------------------------------------------------------
    // Log and display startup details.
    // --------------------------------------------------------

    log_event("SERVER START: Port %d", PORT);

    printf("NetMessenger Server started on port %d\n",
           PORT);

    printf("Node ID: %s\n",
           NID_TAG);

    printf("Storage root: %s\n",
           STORAGE_ROOT);

    printf("Press Ctrl+C to stop.\n");

    // --------------------------------------------------------
    // Accept clients forever.
    // --------------------------------------------------------

    for (;;)
    {
        struct sockaddr_in client_addr;

        socklen_t client_len =
            sizeof(client_addr);

        int client_fd =
            accept(server_fd,
                   (struct sockaddr *)&client_addr,
                   &client_len);

        if (client_fd < 0)
        {
            if (errno == EINTR)
                continue;

            perror("accept");
            continue;
        }

        // ----------------------------------------------------
        // Find an unused client slot.
        // ----------------------------------------------------

        pthread_mutex_lock(&state_mu);

        Client *slot = NULL;

        for (int i = 0; i < MAX_CLIENTS; ++i)
        {
            if (!clients[i].active)
            {
                slot = &clients[i];
                break;
            }
        }

        if (slot)
        {
            slot->active = 1;
            slot->registered = 0;
            slot->fd = client_fd;
            slot->username[0] = '\0';
            slot->rx_used = 0;
        }

        pthread_mutex_unlock(&state_mu);

        // ----------------------------------------------------
        // Server full.
        // ----------------------------------------------------

        if (!slot)
        {
            const char *msg =
                "ERR 005 SERVER_FULL NID:6956\n";

            send_all_fd(client_fd,
                        msg,
                        strlen(msg));

            close(client_fd);

            continue;
        }

        // ----------------------------------------------------
        // One detached pthread per client.
        // ----------------------------------------------------

        if (pthread_create(&slot->thread,
                           NULL,
                           client_thread,
                           slot) != 0)
        {
            close(client_fd);

            pthread_mutex_lock(&state_mu);

            slot->active = 0;
            slot->fd = -1;

            pthread_mutex_unlock(&state_mu);

            continue;
        }

        pthread_detach(slot->thread);
    }

    // Unreachable in normal operation.
    close(server_fd);

    return 0;
}
