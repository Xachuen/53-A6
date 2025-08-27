// Julia Tjia 38516512
// Xavier Chung 41265012

#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_LINE_LENGTH 4096
#define MAX_TRANSFER_BYTES 200

// socket/read stuff

ssize_t socket_read_line(int socket_fd, char *buffer, size_t max_bytes) {
  size_t used = 0;
  while (used + 1 < max_bytes) {
    char ch;
    ssize_t n = recv(socket_fd, &ch, 1, 0);
    if (n == 0)
      break;
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return -1;
    }
    buffer[used++] = ch;
    if (ch == '\n')
      break;
  }
  buffer[used] = '\0';
  return (ssize_t)used;
}

ssize_t socket_read_exact(int socket_fd, void *buffer, size_t desired_bytes) {
  char *out = buffer;
  size_t left = desired_bytes;
  while (left > 0) {
    ssize_t n = recv(socket_fd, out, left, 0);
    if (n == 0)
      return (ssize_t)(desired_bytes - left);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return -1;
    }
    out += n;
    left -= (size_t)n;
  }
  return (ssize_t)desired_bytes;
}

ssize_t socket_write_all(int socket_fd, const void *buffer,
                         size_t total_bytes) {
  const char *p = buffer;
  size_t left = total_bytes;
  while (left > 0) {
    ssize_t n = send(socket_fd, p, left, 0);
    if (n <= 0) {
      if (n < 0 && errno == EINTR)
        continue;
      return -1;
    }
    p += n;
    left -= (size_t)n;
  }
  return (ssize_t)total_bytes;
}

void print_prompt() {
  fputs("> ", stdout);
  fflush(stdout);
}

int main(int argc, char **argv) {
  if (argc != 3) {
    fprintf(stderr, "usage: %s <host> <port>\n", argv[0]);
    return 1;
  }
  const char *server_host = argv[1];
  const char *server_port = argv[2];

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *alist = NULL;
  int rc = getaddrinfo(server_host, server_port, &hints, &alist);
  if (rc != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
    return 1;
  }

  int sock = -1;
  for (struct addrinfo *it = alist; it; it = it->ai_next) {
    sock = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (sock < 0)
      continue;
    if (connect(sock, it->ai_addr, it->ai_addrlen) == 0)
      break;
    close(sock);
    sock = -1;
  }
  freeaddrinfo(alist);
  if (sock < 0) {
    perror("connect");
    return 1;
  }

  char user_line[MAX_LINE_LENGTH];
  char server_line[MAX_LINE_LENGTH];

  for (;;) {
    print_prompt();
    if (!fgets(user_line, sizeof(user_line), stdin))
      break;

    size_t n = strlen(user_line);
    if (n == 0)
      continue;
    if (user_line[n - 1] != '\n') {
      if (n + 1 < sizeof(user_line)) {
        user_line[n++] = '\n';
        user_line[n] = '\0';
      }
    }

    if (socket_write_all(sock, user_line, n) < 0) {
      perror("send");
      close(sock);
      return 1;
    }

    if (strncmp(user_line, "read ", 5) == 0) {
      ssize_t header_n =
          socket_read_line(sock, server_line, sizeof(server_line));
      if (header_n <= 0) {
        fprintf(stderr, "\nconnection lost\n");
        close(sock);
        return 1;
      }

      if (strncmp(server_line, "ERR ", 4) == 0) {
        fputs(server_line, stdout);
        continue;
      }

      size_t want = 0;
      if (sscanf(server_line, "DATA %zu", &want) != 1) {
        fprintf(stderr, "protocol error: %s", server_line);
        close(sock);
        return 1;
      }

      if (want > 0) {
        size_t remaining = want;
        char chunk[MAX_TRANSFER_BYTES];
        int last = -1;

        while (remaining > 0) {
          size_t take = remaining > sizeof(chunk) ? sizeof(chunk) : remaining;
          ssize_t got = socket_read_exact(sock, chunk, take);
          if (got <= 0) {
            fprintf(stderr, "\nconnection lost\n");
            close(sock);
            return 1;
          }
          fwrite(chunk, 1, (size_t)got, stdout);
          last = (unsigned char)chunk[got - 1];
          remaining -= (size_t)got;
        }

        if (last != '\n')
          fputc('\n', stdout);
      }
      fflush(stdout);
    } else {
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(sock, &rfds);
      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 100000;

      int ready = select(sock + 1, &rfds, NULL, NULL, &tv);
      if (ready > 0 && FD_ISSET(sock, &rfds)) {

        ssize_t k = socket_read_line(sock, server_line, sizeof(server_line));
        if (k <= 0) {
          close(sock);
          return 1;
        }
        if (strncmp(server_line, "ERR ", 4) == 0) {
          fputs(server_line, stdout);

        } else if (strncmp(server_line, "DATA ", 5) == 0) {
          size_t payload = 0;
          sscanf(server_line, "DATA %zu", &payload);
          while (payload > 0) {
            char sink[256];
            size_t take = payload > sizeof(sink) ? sizeof(sink) : payload;
            ssize_t got = socket_read_exact(sock, sink, take);
            if (got <= 0) {
              close(sock);
              return 1;
            }
            payload -= (size_t)got;
          }
        }
      }
    }

    if (strncmp(user_line, "quit", 4) == 0)
      break;
  }

  close(sock);
  return 0;
}
