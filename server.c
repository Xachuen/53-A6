// Julia Tjia 38516512
// Xavier Chung 41265012
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_LINE_LENGTH 4096
#define MAX_TRANSFER_BYTES 200

enum OpenMode { MODE_NONE = 0, MODE_READ, MODE_APPEND };

struct FileLock {
  int readers;
  int writer;
  int owner_fd;
  sem_t mutex;
  int refcount;
  char name[512];
};

struct FileNode {
  char name[512];
  struct FileLock *lock;
  struct FileNode *next;
};

struct FileNode *glob_table_head;
sem_t glob_table_sem;

int P(sem_t *s) {
  for (;;) {
    if (sem_wait(s) == 0)
      return 0;
    if (errno != EINTR)
      return -1;
  }
}
int V(sem_t *s) { return sem_post(s); }

struct FileLock *find_or_create_lock(const char *name) {
  struct FileLock *lock = NULL;
  struct FileNode *node = NULL;

  if (P(&glob_table_sem) < 0)
    return NULL;

  for (node = glob_table_head; node; node = node->next) {
    if (strcmp(node->lock->name, name) == 0) {
      lock = node->lock;
      lock->refcount++;
      V(&glob_table_sem);
      return lock;
    }
  }

  node = (struct FileNode *)malloc(sizeof(struct FileNode));
  if (!node) {
    V(&glob_table_sem);
    return NULL;
  }

  lock = (struct FileLock *)malloc(sizeof(struct FileLock));
  if (!lock) {
    free(node);
    V(&glob_table_sem);
    return NULL;
  }

  strncpy(lock->name, name, sizeof(lock->name) - 1);
  lock->name[sizeof(lock->name) - 1] = '\0';
  lock->readers = 0;
  lock->writer = 0;
  lock->owner_fd = -1;
  lock->refcount = 1;
  if (sem_init(&lock->mutex, 0, 1) != 0) {
    free(lock);
    free(node);
    V(&glob_table_sem);
    return NULL;
  }

  node->lock = lock;
  node->next = glob_table_head;
  glob_table_head = node;

  V(&glob_table_sem);
  return lock;
}

void release_lock(struct FileLock *lock) {
  if (!lock)
    return;

  if (P(&glob_table_sem) < 0)
    return;

  lock->refcount--;

  if (lock->refcount == 0 && lock->readers == 0 && lock->writer == 0) {
    struct FileNode **link_to_slot = &glob_table_head;
    while (*link_to_slot && (*link_to_slot)->lock != lock) {
      link_to_slot = &(*link_to_slot)->next;
    }
    if (*link_to_slot) {
      struct FileNode *node_to_free = *link_to_slot;
      *link_to_slot = node_to_free->next;
      V(&glob_table_sem);
      sem_destroy(&lock->mutex);
      free(lock);
      free(node_to_free);
      return;
    }
  }

  V(&glob_table_sem);
}

int try_open_read(struct FileLock *lock) {
  if (!lock)
    return -1;
  if (P(&lock->mutex) < 0)
    return -1;
  if (lock->writer) {
    V(&lock->mutex);
    return -1;
  }
  lock->readers++;
  V(&lock->mutex);
  return 0;
}

int try_open_append(struct FileLock *lock, int client_fd) {
  if (!lock)
    return -1;
  if (P(&lock->mutex) < 0)
    return -1;
  if (lock->writer || lock->readers > 0) {
    V(&lock->mutex);
    return -1;
  }
  lock->writer = 1;
  lock->owner_fd = client_fd;
  V(&lock->mutex);
  return 0;
}

void close_for(int mode, struct FileLock *lock) {
  if (!lock)
    return;
  if (P(&lock->mutex) < 0)
    return;
  if (mode == MODE_READ) {
    if (lock->readers > 0)
      lock->readers--;
  } else if (mode == MODE_APPEND) {
    lock->writer = 0;
    lock->owner_fd = -1;
  }
  V(&lock->mutex);
}

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

void remove_trailing_newline(char *s) {
  size_t n = strlen(s);
  if (n && s[n - 1] == '\n')
    s[n - 1] = '\0';
}

int send_error_line(int socket_fd, const char *message) {
  char line[256];
  int len = snprintf(line, sizeof(line), "ERR %s\n", message);
  if (len < 0)
    return -1;
  fprintf(stdout, "%s\n", message);
  fflush(stdout);
  return (int)socket_write_all(socket_fd, line, (size_t)len);
}

struct Session {
  FILE *file_stream;
  enum OpenMode open_mode;
  char filename[512];
  struct FileLock *lock;
};

void close_session_and_socket(int client_socket, struct Session *session) {
  if (session && session->open_mode != MODE_NONE) {
    if (session->file_stream)
      fclose(session->file_stream);
    close_for(session->open_mode, session->lock);
    release_lock(session->lock);
    session->file_stream = NULL;
    session->open_mode = MODE_NONE;
    session->filename[0] = '\0';
    session->lock = NULL;
  }
  close(client_socket);
}

void *client_worker_thread(void *arg) {
  int client_socket = (int)(intptr_t)arg;
  char command_line[MAX_LINE_LENGTH];

  struct Session session;
  session.file_stream = NULL;
  session.open_mode = MODE_NONE;
  session.filename[0] = '\0';
  session.lock = NULL;

  for (;;) {
    ssize_t got =
        socket_read_line(client_socket, command_line, sizeof(command_line));
    if (got <= 0) {
      close_session_and_socket(client_socket, &session);
      return NULL;
    }

    char echoed[MAX_LINE_LENGTH];
    memcpy(echoed, command_line, (size_t)got);
    echoed[got] = '\0';
    remove_trailing_newline(echoed);
    fprintf(stdout, "%s\n", echoed);
    fflush(stdout);

    char *cmd = strtok(command_line, " \t\r\n");
    if (!cmd)
      continue;

    if (strcmp(cmd, "quit") == 0) {
      close_session_and_socket(client_socket, &session);
      return NULL;

    } else if (strcmp(cmd, "openRead") == 0) {
      char *fname = strtok(NULL, "\r\n");
      if (!fname || !*fname)
        continue;

      if (session.open_mode != MODE_NONE) {
        if (session.open_mode == MODE_READ) {
          if (send_error_line(client_socket,
                              "A file is already open for reading") < 0) {
            close_session_and_socket(client_socket, &session);
            return NULL;
          }
        } else {
          if (send_error_line(client_socket, "A file is already open") < 0) {
            close_session_and_socket(client_socket, &session);
            return NULL;
          }
        }
        continue;
      }

      struct FileLock *lk = find_or_create_lock(fname);
      if (!lk)
        continue;

      if (try_open_read(lk) < 0) {
        if (send_error_line(client_socket,
                            "The file is open by another client.") < 0) {
          release_lock(lk);
          close_session_and_socket(client_socket, &session);
          return NULL;
        }
        release_lock(lk);
        continue;
      }

      FILE *fs = fopen(fname, "rb");
      if (!fs) {
        close_for(MODE_READ, lk);
        release_lock(lk);
        continue;
      }

      session.file_stream = fs;
      session.open_mode = MODE_READ;
      session.lock = lk;
      strncpy(session.filename, fname, sizeof(session.filename) - 1);
      session.filename[sizeof(session.filename) - 1] = '\0';

    } else if (strcmp(cmd, "openAppend") == 0) {
      char *fname = strtok(NULL, "\r\n");
      if (!fname || !*fname)
        continue;

      if (session.open_mode != MODE_NONE) {
        if (session.open_mode == MODE_APPEND) {
          if (send_error_line(client_socket,
                              "A file is already open for appending") < 0) {
            close_session_and_socket(client_socket, &session);
            return NULL;
          }
        } else {
          if (send_error_line(client_socket, "A file is already open") < 0) {
            close_session_and_socket(client_socket, &session);
            return NULL;
          }
        }
        continue;
      }

      struct FileLock *lk = find_or_create_lock(fname);
      if (!lk)
        continue;

      if (try_open_append(lk, client_socket) < 0) {
        if (send_error_line(client_socket,
                            "The file is open by another client.") < 0) {
          release_lock(lk);
          close_session_and_socket(client_socket, &session);
          return NULL;
        }
        release_lock(lk);
        continue;
      }

      FILE *fs = fopen(fname, "ab+");
      if (!fs) {
        close_for(MODE_APPEND, lk);
        release_lock(lk);
        continue;
      }
      fseek(fs, 0, SEEK_END);

      session.file_stream = fs;
      session.open_mode = MODE_APPEND;
      session.lock = lk;
      strncpy(session.filename, fname, sizeof(session.filename) - 1);
      session.filename[sizeof(session.filename) - 1] = '\0';

    } else if (strcmp(cmd, "read") == 0) {
      char *len_tok = strtok(NULL, " \t\r\n");
      if (!len_tok)
        continue;

      long want = strtol(len_tok, NULL, 10);
      if (want < 0)
        want = 0;
      if (want > MAX_TRANSFER_BYTES)
        want = MAX_TRANSFER_BYTES;

      if (session.open_mode != MODE_READ || !session.file_stream) {
        if (send_error_line(client_socket, "File not open") < 0) {
          close_session_and_socket(client_socket, &session);
          return NULL;
        }
        continue;
      }

      char buf[MAX_TRANSFER_BYTES];
      size_t nread = fread(buf, 1, (size_t)want, session.file_stream);

      char hdr[64];
      int hdrlen = snprintf(hdr, sizeof(hdr), "DATA %zu\n", nread);
      if (socket_write_all(client_socket, hdr, (size_t)hdrlen) < 0) {
        close_session_and_socket(client_socket, &session);
        return NULL;
      }
      if (nread > 0 && socket_write_all(client_socket, buf, nread) < 0) {
        close_session_and_socket(client_socket, &session);
        return NULL;
      }

    } else if (strcmp(cmd, "append") == 0) {
      if (session.open_mode != MODE_APPEND || !session.file_stream) {
        if (send_error_line(client_socket, "File not open") < 0) {
          close_session_and_socket(client_socket, &session);
          return NULL;
        }
        continue;
      }
      char *payload = strtok(NULL, "");
      if (!payload)
        payload = "";

      size_t plen = strcspn(payload, "\r\n");

      if (plen > MAX_TRANSFER_BYTES)
        plen = MAX_TRANSFER_BYTES;
      if (plen > 0) {
        if (fwrite(payload, 1, plen, session.file_stream) != plen) {
          if (send_error_line(client_socket, "Write failed") < 0) {
            close_session_and_socket(client_socket, &session);
            return NULL;
          }
          continue;
        }
      }
      fflush(session.file_stream);

    } else if (strcmp(cmd, "close") == 0) {
      if (session.open_mode == MODE_NONE) {
        if (send_error_line(client_socket, "File not open") < 0) {
          close_session_and_socket(client_socket, &session);
          return NULL;
        }
        continue;
      }
      if (session.file_stream)
        fclose(session.file_stream);
      close_for(session.open_mode, session.lock);
      release_lock(session.lock);
      session.file_stream = NULL;
      session.open_mode = MODE_NONE;
      session.filename[0] = '\0';
      session.lock = NULL;
    }
  }
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    return 1;
  }
  const char *listen_port = argv[1];

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  struct addrinfo *alist = NULL;
  int rc = getaddrinfo(NULL, listen_port, &hints, &alist);
  if (rc != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
    return 1;
  }

  sem_init(&glob_table_sem, 0, 1);
  glob_table_head = NULL; // (globals are zeroed, but being explicit is fine)

  int listen_fd = -1;
  for (struct addrinfo *it = alist; it; it = it->ai_next) {
    listen_fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (listen_fd < 0)
      continue;
    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (bind(listen_fd, it->ai_addr, it->ai_addrlen) == 0)
      break;
    close(listen_fd);
    listen_fd = -1;
  }
  freeaddrinfo(alist);
  if (listen_fd < 0) {
    perror("bind/listen");
    return 1;
  }
  if (listen(listen_fd, 16) < 0) {
    perror("listen");
    return 1;
  }

  puts("server started");
  fflush(stdout);

  for (;;) {
    struct sockaddr_storage ss;
    socklen_t slen = sizeof(ss);
    int client_socket = accept(listen_fd, (struct sockaddr *)&ss, &slen);
    if (client_socket < 0) {
      if (errno == EINTR)
        continue;
      perror("accept");
      continue;
    }
    pthread_t tid;
    pthread_create(&tid, NULL, client_worker_thread,
                   (void *)(intptr_t)client_socket);
    pthread_detach(tid);
  }
}
