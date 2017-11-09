#include "server.h"

#include <netdb.h>
#include <stddef.h>
#include <string.h>
#include <sys/errno.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iterator>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "../logging.h"
#include "../map_util.h"

namespace nc {
namespace viz {

static bool BlockingRawWriteToSocket(int sock, const char* buf, uint32_t len) {
  uint32_t total = 0;

  while (total < len) {
    int bytes_written = write(sock, buf + total, len - total);
    if (bytes_written < 0) {
      if (errno != EWOULDBLOCK) {
        LOG(ERROR) << "Unable to write: " << strerror(errno);
        return false;
      }

      fd_set write_fds;
      FD_ZERO(&write_fds);
      FD_SET(sock, &write_fds);

      timeval tv = {0, 0};
      tv.tv_sec = 1;

      int select_return = select(sock + 1, nullptr, &write_fds, NULL, &tv);
      if (select_return < 0) {
        LOG(FATAL) << "Unable to select: " << strerror(errno);
      }
    }

    total += bytes_written;
  }

  return true;
}

bool BlockingWriteMessage(const HeaderAndMessage& msg) {
  const char* buffer_ptr = msg.buffer.data();
  return BlockingRawWriteToSocket(msg.socket, buffer_ptr, msg.buffer.size());
}

TCPServer::TCPServer(uint32_t port, HeaderCallback header_callback,
                     IncomingMessageQueue* incoming)
    : tcp_socket_(-1),
      port_(port),
      to_kill_(false),
      header_callback_(header_callback),
      incoming_(incoming) {}

void TCPServer::Start() {
  OpenSocket();
  thread_ = std::thread([this] { Loop(); });
}

void TCPServer::Stop() {
  if (to_kill_) {
    return;
  }

  LOG(INFO) << "Closing socket and terminating server.";
  to_kill_ = true;

  Join();
  close(tcp_socket_);
}

void TCPServer::Join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

void TCPServer::OpenSocket() {
  sockaddr_in address;
  memset(reinterpret_cast<char*>(&address), 0, sizeof(address));

  if ((tcp_socket_ = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    LOG(FATAL) << "Unable to get socket";
  }

  address.sin_family = AF_INET;
  address.sin_port = htons(port_);
  address.sin_addr.s_addr = INADDR_ANY;

  int yes = 1;
  if (setsockopt(tcp_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) ==
      -1) {
    LOG(FATAL) << "Unable to set REUSEADDR";
  }

  if (bind(tcp_socket_, reinterpret_cast<sockaddr*>(&address),
           sizeof(sockaddr)) == -1) {
    LOG(FATAL) << "Unable to bind: " + std::string(strerror(errno));
  }

  if (listen(tcp_socket_, 10) == -1) {
    LOG(FATAL) << "Unable to listen";
  }

  // Set to non-blocking
  fcntl(tcp_socket_, F_SETFL, O_NONBLOCK);
}

void TCPServer::NewTcpConnection(int* new_socket, bool* try_again) {
  sockaddr_in remote_address;
  socklen_t address_len = sizeof(remote_address);

  *try_again = false;
  int socket;
  if ((socket = accept(tcp_socket_,
                       reinterpret_cast<struct sockaddr*>(&remote_address),
                       &address_len)) == -1) {
    if (errno != EWOULDBLOCK) {
      LOG(FATAL) << "Unable to accept";
    }

    *try_again = true;
    return;
  }

  fcntl(socket, F_SETFL, O_NONBLOCK);
  *new_socket = socket;

  server_connections_.emplace(
      std::piecewise_construct, std::forward_as_tuple(socket),
      std::forward_as_tuple(socket, remote_address, header_callback_,
                            incoming_));
}

void TCPServer::Loop() {
  int last_fd = tcp_socket_;
  fd_set master;
  fd_set read_fds;

  FD_ZERO(&master);
  FD_ZERO(&read_fds);

  FD_SET(tcp_socket_, &master);

  while (!to_kill_) {
    read_fds = master;

    timeval tv = {0, 0};
    tv.tv_sec = 1;

    int select_return = select(last_fd + 1, &read_fds, nullptr, NULL, &tv);
    if (select_return < 0) {
      LOG(FATAL) << "Unable to select: " << strerror(errno);
    }

    {
      std::lock_guard<std::mutex> lock(mu_);
      for (int socket_to_close : sockets_to_close_) {
        server_connections_.erase(socket_to_close);
        close(socket_to_close);
        FD_CLR(socket_to_close, &master);
        FD_CLR(socket_to_close, &read_fds);
      }

      sockets_to_close_.clear();
    }

    if (select_return == 0) {
      continue;  // Timed out
    }

    for (int i = 0; i <= last_fd; i++) {
      if (FD_ISSET(i, &read_fds)) {
        if (i == tcp_socket_) {
          int new_socket;
          bool try_again;

          NewTcpConnection(&new_socket, &try_again);
          if (try_again) {
            break;
          }

          FD_SET(new_socket, &master);
          if (new_socket > last_fd) {
            last_fd = new_socket;
          }
        } else {
          InputChannel* connection = FindOrNull(server_connections_, i);
          if (connection == nullptr) {
            LOG(INFO) << "Missing connection for socket " << i;
            continue;
          }

          if (!connection->ReadFromSocket()) {
            LOG(INFO) << "Error in connection";
            server_connections_.erase(i);
            close(i);
            FD_CLR(i, &master);
          }
        }
      }
    }
  }
}

size_t InputChannel::ConsumeMessage(std::vector<char>::const_iterator from,
                                    std::vector<char>::const_iterator to) {
  size_t bytes_in_buffer = std::distance(from, to);

  size_t header_size;
  size_t message_size;
  if (!header_callback_(from, to, &header_size, &message_size)) {
    return 0;
  }

  size_t total_size = message_size + header_size;
  if (bytes_in_buffer < total_size) {
    // We have the header, but not the message.
    return 0;
  }

  std::vector<char> to_offload(from, std::next(from, total_size));
  auto header_and_message = make_unique<HeaderAndMessage>(socket_);
  header_and_message->buffer = std::move(to_offload);
  header_and_message->header_offset = header_size;
  incoming_->ProduceOrBlock(std::move(header_and_message));
  return 0;
}

bool InputChannel::ReadFromSocket() {
  // Will greedily read as many bytes from the socket as possible.
  bool closed = false;
  while (true) {
    size_t current_size = header_and_message_.size();
    header_and_message_.resize(current_size + kReadChunk);
    char* header_ptr = header_and_message_.data();
    ssize_t bytes_read = read(socket_, header_ptr + current_size, kReadChunk);
    if (bytes_read < 0) {
      if (errno == EWOULDBLOCK) {
        break;
      }

      LOG(ERROR) << "Unable to read: " << strerror(errno);
      return false;
    }

    if (bytes_read == 0) {
      closed = true;
      break;
    }
  }

  size_t offset = 0;
  while (true) {
    size_t bytes_consumed =
        ConsumeMessage(std::next(header_and_message_.begin(), offset),
                       header_and_message_.end());
    if (bytes_consumed == 0) {
      break;
    }

    offset += bytes_consumed;
  }

  // Need to copy over the remaining bytes until the end of the buffer for the
  // next iteration.
  size_t leftover = header_and_message_.size() - offset;
  char* header_ptr = header_and_message_.data();
  memmove(header_ptr, header_ptr + offset, leftover);
  header_and_message_.resize(leftover);
  return true;
}

static void ResolveHostName(const std::string& hostname, in_addr* addr) {
  addrinfo* res;

  int result = getaddrinfo(hostname.c_str(), NULL, NULL, &res);
  if (result == 0) {
    memcpy(addr, &(reinterpret_cast<sockaddr_in*>(res->ai_addr))->sin_addr,
           sizeof(in_addr));
    freeaddrinfo(res);

    return;
  }

  LOG(FATAL) << "Unable to resolve";
}

int Connect(const std::string& destination_address, uint32_t port) {
  sockaddr_in address;
  memset(&address, 0, sizeof(address));

  address.sin_family = AF_INET;
  address.sin_port = htons(port);

  ResolveHostName(destination_address, &(address.sin_addr));
  int s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (::connect(s, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
      0) {
    LOG(FATAL) << "Unable to connect: " << strerror(errno);
  }

  return s;
}

}  // namespace web
}  // namespace nc
