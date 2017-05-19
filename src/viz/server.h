#ifndef NCODE_WEB_SERVER_H_
#define NCODE_WEB_SERVER_H_

#include <stddef.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <netdb.h>
#include <string>
#include <string.h>
#include <thread>
#include <vector>
#include <deque>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <fcntl.h>

#include "../common.h"
#include "../ptr_queue.h"

namespace nc {
namespace viz {

bool BlockingRawReadFromSocket(int sock, char* buf, uint32_t len);

// The main datum that the server produces/consumes.
template <typename HeaderType>
struct HeaderAndMessage {
  HeaderAndMessage(uint64_t connection_id)
      : connection_id(connection_id), last_in_connection(false) {}

  // Connection this message should be sent to / was received on.
  uint64_t connection_id;

  // If this is true the server will terminate the connection after sending this
  // message.
  bool last_in_connection;

  HeaderType header;
  std::vector<char> message;
};

template <typename HeaderType>
std::unique_ptr<HeaderAndMessage<HeaderType>> BlockingReadMessageFromSocket(
    int socket) {
  auto message_ptr = make_unique<HeaderAndMessage<HeaderType>>(socket);
  HeaderType& header = message_ptr->header;

  char* header_ptr = reinterpret_cast<char*>(&header);
  if (!BlockingRawReadFromSocket(socket, header_ptr, sizeof(HeaderType))) {
    return {};
  }

  size_t message_len = HeaderType::MessageSize(header);
  message_ptr->message.resize(message_len);
  std::vector<char>& message_v = message_ptr->message;
  if (!BlockingRawReadFromSocket(socket, message_v.data(), message_len)) {
    return {};
  }

  return message_ptr;
}

template <typename HeaderType>
using MessageQueue = PtrQueue<HeaderAndMessage<HeaderType>, 1024>;

template <typename HeaderType>
class InputChannel {
 public:
  InputChannel(int socket, MessageQueue<HeaderType>* incoming)
      : offset_(0), socket_(socket), incoming_(incoming) {}

  bool ReadFromSocket() {
    const size_t header_len = sizeof(HeaderType);

    while (true) {
      bool header_seen = offset_ >= header_len;
      if (!header_seen) {
        char* header_ptr = reinterpret_cast<char*>(&header_);

        ssize_t bytes_read =
            read(socket_, header_ptr + offset_, header_len - offset_);
        if (bytes_read == -1 || bytes_read == 0) {
          if (errno == EAGAIN) {
            break;
          }

          LOG(ERROR) << "Unable to read / connection closed: "
                     << strerror(errno);
          return false;
        }

        offset_ += bytes_read;
        if (offset_ < header_len) {
          break;
        }
      } else {
        size_t message_len = HeaderType::MessageSize(header_);
        if (message_len != 0) {
          message_.resize(std::max(message_.size(), message_len));

          size_t into_message = offset_ - header_len;
          ssize_t bytes_read = read(socket_, message_.data() + into_message,
                                    message_len - into_message);
          if (bytes_read == -1 || bytes_read == 0) {
            if (errno == EAGAIN) {
              break;
            }

            LOG(ERROR) << "Unable to read / connection closed: "
                       << strerror(errno);
            return false;
          }

          offset_ += bytes_read;
          if (offset_ < message_len + header_len) {
            break;
          }
        }

        auto header_and_message =
            make_unique<HeaderAndMessage<HeaderType>>(socket_);
        header_and_message->header = header_;
        header_and_message->message = std::move(message_);
        incoming_->ProduceOrBlock(std::move(header_and_message));

        offset_ = 0;
      }
    }

    return true;
  }

 private:
  // Stores the header.
  HeaderType header_;

  // Stores the message.
  std::vector<char> message_;

  // A single offset into header + message.
  size_t offset_;

  // The socket.
  int socket_;

  // Outgoing messages.
  MessageQueue<HeaderType>* incoming_;

  DISALLOW_COPY_AND_ASSIGN(InputChannel);
};

bool BlockingRawWriteToSocket(int sock, const char* buf, uint32_t len);

template <typename HeaderType>
bool BlockingWriteMessageToSocket(
    std::unique_ptr<HeaderAndMessage<HeaderType>> msg) {
  char* header_ptr = reinterpret_cast<char*>(&msg->header);
  if (!BlockingRawWriteToSocket(msg->connection_id, header_ptr,
                                sizeof(HeaderType))) {
    return false;
  }

  const std::vector<char>& message_v = msg->message;
  const char* message_ptr = message_v.data();
  if (!BlockingRawWriteToSocket(msg->connection_id, message_ptr,
                                message_v.size())) {
    return false;
  }

  return true;
}

template <typename HeaderType>
class ServerConnection {
 public:
  ServerConnection(sockaddr_in address, int socket,
                   MessageQueue<HeaderType>* incoming)
      : address_(address), input_channel_(socket, incoming) {}

  bool Read() { return input_channel_.ReadFromSocket(); }

 private:
  sockaddr_in address_;
  InputChannel<HeaderType> input_channel_;
};

template <typename HeaderType>
class TCPServer {
 public:
  using QueueType = MessageQueue<HeaderType>;

  TCPServer(uint32_t port, QueueType* incoming, QueueType* outgoing)
      : tcp_socket_(-1),
        port_(port),
        to_kill_(false),
        incoming_(incoming),
        outgoing_(outgoing) {}

  virtual ~TCPServer() { Stop(); }

  // Starts the main loop.
  void Start() {
    OpenSocket();
    thread_ = std::thread([this] { Loop(); });
    send_thread_ = std::thread([this] { WriteToSocket(); });
  }

  // Kills the server.
  void Stop() {
    if (to_kill_) {
      return;
    }

    LOG(INFO) << "Closing socket and terminating server.";
    to_kill_ = true;

    Join();
    close(tcp_socket_);
  }

  void Join() {
    if (thread_.joinable()) {
      thread_.join();
    }

    if (send_thread_.joinable()) {
      send_thread_.join();
    }
  }

 private:
  // Opens the socket for listening.
  void OpenSocket() {
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

  // Called when a new TCP connection is established with the server. Accepts
  // the connection and populates new_socket with  the new socket. Will also set
  // try_again to true if EWOULDBLOCK is returned by accept.
  void NewTcpConnection(int* new_socket, bool* try_again) {
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

    active_connections_.emplace(
        std::piecewise_construct, std::forward_as_tuple(socket),
        std::forward_as_tuple(remote_address, socket, incoming_));
  }

  // Runs the main server loop. Will block.
  void Loop() {
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
          active_connections_.erase(socket_to_close);
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
            ServerConnection<HeaderType>* connection =
                FindOrNull(active_connections_, i);
            if (connection == nullptr) {
              LOG(INFO) << "Missing connection for socket " << i;
              continue;
            }

            if (!connection->Read()) {
              LOG(INFO) << "Error in connection";
              active_connections_.erase(i);
              close(i);
              FD_CLR(i, &master);
            }
          }
        }
      }
    }
  }

  void WriteToSocket() {
    while (!to_kill_) {
      bool timed_out;
      std::unique_ptr<HeaderAndMessage<HeaderType>> message =
          outgoing_->ConsumeOrBlockWithTimeout(std::chrono::seconds(1),
                                               &timed_out);
      if (timed_out) {
        continue;
      }

      if (!message) {
        break;
      }

      int socket = message->connection_id;
      bool last = message->last_in_connection;
      if (!BlockingWriteMessageToSocket(std::move(message))) {
        break;
      }

      if (last) {
        std::lock_guard<std::mutex> lock(mu_);
        sockets_to_close_.emplace_back(socket);
      }
    }
  }

  // Currently active connections.
  std::map<int, ServerConnection<HeaderType>> active_connections_;

  // The socket the server listens on.
  int tcp_socket_;

  // The port the server should listen to.
  const uint32_t port_;

  // Set to true when the server needs to exit.
  std::atomic<bool> to_kill_;

  // The server's main thread.
  std::thread thread_;

  // A thread whose job it is to constantly try to send messages.
  std::thread send_thread_;

  // Queues for messages leaving out/coming in.
  QueueType* incoming_;
  QueueType* outgoing_;

  // Sockets to remove from the set of listening sockets.
  std::vector<int> sockets_to_close_;

  // Protected sockets_to_close_.
  std::mutex mu_;

  DISALLOW_COPY_AND_ASSIGN(TCPServer);
};

// Simple wrapper around a blocking socket that makes it easier to
// connect and send / receive messages.
template <typename HeaderType>
class ClientConnection {
 public:
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

  static std::unique_ptr<ClientConnection> Connect(
      const std::string& destination_address, uint32_t port) {
    sockaddr_in address;
    memset(&address, 0, sizeof(address));

    address.sin_family = AF_INET;
    address.sin_port = htons(port);

    ClientConnection::ResolveHostName(destination_address, &(address.sin_addr));
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (::connect(s, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
        0) {
      LOG(FATAL) << "Unable to connect: " << strerror(errno);
    }

    return std::unique_ptr<ClientConnection>(new ClientConnection(s));
  }

  // Writes a message
  bool WriteToSocket(std::unique_ptr<HeaderAndMessage<HeaderType>> msg) const {
    if (msg->connection_id == static_cast<uint64_t>(-1)) {
      msg->connection_id = tcp_socket_;
    }
    return BlockingWriteMessageToSocket(std::move(msg));
  }

  // Reads a message
  std::unique_ptr<HeaderAndMessage<HeaderType>> ReadFromSocket() const {
    return BlockingReadMessageFromSocket<HeaderType>(tcp_socket_);
  }

  // Closes the socket. After this call this ClientConnection will not be able
  // to send/receive any messages.
  void Close() const { ::close(tcp_socket_); }

 private:
  ClientConnection(int tcp_socket) : tcp_socket_(tcp_socket) {}

  // The socket.
  const int tcp_socket_;
};

}  // namesapce web
}  // namespace nc

#endif
