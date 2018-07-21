#ifndef NCODE_WEB_SERVER_H_
#define NCODE_WEB_SERVER_H_

#include <netinet/in.h>
#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <thread>
#include <vector>
#include <random>

#include "../common.h"
#include "../ptr_queue.h"

namespace nc {
namespace viz {

// Chunk to read from the socket at a time, when it is not clear how many bytes
// need to be read.
static constexpr size_t kReadChunk = 1 << 12;

// The header callback will parse a chunk of bytes into a header and a message.
// This callback needs to figure out what offset from the start of a given
// buffer the header ends and parse enough of the header to figure out what the
// size of the message that follows it is. Should return true on success and set
// header_size and message_size.
using HeaderCallback =
    std::function<bool(std::vector<char>::const_iterator from,
                       std::vector<char>::const_iterator to,
                       size_t* header_size, size_t* message_size)>;

// A connection to the server.
struct TCPConnectionInfo {
  TCPConnectionInfo(uint64_t connection_id, uint32_t remote_ip,
                    uint16_t remote_port,
                    std::chrono::milliseconds first_byte_seen)
      : connection_id(connection_id),
        remote_ip(remote_ip),
        remote_port(remote_port),
        first_byte_seen(first_byte_seen) {}

  // Random connection identifier.
  uint64_t connection_id;

  // Remote IP address (in network format)
  uint32_t remote_ip;

  // Remote port.
  uint16_t remote_port;

  // When the connection started.
  std::chrono::milliseconds first_byte_seen;
};

// The main datum that the server produces.
struct IncomingHeaderAndMessage {
  IncomingHeaderAndMessage(const TCPConnectionInfo& tcp_connection_info,
                           int socket, std::chrono::milliseconds time_rx)
      : tcp_connection_info(tcp_connection_info),
        socket(socket),
        header_offset(0),
        time_rx(time_rx) {}

  // The connection.
  const TCPConnectionInfo& tcp_connection_info;

  // Connection this message was received on. Can be used to identify
  // active connections, will not be unique among all connections.
  int socket;

  // Header+message are stored here.
  std::vector<char> buffer;

  // What the offset of the header is in 'buffer'.
  size_t header_offset;

  // Timestamp the message was received.
  std::chrono::milliseconds time_rx;
};

// The main datum that the server consumes.
struct OutgoingHeaderAndMessage {
  OutgoingHeaderAndMessage(int socket)
      : socket(socket), last_in_connection(false) {}

  // Connection this message should be sent to.
  int socket;

  // If this is true the server will terminate the connection after sending this
  // message.
  bool last_in_connection;

  // Header+message are stored here.
  std::vector<char> buffer;
};

// Queue for incoming messages. There will only be one incoming queue for the
// server.
using IncomingMessageQueue = PtrQueue<IncomingHeaderAndMessage, 1024>;

class InputChannel {
 public:
  InputChannel(const TCPConnectionInfo& connection_info, int socket,
               uint32_t max_message_size, HeaderCallback header_callback,
               IncomingMessageQueue* incoming);

  // Consumes a message between two iterators if possible. Returns false on
  // failure.
  bool ConsumeMessage(std::vector<char>::const_iterator from,
                      std::vector<char>::const_iterator to,
                      size_t* bytes_consumed);

  // Reads as many messages as possible from the socket.
  bool ReadFromSocket();

 private:
  const TCPConnectionInfo connection_info_;

  // Stores the incoming message.
  std::vector<char> incoming_messages_;

  // Index into the array above.
  size_t incoming_messages_index_;

  // The socket.
  int socket_;

  // Max size for a single message.
  uint32_t max_message_size_;

  // Partially parses the header.
  HeaderCallback header_callback_;

  // When messages are received they are added to this queue.
  IncomingMessageQueue* incoming_;

  DISALLOW_COPY_AND_ASSIGN(InputChannel);
};

// Opens a connection to a given address/port and returns a socket.
int Connect(const std::string& destination_address, uint32_t port);

// Writes a message to the socket within msg.
bool BlockingWriteMessage(const OutgoingHeaderAndMessage& msg);

struct TCPServerConfig {
  // Port to listen on.
  uint32_t port_num = 8080;

  // Maximum size an incoming message is allowed to be (in bytes).
  uint32_t max_message_size = 1 << 10;
};

class TCPServer {
 public:
  TCPServer(const TCPServerConfig& config, HeaderCallback header_callback,
            IncomingMessageQueue* incoming_queue);

  virtual ~TCPServer() { Stop(); }

  // Starts the main loop.
  void Start();

  // Kills the server.
  void Stop();

  // Waits for the server to terminate.
  void Join();

 private:
  // Opens the socket for listening.
  void OpenSocket();

  // Called when a new TCP connection is established with the server. Accepts
  // the connection and populates new_socket with  the new socket. Will also
  // set try_again to true if EWOULDBLOCK is returned by accept.
  void NewTcpConnection(int* new_socket, bool* try_again);

  // Runs the main server loop. Will block.
  void Loop();

  // Configuration for the server.
  const TCPServerConfig config_;

  // Currently active connections.
  std::map<int, InputChannel> server_connections_;

  // The socket the server listens on.
  int tcp_socket_;

  // Set to true when the server needs to exit.
  std::atomic<bool> to_kill_;

  // The server's main thread.
  std::thread thread_;

  // All incoming headers are parsed by this callback.
  HeaderCallback header_callback_;

  // Queues for messages leaving out/coming in.
  IncomingMessageQueue* incoming_;

  // Sockets to remove from the set of listening sockets.
  std::vector<int> sockets_to_close_;

  // Protected sockets_to_close_.
  std::mutex mu_;

  // Generates connection IDs.
  std::mt19937 rnd_;

  DISALLOW_COPY_AND_ASSIGN(TCPServer);
};

}  // namesapce web
}  // namespace nc

#endif
