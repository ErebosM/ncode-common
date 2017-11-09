#ifndef NCODE_WEB_SERVER_H_
#define NCODE_WEB_SERVER_H_

#include <netinet/in.h>
#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

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

// The main datum that the server produces/consumes.
struct HeaderAndMessage {
  HeaderAndMessage(int socket)
      : socket(socket), last_in_connection(false), header_offset(0) {}

  // Connection this message should be sent to / was received on. May or may not
  // be valid for incoming messages. Can be used to identify connections.
  int socket;

  // If this is true the server will terminate the connection after sending this
  // message.
  bool last_in_connection;

  // Header+message are stored here.
  std::vector<char> buffer;

  // What the offset of the header is in 'buffer'.
  size_t header_offset;
};

// Queue for incoming messages. There will only be one queue for the server.
using IncomingMessageQueue = PtrQueue<HeaderAndMessage, 1024>;

class InputChannel {
 public:
  InputChannel(int socket, sockaddr_in address, HeaderCallback header_callback,
               IncomingMessageQueue* incoming)
      : socket_(socket),
        address_(address),
        header_callback_(header_callback),
        incoming_(incoming) {}

  // Consumes a message between two iterators if possible.
  size_t ConsumeMessage(std::vector<char>::const_iterator from,
                        std::vector<char>::const_iterator to);

  // Reads as many messages as possible from the socket.
  bool ReadFromSocket();

 private:
  // Stores the incoming message.
  std::vector<char> header_and_message_;

  // The socket.
  int socket_;

  // Address of the remote side.
  sockaddr_in address_;

  // Partially parses the header.
  HeaderCallback header_callback_;

  // When messages are received they are added to this queue.
  IncomingMessageQueue* incoming_;

  DISALLOW_COPY_AND_ASSIGN(InputChannel);
};

// Opens a connection to a given address/port and returns a socket.
int Connect(const std::string& destination_address, uint32_t port);

// Writes a message to the socket within msg.
bool BlockingWriteMessage(const HeaderAndMessage& msg);

class TCPServer {
 public:
  TCPServer(uint32_t port, HeaderCallback header_callback,
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

  // Currently active connections.
  std::map<int, InputChannel> server_connections_;

  // The socket the server listens on.
  int tcp_socket_;

  // The port the server should listen to.
  const uint32_t port_;

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

  DISALLOW_COPY_AND_ASSIGN(TCPServer);
};

}  // namesapce web
}  // namespace nc

#endif
