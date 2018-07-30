#include "server.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iterator>
#include <memory>
#include <random>
#include <type_traits>

#include "../logging.h"

namespace nc {
namespace viz {
namespace {

static constexpr TCPServerConfig kConfig = {8080, 1 << 10};

// A dummy function to parse a single uint32_t integer header with the message
// size.
static bool ParseConstantSizeHeader(std::vector<char>::const_iterator from,
                                    std::vector<char>::const_iterator to,
                                    size_t* header_size, size_t* message_size) {
  if (std::distance(from, to) < 4) {
    return false;
  }

  *header_size = 4;
  uint32_t value = *(reinterpret_cast<const uint32_t*>(&(*from)));
  *message_size = value;
  return true;
}

class Fixture : public ::testing::Test {
 public:
  Fixture() : server_(kConfig, ParseConstantSizeHeader, &incoming_) {}

  std::unique_ptr<OutgoingHeaderAndMessage> GetJunkMessage(uint32_t size,
                                                           int socket) {
    std::vector<char> buffer(size + 4, 'a');
    memcpy(buffer.data(), &size, 4);

    auto out = nc::make_unique<OutgoingHeaderAndMessage>(socket);
    out->buffer = std::move(buffer);
    return out;
  }

  IncomingMessageQueue incoming_;
  TCPServer server_;
};

TEST_F(Fixture, StartWaitKill) {
  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  server_.Stop();
}

TEST_F(Fixture, SimpleMessage) {
  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  int socket = Connect("127.0.0.1", 8080);
  auto to_send = GetJunkMessage(100, socket);
  BlockingWriteMessage(*to_send);

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  server_.Stop();

  std::vector<std::unique_ptr<IncomingHeaderAndMessage>> contents =
      incoming_.Drain();
  ASSERT_EQ(1ul, contents.size());
  ASSERT_EQ(to_send->buffer, contents[0]->buffer);
}

TEST_F(Fixture, MessageTooBig) {
  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  int socket = Connect("127.0.0.1", 8080);
  auto to_send = GetJunkMessage(kConfig.max_message_size + 1, socket);
  BlockingWriteMessage(*to_send);

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  server_.Stop();

  std::vector<std::unique_ptr<IncomingHeaderAndMessage>> contents =
      incoming_.Drain();
  ASSERT_EQ(0ul, contents.size());
}

TEST_F(Fixture, LotsOfMessages) {
  using namespace std::chrono;
  std::mt19937 rnd(1);

  // 1M messages in and about 500MB data.
  size_t msg_count = 1 << 20;
  std::uniform_int_distribution<size_t> dist(10, kConfig.max_message_size - 4);

  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  std::vector<std::unique_ptr<OutgoingHeaderAndMessage>> messages;
  int socket = Connect("127.0.0.1", 8080);
  for (size_t i = 0; i < msg_count; ++i) {
    size_t msg_size = dist(rnd);
    messages.emplace_back(GetJunkMessage(msg_size, socket));
  }

  auto now = high_resolution_clock::now();
  std::thread producer = std::thread([&messages, msg_count] {
    for (size_t i = 0; i < msg_count; ++i) {
      ASSERT_TRUE(BlockingWriteMessage(*messages[i]));
    }
  });

  std::vector<std::unique_ptr<IncomingHeaderAndMessage>> received;
  std::thread consumer = std::thread([this, msg_count, &received] {
    for (size_t i = 0; i < msg_count; ++i) {
      std::unique_ptr<IncomingHeaderAndMessage> msg =
          incoming_.ConsumeOrBlock();
      ASSERT_NE(0ul, msg->tcp_connection_info.connection_id);
      ASSERT_NE(0ul, msg->tcp_connection_info.remote_ip);
      ASSERT_NE(0ul, msg->tcp_connection_info.remote_port);
      received.emplace_back(std::move(msg));
    }
  });

  if (producer.joinable()) {
    producer.join();
  }

  if (consumer.joinable()) {
    consumer.join();
  }

  auto later = high_resolution_clock::now();
  LOG(INFO) << msg_count << " in "
            << duration_cast<milliseconds>(later - now).count() << "ms";

  for (size_t i = 0; i < msg_count; ++i) {
    ASSERT_EQ(messages[i]->buffer, received[i]->buffer);
  }

  server_.Stop();
  ASSERT_EQ(0ul, incoming_.size());
}

}  // namespace
}  // namespace web
}  // namespace nc
