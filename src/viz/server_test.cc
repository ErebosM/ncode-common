#include "gtest/gtest.h"
#include "server.h"

namespace nc {
namespace viz {
namespace {

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
  Fixture() : server_(8080, ParseConstantSizeHeader, &incoming_) {}

  std::unique_ptr<HeaderAndMessage> GetJunkMessage(uint32_t size, int socket) {
    std::vector<char> buffer(size + 4, 'a');
    memcpy(buffer.data(), &size, 4);

    auto out = nc::make_unique<HeaderAndMessage>(socket);
    out->header_offset = 4;
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

  std::vector<std::unique_ptr<HeaderAndMessage>> contents = incoming_.Drain();
  ASSERT_EQ(1ul, contents.size());
  ASSERT_EQ(to_send->buffer, contents[0]->buffer);
}

TEST_F(Fixture, LotsOfMessages) {
  using namespace std::chrono;
  std::mt19937 rnd(1);

  // 1M messages in and about 500MB data.
  size_t msg_count = 1 << 20;
  std::uniform_int_distribution<size_t> dist(10, 1000);

  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  std::vector<std::unique_ptr<HeaderAndMessage>> messages;
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

  std::vector<std::unique_ptr<HeaderAndMessage>> received;
  std::thread consumer = std::thread([this, msg_count, &received] {
    for (size_t i = 0; i < msg_count; ++i) {
      std::unique_ptr<HeaderAndMessage> msg = incoming_.ConsumeOrBlock();
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
