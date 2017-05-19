#include "gtest/gtest.h"
#include "server.h"

namespace nc {
namespace viz {
namespace {

struct DummyHeader {
  static size_t MessageSize(const DummyHeader& header) { return header.len; }

  int len;
};

class Fixture : public ::testing::Test {
 public:
  Fixture() : server_(8080, &incoming_, &outgoing_) {}

  std::unique_ptr<HeaderAndMessage<DummyHeader>> GetJunkMessage() {
    auto message_ptr = make_unique<HeaderAndMessage<DummyHeader>>(-1);
    message_ptr->header.len = 1000;

    std::vector<char>& msg = message_ptr->message;
    msg.resize(1000);
    std::fill(msg.begin(), msg.end(), 'a');
    return message_ptr;
  }

  MessageQueue<DummyHeader> incoming_;
  MessageQueue<DummyHeader> outgoing_;
  TCPServer<DummyHeader> server_;
};

TEST_F(Fixture, StartWaitKill) {
  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  server_.Stop();
}

TEST_F(Fixture, SimpleMessage) {
  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  auto client = ClientConnection<DummyHeader>::Connect("127.0.0.1", 8080);
  ASSERT_TRUE(client);

  auto message = GetJunkMessage();

  ASSERT_TRUE(client->WriteToSocket(std::move(message)));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  server_.Stop();

  std::vector<std::unique_ptr<HeaderAndMessage<DummyHeader>>> contents =
      incoming_.Drain();

  ASSERT_EQ(1ul, contents.size());
  ASSERT_EQ(1000ul, contents[0]->message.size());
}

TEST_F(Fixture, LotsOfMessages) {
  using namespace std::chrono;
  size_t msg_count = 1 << 17;
  std::vector<std::unique_ptr<HeaderAndMessage<DummyHeader>>> messages;
  for (size_t i = 0; i < msg_count; ++i) {
    messages.emplace_back(GetJunkMessage());
  }

  server_.Start();
  std::this_thread::sleep_for(milliseconds(500));
  auto client = ClientConnection<DummyHeader>::Connect("127.0.0.1", 8080);
  ASSERT_TRUE(client);

  auto now = high_resolution_clock::now();
  std::thread producer = std::thread([&client, this, msg_count, &messages] {
    for (size_t i = 0; i < msg_count; ++i) {
      ASSERT_TRUE(client->WriteToSocket(std::move(messages[i])));
    }
  });

  std::thread consumer = std::thread([this, msg_count] {
    for (size_t i = 0; i < msg_count; ++i) {
      std::unique_ptr<HeaderAndMessage<DummyHeader>> msg =
          incoming_.ConsumeOrBlock();
      ASSERT_EQ(1000ul, msg->message.size());
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

  server_.Stop();
  ASSERT_EQ(0ul, incoming_.size());
}

// TEST_F(Fixture, SimpleMessageWaitReply) {
//  server_.StartLoop();
//  std::this_thread::sleep_for(std::chrono::milliseconds(500));
//  auto client = ClientConnection<DummyHeader>::Connect("127.0.0.1", 8080);
//  ASSERT_TRUE(client);
//
//  auto message = GetJunkMessage();
//
//  ASSERT_TRUE(client_connection->WriteToSocket(std::move(message)).ok());
//  auto read_result = client_connection->ReadFromSocket();
//  ASSERT_TRUE(read_result.ok());
//
//  server.Terminate();
//
//  ASSERT_EQ(1, messages_.size());
//  ASSERT_EQ(1, messages_[0].size());
//  ASSERT_EQ(10000, messages_[0].at(0)->size());
//  ASSERT_EQ(*GetJunkMessage(), *read_result.ValueOrDie());
//}

TEST_F(Fixture, MultiSequentialConnections) {
  server_.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  auto client = ClientConnection<DummyHeader>::Connect("127.0.0.1", 8080);
  ASSERT_TRUE(client);

  auto message = GetJunkMessage();
  ASSERT_TRUE(client->WriteToSocket(std::move(message)));

  client->Close();
  client = ClientConnection<DummyHeader>::Connect("127.0.0.1", 8080);

  message = GetJunkMessage();
  ASSERT_TRUE(client->WriteToSocket(std::move(message)));

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  server_.Stop();
  std::vector<std::unique_ptr<HeaderAndMessage<DummyHeader>>> contents =
      incoming_.Drain();

  ASSERT_EQ(2ul, contents.size());
  ASSERT_EQ(1000ul, contents[0]->message.size());
  ASSERT_EQ(1000ul, contents[1]->message.size());
}
//
// TEST_F(Fixture, MultiSimultaneousConnection) {
//  auto factory = std::unique_ptr<ServerConnectionFactory>(
//      new DummyServerConnectionWrapperFactory(this));
//
//  Server server(8080, std::move(factory));
//  Status server_start_status = server.StartLoop();
//  ASSERT_TRUE(server_start_status.ok());
//
//  auto first_result = ClientConnection::Connect("127.0.0.1", 8080);
//  ASSERT_TRUE(first_result.ok());
//  std::unique_ptr<ClientConnection> first_connection =
//      first_result.ValueOrDie();
//
//  for (int i = 0; i < 10; i++) {
//    auto message = GetJunkMessage();
//    ASSERT_TRUE(first_connection->WriteToSocket(std::move(message)).ok());
//  };
//
//  auto second_result = ClientConnection::Connect("127.0.0.1", 8080);
//  ASSERT_TRUE(second_result.ok());
//  std::unique_ptr<ClientConnection> second_connection =
//      second_result.ValueOrDie();
//
//  for (int i = 0; i < 10; i++) {
//    auto message = GetJunkMessage();
//    ASSERT_TRUE(second_connection->WriteToSocket(std::move(message)).ok());
//  };
//
//  // Time to consume the messages
//  for (int i = 0; i < 10; i++) {
//    first_connection->ReadFromSocket();
//    second_connection->ReadFromSocket();
//  };
//
//  server.Terminate();
//
//  ASSERT_EQ(2, messages_.size());
//  ASSERT_EQ(10, messages_[0].size());
//  ASSERT_EQ(10, messages_[1].size());
//}
//
// TEST_F(Fixture, TestForceClose) {
//  auto factory = std::unique_ptr<ServerConnectionFactory>(
//      new DummyServerConnectionWrapperFactory(this, 1));
//
//  Server server(8080, std::move(factory));
//  Status server_start_status = server.StartLoop();
//  ASSERT_TRUE(server_start_status.ok());
//
//  auto result = ClientConnection::Connect("127.0.0.1", 8080);
//  ASSERT_TRUE(result.ok());
//  std::unique_ptr<ClientConnection> client_connection = result.ValueOrDie();
//
//  // We send 10 messages but only the first one will get through. Afterwards
//  the
//  // message handler will start returning non-ok status and the connection
//  // should be terminated by the server.
//  for (int i = 0; i < 10; i++) {
//    auto message = GetJunkMessage();
//    ASSERT_TRUE(client_connection->WriteToSocket(std::move(message)).ok());
//  };
//
//  // Reads on the client side are blocking, but this should terminate when the
//  // server kills the connection.
//  for (int i = 0; i < 10; i++) {
//    client_connection->ReadFromSocket();
//  };
//
//  server.Terminate();
//
//  ASSERT_EQ(1, messages_.size());
//  ASSERT_EQ(1, messages_[0].size());
//  ASSERT_EQ(10000, messages_[0].at(0)->size());
//}
}  // namespace
}  // namespace web
}  // namespace nc
