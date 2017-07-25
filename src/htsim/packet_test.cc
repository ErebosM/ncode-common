#include "gtest/gtest.h"
#include "packet.h"

namespace nc {
namespace htsim {
namespace {

static constexpr net::IPAddress kSrc = net::IPAddress(1);
static constexpr net::IPAddress kDst = net::IPAddress(2);
static constexpr net::IPProto kProto = net::IPProto(1);
static constexpr net::AccessLayerPort kSrcPort = net::AccessLayerPort(4);
static constexpr net::AccessLayerPort kDstPort = net::AccessLayerPort(5);
static constexpr SeqNum kSeqNum = SeqNum(5000);
static constexpr size_t kSize = 10;
static constexpr EventQueueTime kTime = EventQueueTime(10);
static constexpr char kConnectionId[] = "ConnectionId";
static constexpr net::FiveTuple kTestTuple =
    net::FiveTuple(kSrc, kDst, kProto, kSrcPort, kDstPort);

class PacketTest : public ::testing::Test {
 protected:
  PacketTest()
      : udp_packet_(kTestTuple, kSize, kTime),
        tcp_packet_(kTestTuple, kSize, kTime, kSeqNum) {}

  void SetUp() override {}

  UDPPacket udp_packet_;
  TCPPacket tcp_packet_;
};

TEST_F(PacketTest, InitValues) {
  ASSERT_EQ(kTestTuple, udp_packet_.five_tuple());
  ASSERT_EQ(kSize, udp_packet_.size_bytes());
  ASSERT_EQ(kDefaultTTL, udp_packet_.ttl());
  ASSERT_EQ(kDefaultTag, udp_packet_.tag());
  ASSERT_EQ(kSeqNum, tcp_packet_.sequence());
  ASSERT_EQ(kTime, tcp_packet_.time_sent());
}

TEST_F(PacketTest, TTLExpired) {
  for (size_t i = 0; i < kDefaultTTL; ++i) {
    ASSERT_TRUE(udp_packet_.DecrementTTL());
  }
  ASSERT_FALSE(udp_packet_.DecrementTTL());
}

class MockHandler : public PacketHandler {
 public:
  std::vector<PacketPtr> packets() { return std::move(packets_); }

  void HandlePacket(PacketPtr packet) override {
    packets_.emplace_back(std::move(packet));
  }

 private:
  std::vector<PacketPtr> packets_;
};

class DummyConnection : public Connection {
 public:
  DummyConnection(PacketHandler* out, EventQueue* event_queue)
      : Connection(kConnectionId, kTestTuple, out, event_queue) {}

  using Connection::SendPacket;
  void Close() override {}
  void AddData(uint64_t data_bytes) override { Unused(data_bytes); }
  void ReceivePacket(PacketPtr pkt) override { Unused(pkt); };
};

class ConnectionTest : public ::testing::Test {
 protected:
  MockHandler mock_handler_;
  SimTimeEventQueue event_queue_;
};

TEST_F(ConnectionTest, EmptyStats) {
  DummyConnection dummy_connection(&mock_handler_, &event_queue_);
  ASSERT_EQ(0ul, dummy_connection.GetStats().bytes_rx);
  ASSERT_EQ(0ul, dummy_connection.GetStats().pkts_rx);
  ASSERT_EQ(0ul, dummy_connection.GetStats().bytes_tx);
  ASSERT_EQ(0ul, dummy_connection.GetStats().pkts_tx);
}

TEST_F(ConnectionTest, RxPacket) {
  auto packet = GetFreeList<UDPPacket>().New(kTestTuple, kSize, kTime);
  DummyConnection dummy_connection(&mock_handler_, &event_queue_);
  dummy_connection.HandlePacket(std::move(packet));

  ASSERT_EQ(kSize, dummy_connection.GetStats().bytes_rx);
  ASSERT_EQ(1ul, dummy_connection.GetStats().pkts_rx);
  ASSERT_EQ(0ul, dummy_connection.GetStats().bytes_tx);
  ASSERT_EQ(0ul, dummy_connection.GetStats().pkts_tx);
}

TEST_F(ConnectionTest, TxPacket) {
  auto packet = GetFreeList<UDPPacket>().New(kTestTuple, kSize, kTime);
  DummyConnection dummy_connection(&mock_handler_, &event_queue_);
  dummy_connection.SendPacket(std::move(packet));

  ASSERT_EQ(kSize, dummy_connection.GetStats().bytes_tx);
  ASSERT_EQ(1ul, dummy_connection.GetStats().pkts_tx);
  ASSERT_EQ(0ul, dummy_connection.GetStats().bytes_rx);
  ASSERT_EQ(0ul, dummy_connection.GetStats().pkts_rx);
  ASSERT_EQ(1ul, mock_handler_.packets().size());
}

}  // namespace
}  // namespace htsim
}  // namespace ncode
