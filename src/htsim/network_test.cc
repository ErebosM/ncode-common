#include "gtest/gtest.h"
#include "network.h"
#include "udp.h"

namespace nc {
namespace htsim {
namespace {

using namespace std::chrono;

static constexpr size_t kSimEndTimeMs = 10000;
static constexpr size_t kUDPPacketSize = 100;
static constexpr net::Bandwidth kRate = net::Bandwidth::FromMBitsPerSecond(10);
static constexpr net::Delay kDelay = std::chrono::milliseconds(50);

// A simple consumer that calls a callback when the event triggers.
class DummyConsumer : public EventConsumer {
 public:
  DummyConsumer(EventQueue* event_queue, std::function<void()> callback,
                microseconds period = microseconds(0))
      : EventConsumer("SomeId", event_queue),
        period_(event_queue->ToTime(period)),
        callback_(callback) {}

  void HandleEvent() override {
    if (!period_.isZero()) {
      EnqueueIn(period_);
    }
    callback_();
  }

 private:
  EventQueueTime period_;
  std::function<void()> callback_;
};

class NetworkTest : public ::testing::Test {
 protected:
  NetworkTest()
      : event_queue_(),
        network_(event_queue_.RawMillisToTime(100), &event_queue_) {}

  void Run() { event_queue_.RunAndStopIn(milliseconds(kSimEndTimeMs)); }

  SimTimeEventQueue event_queue_;
  Network network_;
};

// A single device with a single UDP source. All packets from the source will
// be dropped, as there are no routes configured.
TEST_F(NetworkTest, SingleDevice) {
  Device device("SomeDevice", net::IPAddress(1), &event_queue_);
  network_.AddDevice(&device);

  UDPSource* udp_source =
      device.AddUDPGenerator(net::IPAddress(2), net::AccessLayerPort(100));

  // 100 bytes every 100 milliseconds
  DummyConsumer consumer(&event_queue_, [udp_source] {
    udp_source->AddData(kUDPPacketSize);
  }, milliseconds(100));
  consumer.EnqueueIn(EventQueueTime::ZeroTime());

  Run();

  DeviceStats stats = device.GetStats();
  ASSERT_EQ(10000ul, stats.bytes_seen);
  ASSERT_EQ(100ul, stats.packets_seen);
  ASSERT_EQ(10000ul, stats.bytes_failed_to_match);
  ASSERT_EQ(100ul, stats.packets_failed_to_match);
  ASSERT_EQ(0ul, stats.bytes_for_localhost);
  ASSERT_EQ(0ul, stats.packets_for_localhost);
  ASSERT_EQ(0ul, stats.num_rules);

  ASSERT_EQ(1ul, stats.connection_stats.size());

  auto it = stats.connection_stats.begin();
  net::FiveTuple tuple = it->first;
  ASSERT_EQ(tuple.dst_port(), net::AccessLayerPort(100));
  ASSERT_EQ(tuple.ip_src(), net::IPAddress(1));
  ASSERT_EQ(tuple.ip_dst(), net::IPAddress(2));
  ASSERT_NE(tuple.src_port(), kWildAccessLayerPort);
}

class TwoDeviceTest : public NetworkTest {
 protected:
  static net::GraphBuilder Graph() {
    net::GraphBuilder out(false);
    out.AddLink({"A", "B", net::DevicePortNumber(10), net::DevicePortNumber(20),
                 kRate, kDelay});
    out.AddLink({"B", "A", net::DevicePortNumber(20), net::DevicePortNumber(10),
                 kRate, kDelay});
    return out;
  }

  TwoDeviceTest()
      : graph_storage_(Graph()),
        device_a_("A", net::IPAddress(1), &event_queue_),
        device_b_("B", net::IPAddress(2), &event_queue_) {}

  virtual ~TwoDeviceTest(){};

  // Adds 2 devices.
  virtual void AddDevices() {
    network_.AddDevice(&device_a_);
    network_.AddDevice(&device_b_);
  }

  // Adds a link between the 2 devices.
  virtual void AddLink(size_t queue_size) {
    const net::GraphLink* link = graph_storage_.LinkPtrOrDie("A", "B");
    pipe_ = make_unique<Pipe>(*link, &event_queue_);
    queue_ = make_unique<FIFOQueue>(*link, queue_size, &event_queue_);
    network_.AddLink(queue_.get(), pipe_.get());
  }

  // Adds a match rule at the first device directing traffic from A to B.
  virtual void AddRoute() {
    MatchRuleKey key(
        kWildPacketTag, kWildDevicePortNumber,
        {net::FiveTuple(kWildIPAddress, net::IPAddress(2), kWildIPProto,
                        kWildAccessLayerPort, kWildAccessLayerPort)});
    auto action = make_unique<MatchRuleAction>(net::DevicePortNumber(10),
                                               kWildPacketTag, 100);
    auto rule = make_unique<MatchRule>(key);
    rule->AddAction(std::move(action));

    auto message = make_unique<SSCPAddOrUpdate>(
        kWildIPAddress, net::IPAddress(1), EventQueueTime(0), std::move(rule));
    device_a_.HandlePacket(std::move(message));
  }

  // Adds a UDP generator to send packets from A to B.
  virtual void AddGenerator(double gap_between_packets_ms) {
    UDPSource* udp_source =
        device_a_.AddUDPGenerator(net::IPAddress(2), net::AccessLayerPort(100));
    consumer_ = make_unique<DummyConsumer>(&event_queue_, [udp_source] {
      udp_source->AddData(kUDPPacketSize);
    }, microseconds(static_cast<uint64_t>(gap_between_packets_ms * 1000.0)));
    consumer_->EnqueueIn(EventQueueTime::ZeroTime());
  }

  void CheckStats(uint64_t gap_between_packets_ms) {
    auto a_stats = device_a_.GetStats();
    auto b_stats = device_b_.GetStats();
    auto pipe_stats = pipe_->GetStats();
    auto queue_stats = queue_->GetStats();

    double num_pkts =
        kSimEndTimeMs / static_cast<double>(gap_between_packets_ms);
    double num_bytes = num_pkts * kUDPPacketSize;

    ASSERT_EQ(num_bytes, a_stats.bytes_seen);
    ASSERT_EQ(num_pkts, a_stats.packets_seen);
    ASSERT_EQ(0ul, a_stats.bytes_failed_to_match);
    ASSERT_EQ(0ul, a_stats.packets_failed_to_match);
    ASSERT_EQ(0ul, a_stats.bytes_for_localhost);
    ASSERT_EQ(0ul, a_stats.packets_for_localhost);
    ASSERT_EQ(1ul, a_stats.num_rules);

    ASSERT_EQ(num_bytes, pipe_stats.bytes_tx);
    ASSERT_EQ(num_pkts, pipe_stats.pkts_tx);

    ASSERT_EQ(0ul, queue_stats.bytes_dropped);
    ASSERT_EQ(0ul, queue_stats.pkts_dropped);
    ASSERT_EQ(num_pkts, queue_stats.pkts_seen);
    ASSERT_EQ(num_bytes, queue_stats.bytes_seen);
    ASSERT_EQ(0ul, queue_stats.queue_size_bytes);
    ASSERT_EQ(0ul, queue_stats.queue_size_pkts);

    ASSERT_EQ(1ul, a_stats.connection_stats.size());
    ASSERT_EQ(1ul, b_stats.connection_stats.size());
  }

  std::unique_ptr<DummyConsumer> consumer_;
  std::unique_ptr<Pipe> pipe_;
  std::unique_ptr<Queue> queue_;

  net::GraphStorage graph_storage_;
  Device device_a_;
  Device device_b_;
};

// 2 devices with a single link between them.
TEST_F(TwoDeviceTest, SinglePipe) {
  AddDevices();
  AddLink(kUDPPacketSize);
  AddRoute();
  AddGenerator(100);
  Run();
  CheckStats(100);
}

// All packets are dropped at A's queue because it is too small to fit even a
// single packet.
TEST_F(TwoDeviceTest, SinglePipeQueueTooSmall) {
  AddDevices();
  AddLink(kUDPPacketSize - 1);
  AddRoute();
  AddGenerator(100);
  Run();

  auto pipe_stats = pipe_->GetStats();
  auto queue_stats = queue_->GetStats();
  ASSERT_EQ(0ul, pipe_stats.bytes_tx);
  ASSERT_EQ(0ul, pipe_stats.pkts_tx);
  ASSERT_EQ(100ul, queue_stats.pkts_dropped);
  ASSERT_EQ(10000ul, queue_stats.bytes_dropped);

  auto b_stats = device_b_.GetStats();
  ASSERT_EQ(0ul, b_stats.connection_stats.size());
}

// It should not matter which order the generators are added
TEST_F(TwoDeviceTest, SinglePipeGeneratorFirst) {
  AddDevices();
  AddLink(kUDPPacketSize);
  AddGenerator(100);
  AddRoute();
  Run();
  CheckStats(100);
}

// The offered load on the queue is higher than its drain rate. At the end of
// the sim the queue should be full.
TEST_F(TwoDeviceTest, SinglePipeSaturated) {
  AddDevices();
  AddLink(100 * kUDPPacketSize);  // Queue for 100 packets.
  AddRoute();
  AddGenerator(0.02);  // The queue can handle 12500 pkts/sec, this is about 50K
                       // pkts per sec.
  Run();

  auto pipe_stats = pipe_->GetStats();
  auto queue_stats = queue_->GetStats();

  ASSERT_EQ(100 * kUDPPacketSize, queue_stats.queue_size_bytes);
  ASSERT_EQ(100ul, queue_stats.queue_size_pkts);
  ASSERT_LT(0ul, pipe_stats.pkts_in_flight);
  ASSERT_LT(0ul, pipe_stats.bytes_in_flight);
}

class TwoDeviceTCPTest : public TwoDeviceTest {
 protected:
  TwoDeviceTCPTest() : TwoDeviceTest() {}

  void AddLink(size_t queue_size) override {
    TwoDeviceTest::AddLink(queue_size);
    const net::GraphLink* link = graph_storage_.LinkPtrOrDie("B", "A");
    reverse_pipe_ = make_unique<Pipe>(*link, &event_queue_);
    reverse_queue_ = make_unique<FIFOQueue>(*link, queue_size, &event_queue_);
    network_.AddLink(reverse_queue_.get(), reverse_pipe_.get());
  }

  void AddRoute() override {
    TwoDeviceTest::AddRoute();
    MatchRuleKey key(
        kWildPacketTag, kWildDevicePortNumber,
        {net::FiveTuple(kWildIPAddress, net::IPAddress(1), kWildIPProto,
                        kWildAccessLayerPort, kWildAccessLayerPort)});
    auto reverse_action = make_unique<MatchRuleAction>(
        net::DevicePortNumber(20), kWildPacketTag, 100);
    auto reverse_rule = make_unique<MatchRule>(key);
    reverse_rule->AddAction(std::move(reverse_action));

    auto message = make_unique<SSCPAddOrUpdate>(
        kWildIPAddress, net::IPAddress(1), EventQueueTime(0),
        std::move(reverse_rule));
    device_b_.HandlePacket(std::move(message));
  }

  // Adds a TCP generator with a number of bytes in the TX buffer.
  void AddTCPGenerator(uint64_t buffer_size_bytes) {
    TCPSource* tcp_source = device_a_.AddTCPGenerator(
        net::IPAddress(2), net::AccessLayerPort(100), 1500, 2000000);
    tcp_source->AddData(buffer_size_bytes);
  }

  std::unique_ptr<Pipe> reverse_pipe_;
  std::unique_ptr<Queue> reverse_queue_;
};

TEST_F(TwoDeviceTCPTest, DISABLED_SingleTransfer) {
  AddDevices();
  AddLink(2 * (kRate.bps() / 8.0) *
          std::chrono::duration<double>(kDelay).count());
  AddRoute();
  AddTCPGenerator(std::numeric_limits<uint64_t>::max());
  Run();

  auto a_stats = device_a_.GetStats();
  auto b_stats = device_b_.GetStats();
  auto pipe_stats = pipe_->GetStats();
  auto queue_stats = queue_->GetStats();

  double bytes_total = (kRate.bps() / 8.0) * (kSimEndTimeMs / 1000.0);
  double pkts_total = bytes_total / 1500.0;
  double pkts_and_acks = pkts_total * 2;
  double ack_bytes = pkts_total * 40.0;

  ASSERT_NEAR(bytes_total, a_stats.bytes_seen, bytes_total * 0.2);
  ASSERT_NEAR(pkts_and_acks, a_stats.packets_seen, pkts_and_acks * 0.2);
  ASSERT_EQ(0ul, a_stats.bytes_failed_to_match);
  ASSERT_EQ(0ul, a_stats.packets_failed_to_match);
  ASSERT_NEAR(ack_bytes, a_stats.bytes_for_localhost, ack_bytes * 0.2);
  ASSERT_NEAR(pkts_total, a_stats.packets_for_localhost, pkts_total * 0.2);
  ASSERT_EQ(1ul, a_stats.num_rules);

  ASSERT_NEAR(bytes_total, pipe_stats.bytes_tx, bytes_total * 0.2);
  ASSERT_NEAR(pkts_total, pipe_stats.pkts_tx, pkts_total * 0.2);

  ASSERT_LT(0ul, queue_stats.bytes_dropped);
  ASSERT_LT(0ul, queue_stats.pkts_dropped);
  ASSERT_NEAR(pkts_total, queue_stats.pkts_seen, pkts_total * 0.2);
  ASSERT_NEAR(bytes_total, queue_stats.bytes_seen, bytes_total * 0.2);
  ASSERT_LT(0ul, queue_stats.queue_size_bytes);
  ASSERT_LT(0ul, queue_stats.queue_size_pkts);

  ASSERT_EQ(1ul, a_stats.connection_stats.size());
  ASSERT_EQ(1ul, b_stats.connection_stats.size());
}

TEST_F(TwoDeviceTCPTest, SinglePacket) {
  AddDevices();
  AddLink(2 * (kRate.bps() / 8.0) *
          std::chrono::duration<double>(kDelay).count());
  AddRoute();
  AddTCPGenerator(1500);
  Run();

  auto a_stats = device_a_.GetStats();
  auto b_stats = device_b_.GetStats();

  ASSERT_EQ(1540.0, a_stats.bytes_seen);
  ASSERT_EQ(2ul, a_stats.packets_seen);
  ASSERT_EQ(1540.0, b_stats.bytes_seen);
  ASSERT_EQ(2ul, b_stats.packets_seen);
}

TEST_F(TwoDeviceTCPTest, SmallTransfer) {
  AddDevices();
  AddLink(2 * (kRate.bps() / 8.0) *
          std::chrono::duration<double>(kDelay).count());
  AddRoute();
  AddTCPGenerator(1000000);
  Run();

  auto a_stats = device_a_.GetStats();
  auto b_stats = device_b_.GetStats();

  double bytes_total = 1000000.0;
  double num_pkts = 1000000.0 / 1500.0;
  double pkts_and_acks = 2 * num_pkts;

  ASSERT_NEAR(bytes_total, a_stats.bytes_seen, bytes_total * 0.2);
  ASSERT_NEAR(pkts_and_acks, a_stats.packets_seen, pkts_and_acks * 0.2);
}

}  // namespace
}  // namespace htsim
}  // namespace ncode
