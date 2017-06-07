#include "flow_driver.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "network.h"

namespace nc {
namespace htsim {
namespace {

using namespace std::chrono;
static constexpr net::FiveTuple kFiveTuple =
    net::FiveTuple(net::IPAddress(1), net::IPAddress(2), net::IPProto(3),
                   net::AccessLayerPort(4), net::AccessLayerPort(5));
static constexpr size_t kUDPPacketSize = 100;

class UDPFlowDriverTest : public ::testing::Test {
 protected:
  UDPFlowDriverTest()
      : udp_flow_driver_(kUDPPacketSize, event_queue_.ToTime(seconds(1))),
        another_udp_flow_driver_(kUDPPacketSize,
                                 event_queue_.ToTime(seconds(1))) {}

  SimTimeEventQueue event_queue_;
  ConstantRateFlowDriver udp_flow_driver_;
  ConstantRateFlowDriver another_udp_flow_driver_;
};

// No change from the initial rate of 0.
TEST_F(UDPFlowDriverTest, NoKeyframes) {
  AddDataEvent next_event = udp_flow_driver_.Next();
  ASSERT_EQ(EventQueueTime::MaxTime(), next_event.at);
  ASSERT_EQ(0ul, next_event.bytes);
}

// Multiple key frames at 0.
TEST_F(UDPFlowDriverTest, MultiZeroKeyframes) {
  udp_flow_driver_.AddRateChangeKeyframes({{EventQueueTime::ZeroTime(), 0},
                                           {EventQueueTime::ZeroTime(), 0},
                                           {EventQueueTime::ZeroTime(), 0}});
  AddDataEvent next_event = udp_flow_driver_.Next();
  ASSERT_EQ(EventQueueTime::MaxTime(), next_event.at);
  ASSERT_EQ(0ul, next_event.bytes);
}

// Bring the rate up to 1 pkt/sec at the first second.
TEST_F(UDPFlowDriverTest, ConstantRate) {
  auto at = event_queue_.ToTime(seconds(1));
  udp_flow_driver_.AddRateChangeKeyframes({{at, kUDPPacketSize * 8}});

  for (size_t i = 0; i < 100; ++i) {
    // Will set the rate at 1 pkt/sec at 1 sec. The first packet will be
    // generated at 2 sec.
    ASSERT_EQ(event_queue_.ToTime(seconds(i + 2)), udp_flow_driver_.Next().at);
  }
}

TEST_F(UDPFlowDriverTest, DISABLED_OverlappingKeyFrames) {
  auto at1 = event_queue_.ToTime(seconds(1));
  udp_flow_driver_.AddRateChangeKeyframes({{at1, kUDPPacketSize * 8}});
  udp_flow_driver_.AddRateChangeKeyframes({{at1, 2 * kUDPPacketSize * 8}});

  auto start_at = milliseconds(1050);
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_EQ(event_queue_.ToTime(start_at + i * milliseconds(50)),
              udp_flow_driver_.Next().at);
  }
}

// Change the rate to the same rate mulitple times.
TEST_F(UDPFlowDriverTest, ConstantRateMultiRateChanges) {
  auto at1 = event_queue_.ToTime(seconds(1));
  auto at2 = event_queue_.ToTime(seconds(5));
  auto at3 = event_queue_.ToTime(seconds(50));
  udp_flow_driver_.AddRateChangeKeyframes({{at1, kUDPPacketSize * 8},
                                           {at2, kUDPPacketSize * 8},
                                           {at3, kUDPPacketSize * 8}});
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_EQ(event_queue_.ToTime(seconds(i + 2)), udp_flow_driver_.Next().at);
  }
}

// Drop the rate to 0.
TEST_F(UDPFlowDriverTest, RateToZero) {
  auto at1 = event_queue_.ToTime(seconds(30));
  auto at2 = event_queue_.ToTime(seconds(70));
  udp_flow_driver_.AddRateChangeKeyframes(
      {{at1, kUDPPacketSize * 8}, {at2, 0}});

  // The first 40 calls should produce times that are a second apart starting
  // from 30 sec. The rest of the calls should produce max time.
  for (size_t i = 0; i < 40; ++i) {
    auto next = udp_flow_driver_.Next();
    ASSERT_EQ(event_queue_.ToTime(seconds(i + 31)), next.at);
    ASSERT_EQ(kUDPPacketSize, next.bytes);
  }

  for (size_t i = 0; i < 100; ++i) {
    auto next = udp_flow_driver_.Next();
    ASSERT_EQ(EventQueueTime::MaxTime(), next.at);
    ASSERT_EQ(0ul, next.bytes);
  }
}

// 10pps -> 1pps -> 20pps
TEST_F(UDPFlowDriverTest, MultiRateChange) {
  auto at1 = event_queue_.ToTime(seconds(1));
  auto at2 = event_queue_.ToTime(seconds(30));
  auto at3 = event_queue_.ToTime(seconds(60));

  udp_flow_driver_.AddRateChangeKeyframes({{at1, 10 * kUDPPacketSize * 8},
                                           {at2, kUDPPacketSize * 8},
                                           {at3, 20 * kUDPPacketSize * 8}});
  // 10pps for the period 1.1s -- 30s is 290 calls to Next().
  auto start_at = milliseconds(1100);
  for (size_t i = 0; i < 290; ++i) {
    ASSERT_EQ(event_queue_.ToTime(start_at + i * milliseconds(100)),
              udp_flow_driver_.Next().at);
  }

  // 1pps for the period 30s -- 60s
  start_at = milliseconds(31000);
  for (size_t i = 0; i < 30; ++i) {
    ASSERT_EQ(event_queue_.ToTime(start_at + i * milliseconds(1000)),
              udp_flow_driver_.Next().at);
  }

  // 20pps for the rest.
  start_at = milliseconds(60050);
  for (size_t i = 0; i < 10000; ++i) {
    ASSERT_EQ(event_queue_.ToTime(start_at + i * milliseconds(50)),
              udp_flow_driver_.Next().at);
  }
}

// Key frames should be sorted upon insertion.
TEST_F(UDPFlowDriverTest, TestInsertOrder) {
  auto at1 = event_queue_.ToTime(seconds(1));
  auto at2 = event_queue_.ToTime(seconds(30));
  auto at3 = event_queue_.ToTime(seconds(60));
  auto at4 = event_queue_.ToTime(seconds(150));

  udp_flow_driver_.AddRateChangeKeyframes({{at1, 10 * kUDPPacketSize * 8},
                                           {at2, kUDPPacketSize * 8},
                                           {at3, 20 * kUDPPacketSize * 8},
                                           {at4, 0}});
  another_udp_flow_driver_.AddRateChangeKeyframes(
      {{at4, 0},
       {at1, 10 * kUDPPacketSize * 8},
       {at3, 20 * kUDPPacketSize * 8},
       {at2, kUDPPacketSize * 8}});
  for (size_t i = 0; i < 10000; ++i) {
    auto next_one = udp_flow_driver_.Next();
    auto next_two = another_udp_flow_driver_.Next();
    ASSERT_EQ(next_one.at, next_two.at);
    ASSERT_EQ(next_one.bytes, next_two.bytes);
  }
}

class TCPFlowDriverTest : public ::testing::Test {
 protected:
  ManualFlowDriver manual_flow_driver_;
  ManualFlowDriver another_manual_flow_driver_;
};

TEST_F(TCPFlowDriverTest, NoEvents) {
  AddDataEvent next_event = manual_flow_driver_.Next();
  ASSERT_EQ(kAddDataInfinity, next_event);
}

TEST_F(TCPFlowDriverTest, SingleEvent) {
  AddDataEvent add_data_event(EventQueueTime(10), 10);
  manual_flow_driver_.AddData({add_data_event});

  ASSERT_EQ(add_data_event, manual_flow_driver_.Next());
  ASSERT_EQ(kAddDataInfinity, manual_flow_driver_.Next());
}

TEST_F(TCPFlowDriverTest, DISABLED_OverlappingEvents) {
  auto at1 = EventQueueTime(1);
  ASSERT_EQ(AddDataEvent(at1, 10), manual_flow_driver_.Next());
  ASSERT_EQ(AddDataEvent(at1, 50), manual_flow_driver_.Next());

  ASSERT_EQ(AddDataEvent(at1, 10), manual_flow_driver_.Next());
  ASSERT_EQ(AddDataEvent(at1, 50), manual_flow_driver_.Next());
}

TEST_F(TCPFlowDriverTest, MultiEvent) {
  auto at1 = EventQueueTime(1);
  auto at2 = EventQueueTime(30);
  auto at3 = EventQueueTime(60);
  auto at4 = EventQueueTime(150);

  manual_flow_driver_.AddData({{at1, 10}, {at2, 50}, {at3, 5}, {at4, 12}});
  ASSERT_EQ(AddDataEvent(at1, 10), manual_flow_driver_.Next());
  ASSERT_EQ(AddDataEvent(at2, 50), manual_flow_driver_.Next());
  ASSERT_EQ(AddDataEvent(at3, 5), manual_flow_driver_.Next());
  ASSERT_EQ(AddDataEvent(at4, 12), manual_flow_driver_.Next());
  ASSERT_EQ(kAddDataInfinity, manual_flow_driver_.Next());
}

TEST_F(TCPFlowDriverTest, EventOrder) {
  auto at1 = EventQueueTime(1);
  auto at2 = EventQueueTime(30);
  auto at3 = EventQueueTime(60);
  auto at4 = EventQueueTime(150);

  manual_flow_driver_.AddData({{at1, 10}, {at2, 50}, {at3, 5}, {at4, 12}});
  another_manual_flow_driver_.AddData(
      {{at2, 50}, {at1, 10}, {at4, 12}, {at3, 5}});
  ASSERT_EQ(another_manual_flow_driver_.Next(), manual_flow_driver_.Next());
  ASSERT_EQ(another_manual_flow_driver_.Next(), manual_flow_driver_.Next());
  ASSERT_EQ(another_manual_flow_driver_.Next(), manual_flow_driver_.Next());
  ASSERT_EQ(another_manual_flow_driver_.Next(), manual_flow_driver_.Next());
  ASSERT_EQ(another_manual_flow_driver_.Next(), manual_flow_driver_.Next());
}

TEST(ExpObjectSizeAndWaitTimeGenerator, MeanValues) {
  size_t mean_object_size = 1000;
  std::chrono::milliseconds mean_wait_time(100);

  SimTimeEventQueue event_queue;
  DefaultObjectSizeAndWaitTimeGenerator gen(
      mean_object_size, false, mean_wait_time, false, 1.0, &event_queue);

  size_t num_objects = 100000;
  double total_object_sizes = 0;
  double total_times = 0;

  for (size_t i = 0; i < num_objects; ++i) {
    ObjectSizeAndWaitTime next = gen.Next();
    size_t time_ms = event_queue.TimeToRawMillis(next.wait_time);

    ASSERT_TRUE(next.object_size > 0);
    ASSERT_TRUE(time_ms > 0);
    total_object_sizes += next.object_size;
    total_times += time_ms;
  }

  ASSERT_NEAR(total_object_sizes / num_objects, mean_object_size,
              0.1 * mean_object_size);
  ASSERT_NEAR(total_times / num_objects, mean_wait_time.count(),
              0.1 * mean_wait_time.count());
}

class MockConnection : public Connection {
 public:
  MockConnection(EventQueue* event_queue)
      : Connection("Mock", kFiveTuple, nullptr, event_queue) {}

  void ReceivePacket(PacketPtr pkt) override { Unused(pkt); }

  MOCK_METHOD0(Close, void());
  MOCK_METHOD1(AddData, void(uint64_t data_bytes));
};

TEST(FlowPackTest, SingleDriver) {
  SimTimeEventQueue event_queue;

  MockConnection mock_connection(&event_queue);
  // There should be 89 calls to AddData, each with kUDPPacket size as argument.
  EXPECT_CALL(mock_connection, AddData(kUDPPacketSize)).Times(89);
  EXPECT_CALL(mock_connection, Close()).Times(0);

  auto flow_driver = make_unique<ConstantRateFlowDriver>(
      kUDPPacketSize, event_queue.ToTime(seconds(1)));

  auto at = event_queue.ToTime(seconds(10));
  // 1pps rate.
  flow_driver->AddRateChangeKeyframes({{at, kUDPPacketSize * 8}});

  // Have to be careful to allocate the pack on the heap, as it contains a very
  // large std::array which will overflow the stack.
  auto pack = make_unique<FlowPack>("FlowPack", &event_queue);
  pack->AddDriver(std::move(flow_driver), &mock_connection);
  pack->Init();

  event_queue.RunAndStopIn(seconds(100));
}

TEST(FlowPackTest, MultiDrivers) {
  SimTimeEventQueue event_queue;

  MockConnection mock_connection_one(&event_queue);
  EXPECT_CALL(mock_connection_one, AddData(kUDPPacketSize)).Times(89);
  EXPECT_CALL(mock_connection_one, Close()).Times(0);

  MockConnection mock_connection_two(&event_queue);
  EXPECT_CALL(mock_connection_two, AddData(2 * kUDPPacketSize)).Times(159);
  EXPECT_CALL(mock_connection_two, Close()).Times(0);

  auto flow_driver_one = make_unique<ConstantRateFlowDriver>(
      kUDPPacketSize, event_queue.ToTime(seconds(1)));
  auto at = event_queue.ToTime(seconds(10));
  flow_driver_one->AddRateChangeKeyframes({{at, kUDPPacketSize * 8}});

  auto flow_driver_two = make_unique<ConstantRateFlowDriver>(
      2 * kUDPPacketSize, event_queue.ToTime(seconds(1)));
  at = event_queue.ToTime(seconds(20));
  flow_driver_two->AddRateChangeKeyframes({{at, 4 * kUDPPacketSize * 8}});

  auto pack = make_unique<FlowPack>("FlowPack", &event_queue);
  pack->AddDriver(std::move(flow_driver_one), &mock_connection_one);
  pack->AddDriver(std::move(flow_driver_two), &mock_connection_two);
  pack->Init();

  event_queue.RunAndStopIn(seconds(100));
}

}  // namespace
}  // namespace htsim
}  // namespace ncode
