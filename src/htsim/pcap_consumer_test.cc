#include "pcap_consumer.h"

#include "../common.h"
#include "../net/net_common.h"
#include "queue.h"
#include "gtest/gtest.h"

namespace nc {
namespace htsim {
namespace {

class DummyPacketHandler : public PacketHandler {
 public:
  DummyPacketHandler(EventQueue* event_queue)
      : i_(0), event_queue_(event_queue) {}

  size_t packet_count() const { return i_; }
  std::chrono::nanoseconds last_packet_rx_at() const {
    return event_queue_->TimeToNanos(last_packet_rx_at_);
  };

  void HandlePacket(PacketPtr pkt) override {
    Unused(pkt);
    ++i_;
    ASSERT_TRUE(last_packet_rx_at_ <= pkt->time_sent());
    last_packet_rx_at_ = event_queue_->CurrentTime();
    ASSERT_EQ(last_packet_rx_at_, pkt->time_sent());
  }

 private:
  size_t i_;
  EventQueue* event_queue_;
  EventQueueTime last_packet_rx_at_;
};

class ConsumerTest : public ::testing::Test {
 protected:
  ConsumerTest()
      : dummy_handler_(&event_queue_),
        pipe_("A", "B", EventQueueTime::ZeroTime(), &event_queue_) {
    pipe_.Connect(&dummy_handler_);
    source_ = make_unique<pcap::DefaultOfflineSourceProvider>(
        "pcap_test_data/output_dump");
  }

  SimTimeEventQueue event_queue_;
  DummyPacketHandler dummy_handler_;
  Pipe pipe_;
  std::unique_ptr<pcap::DefaultOfflineSourceProvider> source_;

  std::vector<std::unique_ptr<BulkPacketSource>> sources_;
};

TEST_F(ConsumerTest, MultiConsume) {
  auto pcap_source =
      make_unique<PcapPacketGen>(std::move(source_), &event_queue_);
  std::vector<std::unique_ptr<BulkPacketSource>> sources;
  sources.emplace_back(std::move(pcap_source));

  BulkPacketGenerator bulk_generator("PcapPacketGen", std::move(sources),
                                     &pipe_, &event_queue_);

  // An hour should be more than the timestamp of the last packet in the file.
  event_queue_.RunAndStopIn(std::chrono::hours(1));

  // Delta between last and first packet in the trace.
  std::chrono::microseconds last_pkt(37264);
  ASSERT_EQ(last_pkt, dummy_handler_.last_packet_rx_at());
  ASSERT_EQ(9933ul, pipe_.GetStats().pkts_tx);
  ASSERT_EQ(dummy_handler_.packet_count(), pipe_.GetStats().pkts_tx);
}

}  // namespace
}  // namespace htsim
}  // namespace ncode
