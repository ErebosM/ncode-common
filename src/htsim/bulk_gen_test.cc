#include "gtest/gtest.h"
#include "bulk_gen.h"

namespace nc {
namespace htsim {
namespace {

using namespace net;
using namespace std::chrono;

static constexpr IPAddress kSrc = IPAddress(1);
static constexpr IPAddress kDst = IPAddress(2);
static constexpr AccessLayerPort kSrcPort = AccessLayerPort(100);
static constexpr AccessLayerPort kDstPort = AccessLayerPort(200);
static constexpr size_t kSize = 100;
static constexpr milliseconds kTimeGap = milliseconds(100);
static constexpr char kSomeId[] = "Id";

class DummySource : public BulkPacketSource {
 public:
  DummySource(EventQueue* event_queue,
              size_t max = std::numeric_limits<size_t>::max())
      : i_(0), max_(max), event_queue_(event_queue) {}

  PacketPtr GetPacket(milliseconds at) {
    net::FiveTuple tuple(kSrc, kDst, kProtoTCP, kSrcPort, kDstPort);
    auto pkt =
        GetFreeList<UDPPacket>().New(tuple, kSize, event_queue_->ToTime(at));
    return std::move(pkt);
  }

  PacketPtr NextPacket() override {
    if (i_ == max_) {
      return PacketPtr();
    }

    return GetPacket(kTimeGap * i_++);
  }

 private:
  size_t i_;
  size_t max_;
  EventQueue* event_queue_;
};

class DummyHandler : public htsim::PacketHandler {
 public:
  DummyHandler() : i_(0) {}

  void HandlePacket(PacketPtr pkt) override {
    Unused(pkt);
    ++i_;
  }

  size_t i() const { return i_; }

 private:
  size_t i_;
};

class BulkGenFixture : public ::testing::Test {
 protected:
  SimTimeEventQueue event_queue_;
  DummyHandler out_;
};

TEST_F(BulkGenFixture, Init) {
  auto source = make_unique<DummySource>(&event_queue_);
  std::vector<std::unique_ptr<BulkPacketSource>> sources;
  sources.emplace_back(std::move(source));

  BulkPacketGenerator packet_generator(kSomeId, std::move(sources), &out_,
                                       &event_queue_);
  ASSERT_EQ(0ul, out_.i());
}

TEST_F(BulkGenFixture, SingleSourceLongRunning) {
  auto source = make_unique<DummySource>(&event_queue_);
  std::vector<std::unique_ptr<BulkPacketSource>> sources;
  sources.emplace_back(std::move(source));

  BulkPacketGenerator packet_generator(kSomeId, std::move(sources), &out_,
                                       &event_queue_);
  event_queue_.RunAndStopIn(hours(10));

  // 10 hours, 100 ms between packets -> 360000 packets.
  ASSERT_EQ(360000ul, out_.i());
}

TEST_F(BulkGenFixture, MultiSourcesLongRunning) {
  auto source_one = make_unique<DummySource>(&event_queue_);
  auto source_two = make_unique<DummySource>(&event_queue_);
  std::vector<std::unique_ptr<BulkPacketSource>> sources;
  sources.emplace_back(std::move(source_one));
  sources.emplace_back(std::move(source_two));

  BulkPacketGenerator packet_generator(kSomeId, std::move(sources), &out_,
                                       &event_queue_);
  event_queue_.RunAndStopIn(hours(10));
  ASSERT_EQ(2 * 360000ul, out_.i());
}

TEST_F(BulkGenFixture, SingleShortSource) {
  auto source = make_unique<DummySource>(&event_queue_, 10);
  std::vector<std::unique_ptr<BulkPacketSource>> sources;
  sources.emplace_back(std::move(source));

  BulkPacketGenerator packet_generator(kSomeId, std::move(sources), &out_,
                                       &event_queue_);
  event_queue_.RunAndStopIn(hours(10));
  ASSERT_EQ(10ul, out_.i());
}

TEST_F(BulkGenFixture, SingleThreadShortSource) {
  auto source = make_unique<DummySource>(&event_queue_, 10);
  std::vector<std::unique_ptr<BulkPacketSource>> sources;
  sources.emplace_back(std::move(source));

  SingleThreadBulkPacketGenerator packet_generator(kSomeId, std::move(sources),
                                                   &out_, &event_queue_);
  event_queue_.RunAndStopIn(hours(10));
  ASSERT_EQ(10ul, out_.i());
}

}  // namespace
}  // namespace htsim
}  // namespace ncode
