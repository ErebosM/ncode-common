#include <gflags/gflags.h>
#include <stddef.h>
#include <chrono>
#include <type_traits>

#include "../common.h"
#include "../event_queue.h"
#include "../free_list.h"
#include "../logging.h"
#include "../net/net_common.h"
#include "packet.h"
#include "physical.h"

using namespace nc;
using namespace std::chrono;

static constexpr size_t kPacketPayloadBytes = 1000;

DEFINE_string(iface, "lo0", "Interface to send packets out of");
DEFINE_double(rate_Mbps, 1000.0, "Rate in Mbps.");

class FixedRateUDPGen : public EventConsumer {
 public:
  FixedRateUDPGen(const net::FiveTuple& five_tuple, net::Bandwidth rate,
                  htsim::PacketHandler* handler, EventQueue* event_queue)
      : EventConsumer("FixedRateUDPGen", event_queue),
        start_(event_queue->CurrentTime()),
        five_tuple_(five_tuple),
        handler_(handler) {
    count_ = 0;
    double rate_bps = rate.bps();
    double rate_BytesPerSecond = rate_bps / 8;

    size_t packet_size_bytes =
        kPacketPayloadBytes + sizeof(nc::pcap::EtherHeader) +
        sizeof(nc::pcap::TCPHeader) + sizeof(nc::pcap::UDPHeader);
    double rate_PktsPerSecond = rate_BytesPerSecond / packet_size_bytes;
    auto duration = std::chrono::duration<double>(1 / rate_PktsPerSecond);
    LOG(ERROR) << "period sec " << duration.count() << " per sec "
               << static_cast<uint64_t>(rate_PktsPerSecond);
    period_ = event_queue->ToTime(duration);
    CHECK(five_tuple_.ip_proto() == net::kProtoUDP);
    EnqueueIn(period_);
  }

  void HandleEvent() override {
    auto packet = GetFreeList<htsim::UDPPacket>().New(
        five_tuple_, kPacketPayloadBytes, event_queue()->CurrentTime());
    handler_->HandlePacket(std::move(packet));
    ++count_;
    EnqueueIn(period_);
  }

  void PrintStats() const {
    EventQueueTime now = event_queue()->CurrentTime();
    EventQueueTime delta = now - start_;
    double delta_sec =
        duration_cast<duration<double>>(event_queue()->TimeToNanos(delta))
            .count();
    double period_sec =
        duration_cast<duration<double>>(event_queue()->TimeToNanos(period_))
            .count();

    double expected = delta_sec / period_sec;
    LOG(INFO) << "Expected " << expected << " got " << count_;
  }

 private:
  EventQueueTime start_;
  uint64_t count_;

  net::FiveTuple five_tuple_;
  EventQueueTime period_;
  htsim::PacketHandler* handler_;
};

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  htsim::PhysicalInterfacePacketHandler physical_handler(FLAGS_iface);
  BusyWaitEventQueue event_queue;

  net::FiveTuple five_tuple(
      net::StringToIPOrDie("10.0.0.45"), net::StringToIPOrDie("10.0.0.49"),
      net::kProtoUDP, net::AccessLayerPort(4352), net::AccessLayerPort(4352));

  FixedRateUDPGen udp_gen(five_tuple,
                          net::Bandwidth::FromMBitsPerSecond(FLAGS_rate_Mbps),
                          &physical_handler, &event_queue);
  event_queue.RunAndStopIn(seconds(100));
  udp_gen.PrintStats();
}
