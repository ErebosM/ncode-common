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

class FixedRateUDPGen : public EventConsumer {
 public:
  FixedRateUDPGen(const net::FiveTuple& five_tuple, net::Bandwidth rate,
                  htsim::PacketHandler* handler, EventQueue* event_queue)
      : EventConsumer("FixedRateUDPGen", event_queue),
        five_tuple_(five_tuple),
        handler_(handler) {
    double rate_bps = rate.bps();
    double rate_BytesPerSecond = rate_bps / 8;

    size_t packet_size_bytes =
        kPacketPayloadBytes + sizeof(nc::pcap::EtherHeader) +
        sizeof(nc::pcap::TCPHeader) + sizeof(nc::pcap::UDPHeader);
    double rate_PktsPerSecond = rate_BytesPerSecond / packet_size_bytes;
    auto duration = std::chrono::duration<double>(1 / rate_PktsPerSecond);
    period_ = event_queue->ToTime(duration);
    CHECK(five_tuple_.ip_proto() == net::kProtoUDP);
    EnqueueIn(period_);
  }

  void HandleEvent() override {
    auto packet = GetFreeList<htsim::UDPPacket>().New(
        five_tuple_, kPacketPayloadBytes, event_queue()->CurrentTime());
    handler_->HandlePacket(std::move(packet));
    EnqueueIn(period_);
  }

 private:
  net::FiveTuple five_tuple_;
  EventQueueTime period_;
  htsim::PacketHandler* handler_;
};

int main(int argc, char** argv) {
  Unused(argc);
  Unused(argv);

  htsim::PhysicalInterfacePacketHandler physical_handler("lo0");
  RealTimeEventQueue event_queue(true);

  net::FiveTuple five_tuple(
      net::StringToIPOrDie("10.0.0.45"), net::StringToIPOrDie("10.0.0.49"),
      net::kProtoUDP, net::AccessLayerPort(4352), net::AccessLayerPort(4352));

  FixedRateUDPGen udp_gen(five_tuple, net::Bandwidth::FromGBitsPerSecond(1),
                          &physical_handler, &event_queue);
  event_queue.RunAndStopIn(hours(99));
}
