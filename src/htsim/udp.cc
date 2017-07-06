#include "udp.h"

#include <type_traits>

#include "../free_list.h"

namespace nc {
namespace htsim {

UDPSource::UDPSource(const std::string& id, const net::FiveTuple& five_tuple,
                     PacketHandler* out_handler, EventQueue* event_queue)
    : Connection(id, five_tuple, out_handler, event_queue) {}

UDPSink::UDPSink(const std::string& id, const net::FiveTuple& five_tuple,
                 PacketHandler* out_handler, EventQueue* event_queue)
    : Connection(id, five_tuple, out_handler, event_queue) {}

void UDPSource::AddData(uint64_t pkt_size) {
  auto pkt_ptr = GetFreeList<UDPPacket>().New(five_tuple_, pkt_size,
                                              event_queue_->CurrentTime());
  SendPacket(std::move(pkt_ptr));
}

void UDPSink::ReceivePacket(PacketPtr pkt) { Unused(pkt); }

}  // namespace htsim
}  // namespace nc
