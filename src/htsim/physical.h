#ifndef HTSIM_PHYSICAL_H
#define HTSIM_PHYSICAL_H

#include <pcap/pcap.h>
#include <stddef.h>
#include <iostream>

#include "../net/pcap.h"
#include "packet.h"

namespace nc {
namespace htsim {

// Creates a real-life physical packet for a simulated one and sends it out a
// given interface.
class PhysicalInterfacePacketHandler : public PacketHandler {
 public:
  PhysicalInterfacePacketHandler(const std::string& iface_name);

  void HandlePacket(PacketPtr pkt) override;

 private:
  static constexpr size_t kSendBufferSize = 2048;

  void SendUDP(const UDPPacket& udp_packet);

  // The pcap handle.
  pcap_t* pcap_;

  // The sending buffer. Scratch space.
  char sendbuf_[kSendBufferSize];

  // Pointer to the ip header in sendbuf_.
  pcap::IPHeader* iph_;

  // If the interface is a loopback we have to use a null link-layer header
  // instead of the ethernet one. This field gives the size of the link-layer
  // header.
  uint8_t ll_header_size_;
};

}  // namespace htsim
}  // namespace nc

#endif
