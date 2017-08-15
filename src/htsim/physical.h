#ifndef HTSIM_PHYSICAL_H
#define HTSIM_PHYSICAL_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <netinet/ether.h>
#include <sys/ioctl.h>
#include <netinet/udp.h>
#include <net/if.h>

#include "../common.h"
#include "../free_list.h"
#include "../event_queue.h"
#include "../net/net_common.h"
#include "../net/pcap.h"
#include "htsim.h"

namespace nc {
namespace htsim {

// Creates a real-life physical packet for a simulated one and sends it out a
// given interface. Uses raw sockets, so make sure to have enough privileges if
// using this.
class PhysicalInterfacePacketHandler : public PacketHandler {
 private:
  static constexpr size_t kSendBufferSize = 2048;

  PhysicalInterfacePacketHandler(const std::string& iface_name) {
    socket_ = socket(AF_PACKET, SOCK_RAW, IPPROTO_RAW);
    CHECK(socket_ != -1);

    ifreq if_request;
    memset(&if_request, 0, sizeof(struct ifreq));
    strncpy(if_request.ifr_name, iface_name.c_str(), IFNAMSIZ - 1);

    CHECK(ioctl(socket_, SIOCGIFINDEX, &if_request) == 0)
        << "Cannot get interface index for " << iface_name;
    if_index_ = if_request.ifr_ifindex;

    CHECK(ioctl(socket_, SIOCGIFHWADDR, &if_request) == 0)
        << "Cannot get HW address for " << iface_name;

    ether_header* eh = reinterpret_cast<ether_header*>(sendbuf_);
    memset(eh, 0, sizeof(struct ether_header));
    eh->ether_shost[0] = ((uint8_t*)&if_request.ifr_hwaddr.sa_data)[0];
    eh->ether_shost[1] = ((uint8_t*)&if_request.ifr_hwaddr.sa_data)[1];
    eh->ether_shost[2] = ((uint8_t*)&if_request.ifr_hwaddr.sa_data)[2];
    eh->ether_shost[3] = ((uint8_t*)&if_request.ifr_hwaddr.sa_data)[3];
    eh->ether_shost[4] = ((uint8_t*)&if_request.ifr_hwaddr.sa_data)[4];
    eh->ether_shost[5] = ((uint8_t*)&if_request.ifr_hwaddr.sa_data)[5];
    eh->ether_type = htons(ETH_P_IP);

    iph_ = reinterpret_cast<iphdr*>(sendbuf_ + sizeof(struct ether_header));
    iph_->ihl = 5;
    iph_->version = 4;
    iph_->tos = 0;
  }

  void SendUDP(const UDPPacket& udp_packet) {
    iph_->id = htons(udp_packet.ip_id());
    iph_->ttl = udp_packet.ttl();
    iph_->protocol = 0x11;

    uint32_t ip_src_raw = udp_packet.five_tuple().ip_src().Raw();
    uint32_t ip_dst_raw = udp_packet.five_tuple().ip_dst().Raw();
    iph_->saddr = htonl(ip_src_raw);
    iph_->daddr = htonl(ip_dst_raw);

    udphdr* udph = reinterpret_cast<udphdr*>(sendbuf_ + sizeof(struct iphdr) +
                                             sizeof(struct ether_header));

    uint16_t src_port_raw = udp_packet.five_tuple().src_port().Raw();
    uint16_t dst_port_raw = udp_packet.five_tuple().dst_port().Raw();

    udph->source = htons(src_port_raw);
    udph->dest = htons(dst_port_raw);
    udph->check = 0;

    uint32_t total_len = udp_packet.payload_bytes() + sizeof(struct udphdr);
    udph->len = htons(total_len);

    uint32_t total_len_ip = total_len + sizeof(struct iphdr);
    CHECK(total_len_ip <= std::numeric_limits<uint16_t>::max());
    iph_->tot_len = htons(total_len_ip);
  }

  unsigned short csum(unsigned short* buf, int nwords) {
    unsigned long sum;
    for (sum = 0; nwords > 0; nwords--) sum += *buf++;
    sum = (sum >> 16) + (sum & 0xffff);
    sum += (sum >> 16);
    return (unsigned short)(~sum);
  }

  // The raw socket.
  int socket_;

  // The index of the interface.
  int if_index_;

  // The sending buffer. Scratch space.
  char sendbuf_[kSendBufferSize];

  // Pointer to the ip header in sendbuf_.
  iphdr* iph_;
};

}  // namespace htsim
}  // namespace nc

#endif
