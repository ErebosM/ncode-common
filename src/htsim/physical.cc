#include "physical.h"

#if defined(__linux__)
#include <net/if.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <string.h>
#elif defined(__APPLE__)
#include <ifaddrs.h>
#include <net/if_dl.h>
#else
#error no definition for GetMacAddress() on this platform!
#endif

namespace nc {
namespace htsim {

#if defined(__linux__)
static bool GetMacAddress(u_char* mac_addr, const char* if_name) {
  ifreq ifinfo;
  strcpy(ifinfo.ifr_name, if_name);
  int sd = socket(AF_INET, SOCK_DGRAM, 0);
  int result = ioctl(sd, SIOCGIFHWADDR, &ifinfo);
  close(sd);

  if (result == 0) {
    memcpy(mac_addr, ifinfo.ifr_hwaddr.sa_data, IFHWADDRLEN);
    return true;
  } else {
    return false;
  }
}
#elif defined(__APPLE__)
static bool GetMacAddress(u_char* mac_addr, const char* if_name) {
  ifaddrs* iflist;
  bool found = false;
  if (getifaddrs(&iflist) == 0) {
    for (ifaddrs* cur = iflist; cur; cur = cur->ifa_next) {
      if ((cur->ifa_addr->sa_family == AF_LINK) &&
          (strcmp(cur->ifa_name, if_name) == 0) && cur->ifa_addr) {
        sockaddr_dl* sdl = (sockaddr_dl*)cur->ifa_addr;
        memcpy(mac_addr, LLADDR(sdl), sdl->sdl_alen);
        found = true;
        break;
      }
    }

    freeifaddrs(iflist);
  }
  return found;
}
#else
#error no definition for GetMacAddress() on this platform!
#endif

static unsigned short csum(unsigned short* buf, int nwords) {
  unsigned long sum;
  for (sum = 0; nwords > 0; nwords--) sum += *buf++;
  sum = (sum >> 16) + (sum & 0xffff);
  sum += (sum >> 16);
  return (unsigned short)(~sum);
}

PhysicalInterfacePacketHandler::PhysicalInterfacePacketHandler(
    const std::string& iface_name) {
  char pcap_errbuf[PCAP_ERRBUF_SIZE];
  pcap_errbuf[0] = '\0';
  pcap_ = pcap_open_live(iface_name.c_str(), 96, 0, 0, pcap_errbuf);
  CHECK(pcap_errbuf[0] == '\0');
  CHECK(pcap_ != nullptr);

  int link_type = pcap_datalink(pcap_);
  if (link_type == DLT_EN10MB) {
    // Ethernet interface. Regular ethernet header.
    pcap::EtherHeader* eh = reinterpret_cast<pcap::EtherHeader*>(sendbuf_);
    memset(eh, 0, sizeof(pcap::EtherHeader));
    CHECK(GetMacAddress(eh->ether_shost, iface_name.c_str()));
    eh->ether_type = htons(0x0800);
    ll_header_size_ = sizeof(pcap::EtherHeader);
  } else if (link_type == DLT_NULL) {
    // Loopback interface, no ethernet header, just a null pseudoheader
    uint32_t* header = reinterpret_cast<uint32_t*>(sendbuf_);
    *header = 2;  // IPv4.
    ll_header_size_ = 4;
  } else {
    LOG(FATAL) << "Bad link on interface " << iface_name;
  }

  iph_ = reinterpret_cast<pcap::IPHeader*>(sendbuf_ + ll_header_size_);
  iph_->ip_hl = 5;
  iph_->ip_v = 4;
  iph_->ip_tos = 0;
}

void PhysicalInterfacePacketHandler::SendUDP(const UDPPacket& udp_packet) {
  iph_->ip_id = htons(udp_packet.ip_id());
  iph_->ip_ttl = udp_packet.ttl();
  iph_->ip_p = 0x11;

  uint32_t ip_src_raw = udp_packet.five_tuple().ip_src().Raw();
  uint32_t ip_dst_raw = udp_packet.five_tuple().ip_dst().Raw();
  iph_->ip_src.s_addr = htonl(ip_src_raw);
  iph_->ip_dst.s_addr = htonl(ip_dst_raw);

  pcap::UDPHeader* udph = reinterpret_cast<pcap::UDPHeader*>(
      sendbuf_ + sizeof(pcap::IPHeader) + ll_header_size_);

  uint16_t src_port_raw = udp_packet.five_tuple().src_port().Raw();
  uint16_t dst_port_raw = udp_packet.five_tuple().dst_port().Raw();

  udph->uh_sport = htons(src_port_raw);
  udph->uh_dport = htons(dst_port_raw);
  udph->uh_sum = 0;

  uint32_t total_len = udp_packet.payload_bytes() + sizeof(pcap::UDPHeader);
  udph->uh_ulen = htons(total_len);

  uint32_t total_len_ip = total_len + sizeof(pcap::IPHeader);
  CHECK(total_len_ip <= std::numeric_limits<uint16_t>::max());
  iph_->ip_len = htons(total_len_ip);
  iph_->ip_sum =
      csum(reinterpret_cast<unsigned short*>(sendbuf_ + ll_header_size_),
           (sizeof(pcap::IPHeader) / 2));

  while (pcap_inject(pcap_, sendbuf_, total_len + sizeof(pcap::IPHeader) +
                                          ll_header_size_) == -1) {
    if (errno != 55) {
      // No buffer space available
      LOG(FATAL) << pcap_geterr(pcap_) << " error " << errno;
    }
  }
}

void PhysicalInterfacePacketHandler::HandlePacket(PacketPtr pkt) {
  if (pkt->five_tuple().ip_proto() == net::kProtoUDP) {
    const UDPPacket* udp_packet = static_cast<const UDPPacket*>(pkt.get());
    SendUDP(*udp_packet);
    return;
  }

  LOG(FATAL) << "Don't know how to handle IP type " << pkt->ip_id();
}

}  // namespace htsim
}  // namespace nc
