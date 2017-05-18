#include "pcap.h"

#include <netinet/in.h>
#include <pcap/pcap.h>
#include <poll.h>
#include <mutex>

#include "../common.h"
#include "../logging.h"
#include "../substitute.h"

namespace nc {
namespace pcap {

void PacketHandler::HandleTCP(Timestamp timestamp, const IPHeader& ip_header,
                              const TCPHeader& tcp_header,
                              uint16_t payload_len) {
  Unused(timestamp);
  Unused(ip_header);
  Unused(tcp_header);
  Unused(payload_len);
}

void PacketHandler::HandleUDP(Timestamp timestamp, const IPHeader& ip_header,
                              const UDPHeader& udp_header,
                              uint16_t payload_len) {
  Unused(timestamp);
  Unused(ip_header);
  Unused(udp_header);
  Unused(payload_len);
}
void PacketHandler::HandleICMP(Timestamp timestamp, const IPHeader& ip_header,
                               const ICMPHeader& icmp_header,
                               uint16_t payload_len) {
  Unused(timestamp);
  Unused(ip_header);
  Unused(icmp_header);
  Unused(payload_len);
}
void PacketHandler::HandleUnknownIP(Timestamp timestamp,
                                    const IPHeader& ip_header,
                                    uint16_t payload_len) {
  Unused(timestamp);
  Unused(ip_header);
  Unused(payload_len);
}

bool VerifyTCPHeader(const IPHeader& ip_header, const TCPHeader& tcp_header,
                     uint16_t* payload_size) {
  if (tcp_header.th_off < 5) {
    LOG(INFO) << Substitute("TCP header offset too short: $0 words",
                            tcp_header.th_off);

    return false;
  }

  uint32_t headers_size = (ip_header.ip_hl + tcp_header.th_off) * 4;
  uint16_t ip_len = ntohs(ip_header.ip_len);
  if (headers_size > ip_len) {
    LOG(INFO) << Substitute("Wrong TCP header size: ip_len $0, headers_size $1",
                            ip_len, headers_size);

    return false;
  }

  *payload_size = ip_len - headers_size;
  return true;
}

bool VerifyUDPHeader(const IPHeader& ip_header, uint16_t* payload_size) {
  uint32_t headers_size = ip_header.ip_hl * 4 + pcap::kSizeUDP;
  uint16_t ip_len = ntohs(ip_header.ip_len);
  if (headers_size > ip_len) {
    LOG(INFO) << Substitute("Wrong UDP header size: ip_len $0, headers_size $1",
                            ip_len, headers_size);

    return false;
  }

  *payload_size = ip_len - headers_size;
  return true;
}

bool VerifyICMPHeader(const IPHeader& ip_header, uint16_t* payload_size) {
  uint32_t headers_size = ip_header.ip_hl * 4 + pcap::kSizeICMP;
  uint16_t ip_len = ntohs(ip_header.ip_len);
  if (headers_size > ip_len) {
    LOG(INFO) << Substitute(
        "Wrong ICMP header size estimate: ip_len $0, headers_size $1", ip_len,
        headers_size);

    return false;
  }

  *payload_size = ip_len - headers_size;
  return true;
}

// Called to handle a single packet. Will dispatch it to HandleTcp or
// HandleUdp.This is in a free function because the pcap library expects an
// unbound function pointer
static void HandlePkt(u_char* d, const struct pcap_pkthdr* header,
                      const u_char* packet) {
  using namespace std::chrono;
  PcapBase* data = reinterpret_cast<PcapBase*>(d);

  Timestamp timestamp;
  ExternalTimestampProvider* external_timestamp_provider =
      data->timestamp_provider();
  if (external_timestamp_provider) {
    timestamp = external_timestamp_provider->NextTimestamp();
  } else {
    // Timestamp is assumed to have microsecond precision.
    timestamp = seconds(header->ts.tv_sec) + microseconds(header->ts.tv_usec);
  }

  int datalink = data->datalink();
  size_t offset;
  // Have to figure out what the offset is.
  switch (datalink) {
    case DLT_EN10MB: {
      if (header->caplen < 14) {
        return;
      }
      // Only handle IP and 802.1Q VLAN tagged packets
      if (packet[12] == 8 && packet[13] == 0) {
        // Regular ethernet
        offset = kSizeEthernet;
      } else if (packet[12] == 0x81 && packet[13] == 0) {
        // Skip 802.1Q VLAN and priority information
        offset = kSizeEthernetDotOneQ;
      } else {
        LOG(ERROR) << "Non-IP frame";
        return;
      }
      break;
    }

    case DLT_RAW: {
      offset = 0;
      break;
    }

    case DLT_NULL: {
      offset = 4;
      break;
    }

    case DLT_LINUX_SLL: {
      offset = 16;
      break;
    }

    default: {
      LOG(FATAL) << "Unknown datalink " << datalink;
      return;
    }
  }

  const IPHeader* ip_header =
      reinterpret_cast<const IPHeader*>(packet + offset);

  uint16_t off = ntohs(ip_header->ip_off);
  if (off && !(off & IP_DF)) {
    // Don't know how to deal with fragments yet.
    return;
  }

  if (off & IP_RF) {
    // Reserved bit set -- rfc3541
    LOG(INFO) << "Packet with evil bit";
    return;
  }

  size_t size_ip = ip_header->ip_hl * 4;
  if (size_ip < 20) {
    LOG(INFO) << Substitute(
        "Invalid IP header length: $0 bytes, pcap header len: $1", size_ip,
        header->len);

    return;
  }

  PacketHandler* handler = data->handler();
  uint16_t payload_len;
  switch (ip_header->ip_p) {
    case IPPROTO_TCP: {
      const TCPHeader* tcp_header =
          reinterpret_cast<const TCPHeader*>(packet + offset + size_ip);
      if (!VerifyTCPHeader(*ip_header, *tcp_header, &payload_len)) {
        return;
      }

      handler->HandleTCP(timestamp, *ip_header, *tcp_header, payload_len);
      break;
    }
    case IPPROTO_UDP: {
      const UDPHeader* udp_header =
          reinterpret_cast<const UDPHeader*>(packet + offset + size_ip);
      if (!VerifyUDPHeader(*ip_header, &payload_len)) {
        return;
      }

      handler->HandleUDP(timestamp, *ip_header, *udp_header, payload_len);
      break;
    }
    case IPPROTO_ICMP: {
      const ICMPHeader* icmp_header =
          reinterpret_cast<const ICMPHeader*>(packet + offset + size_ip);
      if (!VerifyICMPHeader(*ip_header, &payload_len)) {
        return;
      }

      handler->HandleICMP(timestamp, *ip_header, *icmp_header, payload_len);
      break;
    }
    default: {
      // This will be off, but we don't know what the protocol is.
      payload_len = ntohs(ip_header->ip_len) - ip_header->ip_hl * 4;
      handler->HandleUnknownIP(timestamp, *ip_header, payload_len);
    }
  }
}

int PcapBase::datalink() const {
  CHECK(handle_ != nullptr);
  return datalink_;
}

void PcapBase::set_timestamp_provider(
    ExternalTimestampProvider* timestamp_provider) {
  timestamp_provider_ = timestamp_provider;
}

ExternalTimestampProvider* PcapBase::timestamp_provider() const {
  return timestamp_provider_;
}

// pcap_compile is inherently thread unsafe.
static std::mutex pcap_compile_mutex_;

void PcapBase::InstallBPF(bpf_u_int32 net) {
  std::lock_guard<std::mutex> lock(pcap_compile_mutex_);

  struct bpf_program fp;
  if (pcap_compile(handle_, &fp, bpf_filter_.c_str(), 0, net) == -1) {
    pcap_freecode(&fp);
    LOG(FATAL) << "Could not parse filter " << bpf_filter_
               << ", pcap said: " << std::string(pcap_geterr(handle_));
  }

  if (pcap_setfilter(handle_, &fp) == -1) {
    pcap_freecode(&fp);
    LOG(FATAL) << "Could not install filter " << bpf_filter_
               << ", pcap said: " << std::string(pcap_geterr(handle_));
  }

  pcap_freecode(&fp);
}

void OnlinePcap::PcapOpen(const std::string& source) {
  char errbuf[PCAP_ERRBUF_SIZE];
  bpf_u_int32 mask = 0;
  bpf_u_int32 net = 0;

  // A c-style string for the source.
  const char* source_c = source.c_str();
  handle_ = pcap_open_live(source_c, snapshot_len_, 1, 1000, errbuf);
  CHECK(handle_ != nullptr) << "Could not open source " << source
                            << ", pcap said: " << std::string(errbuf);

  datalink_ = pcap_datalink(handle_);
  CHECK(pcap_lookupnet(source_c, &net, &mask, errbuf) != -1)
      << "Could not get netmask for device " << source
      << ", pcap said: " << std::string(errbuf);
  CHECK(pcap_setnonblock(handle_, 1, errbuf) != -1)
      << "Could not set to non-blocking device " << source
      << ", pcap said: " << std::string(errbuf);
  InstallBPF(net);
}

void OnlinePcap::Run() {
  int poll_result;
  pollfd pfd;
  LOG(INFO) << "Will start listening on " << source_;

  pfd.fd = pcap_fileno(handle_);
  pfd.events = POLLIN;

  while (true) {
    poll_result = poll(&pfd, 1, 1000);
    CHECK(poll_result != -1) << "Bad poll on pcap fd";

    switch (poll_result) {
      case 0:  // timeout
        break;

      default:  // packet
        pcap_dispatch(handle_, -1, HandlePkt, reinterpret_cast<u_char*>(this));
    }
  }
}

void OfflinePcap::Run() {
  // Will get a series of sources from the source provider and process
  // every one of them in order.
  while (true) {
    std::string next_source = source_->NextSource();
    if (next_source.empty()) {
      break;
    }

    PcapOpen(next_source);
    LOG(INFO) << "Will start reading from " << next_source;
    int ret =
        pcap_loop(handle_, -1, HandlePkt, reinterpret_cast<u_char*>(this));
    if (ret == 0) {
      LOG(INFO) << "Done reading from " << next_source;
    } else {
      LOG(ERROR) << "Error while reading from " << next_source
                 << ", pcap said: " << std::string(pcap_geterr(handle_));
    }

    pcap_close(handle_);
    handle_ = nullptr;
  }
}

bool OfflinePcap::NextPacket() {
  if (handle_ == nullptr) {
    std::string next_source = source_->NextSource();
    if (next_source.empty()) {
      return false;
    }

    PcapOpen(next_source);
    LOG(INFO) << "Will start reading from " << next_source;
  }

  struct pcap_pkthdr* header_ptr;
  const u_char* packet_ptr;

  int ret = pcap_next_ex(handle_, &header_ptr, &packet_ptr);
  CHECK(ret != -1) << "Error while reading, pcap said: "
                   << std::string(pcap_geterr(handle_));
  if (ret == -2) {
    LOG(INFO) << "Done reading";
    pcap_close(handle_);
    handle_ = nullptr;
    return NextPacket();
  }

  HandlePkt(reinterpret_cast<u_char*>(this), header_ptr, packet_ptr);
  return true;
}

void OfflinePcap::PcapOpen(const std::string& source) {
  char errbuf[PCAP_ERRBUF_SIZE];
  FILE* fp = fopen(source.c_str(), "rb");
  CHECK(fp != nullptr) << "Unable to open file";

  handle_ = pcap_fopen_offline(fp, errbuf);
  CHECK(handle_ != nullptr) << "Could not open source " << source
                            << ", pcap said: " << std::string(errbuf);
  datalink_ = pcap_datalink(handle_);
  InstallBPF(PCAP_NETMASK_UNKNOWN);
}

DefaultOfflineSourceProvider::DefaultOfflineSourceProvider(
    const std::string& filename)
    : next_index_(0), filenames_({filename}) {}

DefaultOfflineSourceProvider::DefaultOfflineSourceProvider(
    const std::vector<std::string>& filenames)
    : next_index_(0), filenames_(filenames) {}

std::string DefaultOfflineSourceProvider::NextSource() {
  if (next_index_ == filenames_.size()) {
    return "";
  }

  return filenames_[next_index_++];
}

}  // namespace pcap
}  // namespace nc
