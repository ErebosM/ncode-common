// Structs from netinet. They reside in their own namespace to avoid confusion.

#ifndef NCODE_PCAP_H
#define NCODE_PCAP_H

#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <pcap/pcap.h>
#include <stddef.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace nc {
namespace pcap {

// Ethernet headers are always exactly 14 bytes, except if they are .1q
// encapsulated when they are 18.
static constexpr size_t kSizeEthernet = 14;
static constexpr size_t kSizeEthernetDotOneQ = 18;

// Ethernet addresses are 6 bytes
static constexpr size_t kEthernetAddressLen = 6;

// UDP header size is always constant
static constexpr size_t kSizeUDP = 6;

// ICMP minimum length
static constexpr size_t kSizeICMP = ICMP_MINLEN;

typedef ip IPHeader;
struct TCPHeader {
  static constexpr unsigned char kFinFlag = 0x01;
  static constexpr unsigned char kSynFlag = 0x02;
  static constexpr unsigned char kRstFlag = 0x04;
  static constexpr unsigned char kPushFlag = 0x08;
  static constexpr unsigned char kAckFlag = 0x10;

  uint16_t th_sport; /* source port */
  uint16_t th_dport; /* destination port */
  uint32_t th_seq;   /* sequence number */
  uint32_t th_ack;   /* acknowledgment number */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  unsigned int th_x2 : 4, /* (unused) */
      th_off : 4;         /* data offset */
#endif
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  unsigned int th_off : 4, /* data offset */
      th_x2 : 4;           /* (unused) */
#endif
  unsigned char th_flags;

  uint16_t th_win; /* window */
  uint16_t th_sum; /* checksum */
  uint16_t th_urp; /* urgent pointer */
};

struct UDPHeader {
  uint16_t uh_sport; /* source port */
  uint16_t uh_dport; /* destination port */
  uint16_t uh_ulen;  /* udp length */
  uint16_t uh_sum;   /* udp checksum */
};

typedef icmp ICMPHeader;

// Performs basic checks on the TCP header of a packet.
bool VerifyTCPHeader(const IPHeader& ip_header, const TCPHeader& tcp_header,
                     uint16_t* payload_size);

// Performs basic checks on the UDP header of a packet.
bool VerifyUDPHeader(const IPHeader& ip_header, uint16_t* payload_size);

// Performs basic checks on the ICMP header of a packet.
bool VerifyICMPHeader(const IPHeader& ip_header, uint16_t* payload_size);

// All timestamps are in nanoseconds.
using Timestamp = std::chrono::nanoseconds;

// An interface for a class that knows how to handle incoming packets. Only the
// headers are included.
class PacketHandler {
 public:
  virtual ~PacketHandler() {}

  virtual void HandleTCP(Timestamp timestamp, const IPHeader& ip_header,
                         const TCPHeader& tcp_header, uint16_t payload_len);
  virtual void HandleUDP(Timestamp timestamp, const IPHeader& ip_header,
                         const UDPHeader& udp_header, uint16_t payload_len);
  virtual void HandleICMP(Timestamp timestamp, const IPHeader& ip_header,
                          const ICMPHeader& icmp_header, uint16_t payload_len);
  virtual void HandleUnknownIP(Timestamp timestamp, const IPHeader& ip_header,
                               uint16_t payload_len);
};

// A class that knows how to provide timetamps.
class ExternalTimestampProvider {
 public:
  virtual ~ExternalTimestampProvider() {}

  // Returns the next timestamp. Will be called once per packet, in the order
  // packets are seen by pcap's handle.
  virtual Timestamp NextTimestamp() = 0;
};

// A class that provides source files for reading.
class OfflineSourceProvider {
 public:
  virtual ~OfflineSourceProvider() {}

  // Returns the next source to be read from, or an empty string if no more
  // sources should be processed.
  virtual std::string NextSource() = 0;
};

// An OfflineSourceProvider that reads from one or many .pcap files.
class DefaultOfflineSourceProvider : public OfflineSourceProvider {
 public:
  DefaultOfflineSourceProvider(const std::string& filename);

  DefaultOfflineSourceProvider(const std::vector<std::string>& filenames);

  std::string NextSource() override;

 private:
  size_t next_index_;
  std::vector<std::string> filenames_;
};

class PcapBase {
 public:
  static constexpr const char* kDefaultFilter = "ip";

  virtual ~PcapBase() {}

  // The BPF filter that will be used during capture.
  void set_bfp_filter(const std::string& filter) { bpf_filter_ = filter; }
  const std::string& bpf_filter() const { return bpf_filter_; }

  // If the timestamp provider is set the timestamps of the packets will be
  // ignored and the provider will be used to produce a timestamp for each
  // packet. The pointer is non-owning.
  void set_timestamp_provider(ExternalTimestampProvider* timestamp_provider);
  ExternalTimestampProvider* timestamp_provider() const;

  // The packet handler.
  PacketHandler* handler() const { return handler_; }

  // Runs an online/offline trace and feeds all packets to the given handler.
  // The pointer is non-owning.
  virtual void Run() = 0;

  // The datalink of the current trace. Will crash if no trace is being
  // performed.
  int datalink() const;

 protected:
  PcapBase(PacketHandler* handler)
      : handle_(nullptr),
        datalink_(0),
        bpf_filter_(kDefaultFilter),
        timestamp_provider_(nullptr),
        handler_(handler) {}

  // Opens and constructs a new pcap handle.
  virtual void PcapOpen(const std::string& source) = 0;

  // Attempts to install the BPF filter to the pcap handle.
  void InstallBPF(bpf_u_int32 net);

  // The pcap handle and the datalink type.
  pcap_t* handle_;
  int datalink_;

 private:
  std::string bpf_filter_;
  ExternalTimestampProvider* timestamp_provider_;
  PacketHandler* handler_;
};

class OnlinePcap : public PcapBase {
 public:
  OnlinePcap(const std::string& online_source, size_t snapshot_len,
             PacketHandler* handler)
      : PcapBase(handler),
        source_(online_source),
        snapshot_len_(snapshot_len) {}

  const std::string& source() { return source_; }
  size_t snapshot_len() { return snapshot_len_; }

  void Run() override;

 private:
  void PcapOpen(const std::string& source) override;

  std::string source_;
  size_t snapshot_len_;
};

class OfflinePcap : public PcapBase {
 public:
  OfflinePcap(std::unique_ptr<OfflineSourceProvider> source,
              PacketHandler* handler)
      : PcapBase(handler), source_(std::move(source)) {}

  void Run() override;

  // Processes the next packet from the trace. Returns true on success.
  bool NextPacket();

 private:
  void PcapOpen(const std::string& source) override;

  std::unique_ptr<OfflineSourceProvider> source_;
};

}  // namespace pcap
}  // namespace nc

#endif /* NCODE_PCAP_H */
