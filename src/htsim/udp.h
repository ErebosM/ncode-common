#ifndef NCODE_HTSIM_UDP_H
#define NCODE_HTSIM_UDP_H

#include <cstdint>

#include "../common.h"
#include "../logging.h"
#include "../net/net_common.h"
#include "packet.h"

namespace nc {
namespace htsim {

class UDPSource : public Connection {
 public:
  UDPSource(const std::string& id, const net::FiveTuple& five_tuple,
            PacketHandler* out_handler, EventQueue* event_queue);

  void AddData(uint64_t data) override;

  void Close() override { CHECK(false) << "Cannot close a UDP connection"; }

  void ReceivePacket(PacketPtr pkt) override {
    Unused(pkt);
    CHECK(false) << "UDP sources should not receive any packets";
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(UDPSource);
};

class UDPSink : public Connection {
 public:
  UDPSink(const std::string& id, const net::FiveTuple& five_tuple,
          PacketHandler* out_handler, EventQueue* event_queue);

  void AddData(uint64_t data) override {
    Unused(data);
    CHECK(false) << "UDP sinks cannot send data";
  }

  void Close() override { CHECK(false) << "Cannot close a UDP connection"; }

 private:
  void ReceivePacket(PacketPtr pkt) override;

  DISALLOW_COPY_AND_ASSIGN(UDPSink);
};

}  // namespace htsim
}  // namespace nc

#endif
