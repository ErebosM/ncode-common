#ifndef HT2SIM_NETWORK_H
#define HT2SIM_NETWORK_H

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "../common.h"
#include "../map_util.h"
#include "../net/net_common.h"
#include "htsim.h"
#include "match.h"
#include "packet.h"
#include "queue.h"
#include "tcp.h"

namespace nc {
namespace htsim {

// Forward references
class DeviceInterface;
class NetworkSim;

class Port : public PacketHandler {
 public:
  // Handles packet coming into the port from the outside, not from the parent
  // device.
  void HandlePacket(PacketPtr pkt) override;

  // After this call packets that are passed to this port via SendPacketOut will
  // flow via the given handler.
  void Connect(PacketHandler* out_handler);

  // Similar to Connect, but changes the handler instead of setting it. Calling
  // connect twice would result in a crash. Calling this before having called
  // Connect will also crash.
  void Reconnect(PacketHandler* out_handler);

  // Sends a packet out this port to the handler it is connected to.
  void SendPacketOut(PacketPtr pkt);

  // This port's number.
  net::DevicePortNumber number() const { return number_; }

  // Changes this port's internal/external status.
  void set_internal(bool internal) { internal_ = internal; }

  bool internal() const { return internal_; }

 private:
  Port(net::DevicePortNumber number, DeviceInterface* device);

  // This port's number.
  const net::DevicePortNumber number_;

  // The device this port is attached to. This is a naked pointer since the
  // lifetime of the Port is tied to that of the device.
  DeviceInterface* parent_device_;

  // The handler that packets will go to upon exiting via this port.
  PacketHandler* out_handler_;

  // If a port is marked as internal the transitions internal->external and
  // external->internal can be monitored on the device.
  bool internal_;

  friend class DeviceInterface;
  DISALLOW_COPY_AND_ASSIGN(Port);
};

class TCPRtxTimer;
class TCPSource;
class UDPSource;
class Network;

struct DeviceStats {
  uint32_t num_rules = 0;
  uint64_t packets_seen = 0;
  uint64_t packets_failed_to_match = 0;
  uint64_t bytes_seen = 0;
  uint64_t bytes_failed_to_match = 0;
  uint64_t packets_for_localhost = 0;
  uint64_t bytes_for_localhost = 0;
  uint64_t route_updates_seen = 0;
  std::map<net::FiveTuple, ConnectionStats> connection_stats;
};

class DeviceInterface : public SimComponent, public PacketHandler {
 public:
  DeviceInterface(const std::string& id, nc::net::IPAddress ip_address,
                  EventQueue* event_queue)
      : SimComponent(id, event_queue),
        ip_address_(ip_address),
        network_(nullptr),
        replies_handler_(nullptr) {}

  virtual ~DeviceInterface() {}

  // Finds or adds a port to this device. The returned pointer is non-owning.
  Port* FindOrCreatePort(net::DevicePortNumber port_num);

  // Creates a new port on the device with a unique for the device port number.
  // This function will always return a new port.
  Port* NextAvailablePort();

  // Sets this device's Network. Will be called automatically when the device is
  // added to the network.
  void set_network(Network* network) { network_ = network; }

  PacketHandler* tx_replies_handler() { return replies_handler_; }
  void set_tx_replies_handler(PacketHandler* tx_replies_handler) {
    replies_handler_ = tx_replies_handler;
  }

  // Called when one of this device's ports receives a packet. This is not the
  // same as HandlePacket, which is called when a packet arrives that is
  // destined for the device itself.
  virtual void HandlePacketFromPort(Port* input_port, PacketPtr pkt) = 0;

  // Returns this device's ip address.
  net::IPAddress ip_address() const { return ip_address_; }

 protected:
  // This device's address.
  net::IPAddress ip_address_;

  // The parent network instance.
  Network* network_;

  // A map from port number to the port object.
  std::map<net::DevicePortNumber, std::unique_ptr<Port>> port_number_to_port_;

  // Replies to update messages and stats are sent out via this handler (instead
  // of using the routing table to find a destination etc.). If this is  nullptr
  // no replies are sent.
  PacketHandler* replies_handler_;
};

// A device in the network. Each device can perform forwarding based on a set of
// rules.
class Device : public DeviceInterface {
 public:
  static const net::DevicePortNumber kLoopbackPortNum;

  Device(const std::string& id, net::IPAddress ip_address,
         EventQueue* event_queue);

  void HandlePacket(PacketPtr pkt) override;

  // Returns the status of this device.
  DeviceStats GetStats() const {
    DeviceStats return_stats = stats_;
    for (const auto& tuple_and_connection : connections_) {
      const net::FiveTuple& tuple = tuple_and_connection.first;
      const Connection* connection = tuple_and_connection.second.get();
      return_stats.connection_stats[tuple] = connection->GetStats();
    }

    return_stats.num_rules = matcher_.NumRules();
    return return_stats;
  }

  void HandlePacketFromPort(Port* input_port, PacketPtr pkt) override;

  void HandlePacketWithAction(Port* input_port, PacketPtr pkt,
                              const MatchRuleAction* action);

  virtual void PostProcessStats(const nc::htsim::SSCPStatsRequest& request,
                                nc::htsim::SSCPStatsReply* reply) {
    nc::Unused(request);
    nc::Unused(reply);
  }

  // Returns a pointer to the loopback port. Traffic sent to this port will end
  // up in the device's stack and all traffic destined for this device will be
  // delivered to the loopback port.
  Port* GetLoopbackPort() { return FindOrCreatePort(kLoopbackPortNum); }

  // Constructs a new TCP source and returns a non-owning pointer to it.
  TCPSource* AddTCPGenerator(net::IPAddress dst_address,
                             net::AccessLayerPort dst_port, uint16_t mss,
                             uint32_t maxcwnd);

  // Constructs a new UDP source and returns a non-owning pointer to it.
  UDPSource* AddUDPGenerator(net::IPAddress dst_address,
                             net::AccessLayerPort dst_port);

  // Adds observers that will observe packets that are transfered to/from
  // internal/external ports.
  void AddInternalExternalObserver(PacketObserver* observer);
  void AddExternalInternalObserver(PacketObserver* observer);

  void set_die_on_fail_to_match(bool die_on_fail_to_match) {
    die_on_fail_to_match_ = die_on_fail_to_match;
  }

 private:
  // Prepares a 5-tuple for a new connection originating from this device.
  net::FiveTuple PrepareTuple(net::IPAddress dst_address,
                              net::AccessLayerPort dst_port, bool tcp);

  // Picks a source port or throws an exception.
  net::FiveTuple PickSrcPortOrDie(const net::FiveTuple& tuple_with_no_src_port);

  // Forwarding rules go here.
  Matcher matcher_;

  // Information about the device.
  DeviceStats stats_;

  // Map from 5-tuples of incoming packets to connections that can accept the
  // packets.
  std::unordered_map<net::FiveTuple, std::unique_ptr<Connection>,
                     net::FiveTupleHasher> connections_;

  // All packets that move from internal to external ports are observed by this
  // observer (if not null).
  PacketObserver* internal_external_observer_;

  // All packets that move from external to internal ports are observed by this
  // observer (if not null).
  PacketObserver* external_internal_observer_;

  // Dies if a packet fails to match an entry in the routing table.
  bool die_on_fail_to_match_;

  DISALLOW_COPY_AND_ASSIGN(Device);
};

class Network : public SimComponent {
 public:
  Network(EventQueueTime tcp_retx_scan_period, EventQueue* event_queue);

  // Adds a device.
  void AddDevice(DeviceInterface* device);

  // Adds a link (unidirectional). The link is a queue that is connected to a
  // pipe. The source / dst of the pipe should already be present.
  void AddLink(Queue* queue, Pipe* pipe, const std::string& src,
               const std::string& dst, nc::net::DevicePortNumber src_port,
               nc::net::DevicePortNumber dst_port, bool internal = false);

  // Adds a TCP source to the common retx timer.
  void RegisterTCPSourceWithRetxTimer(TCPSource* src);

 private:
  // Finds a single device or dies.
  DeviceInterface& FindDeviceOrDie(const std::string& id) {
    return *FindOrDie(id_to_device_, id);
  }

  // Network components
  std::map<std::string, DeviceInterface*> id_to_device_;

  // All TCP connections in the network will share the same retx timer.
  std::unique_ptr<TCPRtxTimer> tcp_retx_timer_;

  DISALLOW_COPY_AND_ASSIGN(Network);
};

}  // namespace htsim
}  // namespace ncode

#endif
