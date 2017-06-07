#include "network.h"

#include <chrono>
#include <limits>
#include <utility>

#include "../event_queue.h"
#include "../substitute.h"
#include "../logging.h"
#include "tcp.h"
#include "udp.h"

namespace nc {
namespace htsim {

static std::string GetSinkId(const net::FiveTuple& five_tuple) {
  return Substitute("sink_$0_port_$1_to_$2_port_$3_proto_$4",
                    net::IPToStringOrDie(five_tuple.ip_src()),
                    five_tuple.src_port().Raw(),
                    net::IPToStringOrDie(five_tuple.ip_dst()),
                    five_tuple.dst_port().Raw(), five_tuple.ip_proto().Raw());
}

static std::string GetGeneratorId(const net::FiveTuple& five_tuple) {
  return Substitute("generator_$0_port_$1_to_$2_port_$3_proto_$4",
                    net::IPToStringOrDie(five_tuple.ip_src()),
                    five_tuple.src_port().Raw(),
                    net::IPToStringOrDie(five_tuple.ip_dst()),
                    five_tuple.dst_port().Raw(), five_tuple.ip_proto().Raw());
}

const net::DevicePortNumber Device::kLoopbackPortNum =
    net::DevicePortNumber::Max();

void Device::HandlePacket(PacketPtr pkt) {
  if (pkt->size_bytes() == 0) {
    // The packet is an SSCP message.
    uint8_t type = pkt->five_tuple().ip_proto().Raw();
    if (type == SSCPAddOrUpdate::kSSCPAddOrUpdateType) {
      ++stats_.route_updates_seen;

      SSCPAddOrUpdate* add_or_update_message =
          static_cast<SSCPAddOrUpdate*>(pkt.get());
      matcher_.AddRule(add_or_update_message->TakeRule());

      if (add_or_update_message->tx_id() != SSCPMessage::kNoTxId &&
          replies_handler_ != nullptr) {
        auto reply = make_unique<SSCPAck>(
            ip_address_, pkt->five_tuple().ip_src(),
            event_queue_->CurrentTime(), add_or_update_message->tx_id());

        LOG(INFO) << "Will TX ACK " << reply->ToString();
        replies_handler_->HandlePacket(std::move(reply));
      }
    } else if (type == SSCPStatsRequest::kSSCPStatsRequestType) {
      auto reply = make_unique<SSCPStatsReply>(
          ip_address_, pkt->five_tuple().ip_src(), event_queue_->CurrentTime());
      matcher_.PopulateSSCPStats(reply.get());

      CHECK(replies_handler_)
          << "Received stats request, but no output handler";
      replies_handler_->HandlePacket(std::move(reply));
    }

    return;
  }

  const net::FiveTuple& incoming_tuple = pkt->five_tuple();
  net::FiveTuple outgoing_tuple = incoming_tuple.Reverse();
  auto it = connections_.find(outgoing_tuple);
  if (it != connections_.end()) {
    it->second->HandlePacket(std::move(pkt));
    return;
  }

  // Will have to add a new sink for the incoming packets.
  Port* loopback_port = GetLoopbackPort();
  std::string sink_id = GetSinkId(incoming_tuple);

  std::unique_ptr<Connection> new_connection;
  net::IPProto ip_proto = incoming_tuple.ip_proto();
  if (ip_proto == net::kProtoUDP) {
    new_connection = make_unique<UDPSink>(sink_id, outgoing_tuple,
                                          loopback_port, event_queue_);
    LOG(INFO) << "Added UDP sink at " << id() << " for " << outgoing_tuple;
  } else if (ip_proto == net::kProtoTCP) {
    new_connection = make_unique<TCPSink>(sink_id, outgoing_tuple,
                                          loopback_port, event_queue_);
    LOG(ERROR) << pkt->ToString();
    LOG(INFO) << "Added TCP sink at " << id() << " for " << outgoing_tuple;
  } else {
    LOG(FATAL) << "Don't know how to create new connection for IP proto "
               << incoming_tuple.ip_proto().Raw();
  }

  Connection* raw_ptr = new_connection.get();
  connections_.emplace(outgoing_tuple, std::move(new_connection));
  raw_ptr->HandlePacket(std::move(pkt));
}

net::FiveTuple Device::PickSrcPortOrDie(
    const net::FiveTuple& tuple_with_no_src_port) {
  const net::FiveTuple& t = tuple_with_no_src_port;
  for (uint16_t i = 1; i < std::numeric_limits<uint16_t>::max(); ++i) {
    net::FiveTuple return_tuple(t.ip_src(), t.ip_dst(), t.ip_proto(),
                                net::AccessLayerPort(i), t.dst_port());
    if (connections_.find(return_tuple) == connections_.end()) {
      return return_tuple;
    }
  }

  CHECK(false) << "Out of src ports";
  return net::FiveTuple::kDefaultTuple;
}

net::FiveTuple Device::PrepareTuple(net::IPAddress dst_address,
                                    net::AccessLayerPort dst_port, bool tcp) {
  net::FiveTuple tuple(ip_address_, dst_address,
                       tcp ? net::kProtoTCP : net::kProtoUDP,
                       kWildAccessLayerPort, dst_port);
  tuple = PickSrcPortOrDie(tuple);
  return tuple;
}

TCPSource* Device::AddTCPGenerator(net::IPAddress dst_address,
                                   net::AccessLayerPort dst_port, uint16_t mss,
                                   uint32_t maxcwnd) {
  net::FiveTuple tuple = PrepareTuple(dst_address, dst_port, true);
  Port* loopback_port = GetLoopbackPort();
  std::string gen_id = GetGeneratorId(tuple);

  auto new_connection = make_unique<TCPSource>(gen_id, tuple, mss, maxcwnd,
                                               loopback_port, event_queue_);

  CHECK(network_ != nullptr) << "Device not part of a network";
  network_->RegisterTCPSourceWithRetxTimer(new_connection.get());

  TCPSource* raw_ptr = new_connection.get();
  connections_.emplace(tuple, std::move(new_connection));

  LOG(INFO) << Substitute("Added TCP generator at $0 with 5-tuple $1", id_,
                          tuple.ToString());
  return raw_ptr;
}

UDPSource* Device::AddUDPGenerator(net::IPAddress dst_address,
                                   net::AccessLayerPort dst_port) {
  net::FiveTuple tuple = PrepareTuple(dst_address, dst_port, false);
  Port* loopback_port = GetLoopbackPort();
  std::string gen_id = GetGeneratorId(tuple);

  auto new_connection =
      make_unique<UDPSource>(gen_id, tuple, loopback_port, event_queue_);
  UDPSource* raw_ptr = new_connection.get();
  connections_.emplace(tuple, std::move(new_connection));
  return raw_ptr;
}

Device::Device(const std::string& id, net::IPAddress ip_address,
               EventQueue* event_queue)
    : SimComponent(id, event_queue),
      ip_address_(ip_address),
      matcher_("matcher_for_" + id),
      network_(nullptr),
      replies_handler_(nullptr),
      sample_handler_(nullptr),
      internal_external_observer_(nullptr),
      external_internal_observer_(nullptr),
      sample_prob_(0),
      distribution_(0, 1.0),
      die_on_fail_to_match_(false) {}

Port::Port(net::DevicePortNumber number, Device* device)
    : number_(number),
      parent_device_(device),
      out_handler_(nullptr),
      internal_(false) {}

void Device::EnableSampling(PacketHandler* sample_hander, size_t n) {
  sample_handler_ = sample_hander;
  if (n != 0) {
    sample_prob_ = 1.0 / n;
  } else {
    sample_prob_ = 0.0;
  }
}

void Port::HandlePacket(PacketPtr pkt) {
  parent_device_->HandlePacketFromPort(this, std::move(pkt));
}

void Port::SendPacketOut(PacketPtr pkt) {
  out_handler_->HandlePacket(std::move(pkt));
}

void Port::Connect(PacketHandler* out_handler) {
  if (out_handler == out_handler_) {
    return;
  }

  CHECK(out_handler_ == nullptr) << "Tried to connect port " << number_.Raw()
                                 << " twice on " << parent_device_->id();
  out_handler_ = out_handler;
}

void Port::Reconnect(PacketHandler* out_handler) {
  CHECK(out_handler_ != nullptr) << "Tried to reconnect an unconnected port";
  out_handler_ = out_handler;
}

Port* Device::FindOrCreatePort(net::DevicePortNumber port_num) {
  auto it = port_number_to_port_.find(port_num);
  if (it != port_number_to_port_.end()) {
    return it->second.get();
  }

  std::unique_ptr<Port> port_ptr =
      std::unique_ptr<Port>(new Port(port_num, this));
  Port* port_naked_ptr = port_ptr.get();

  port_number_to_port_.emplace(port_num, std::move(port_ptr));
  return port_naked_ptr;
}

Port* Device::NextAvailablePort() {
  for (uint32_t i = 1; i < net::DevicePortNumber::Max().Raw(); ++i) {
    auto port_number = net::DevicePortNumber(i);
    if (ContainsKey(port_number_to_port_, port_number)) {
      continue;
    }

    std::unique_ptr<Port> port_ptr =
        std::unique_ptr<Port>(new Port(port_number, this));
    Port* port_naked_ptr = port_ptr.get();
    port_number_to_port_.emplace(port_number, std::move(port_ptr));
    return port_naked_ptr;
  }

  LOG(FATAL) << "Out of port numbers";
  return nullptr;
}

void Device::HandlePacketFromPort(Port* input_port, PacketPtr pkt) {
  uint32_t pkt_size = pkt->size_bytes();
  stats_.packets_seen += 1;
  stats_.bytes_seen += pkt_size;

  if (pkt->five_tuple().ip_dst() == ip_address()) {
    stats_.packets_for_localhost += 1;
    stats_.bytes_for_localhost += pkt_size;
    HandlePacket(std::move(pkt));
    return;
  }

  const MatchRuleAction* action =
      matcher_.MatchOrNull(*pkt, input_port->number());
  if (action == nullptr) {
    stats_.packets_failed_to_match += 1;
    stats_.bytes_failed_to_match += pkt_size;

    // Packet will be dropped.
    if (die_on_fail_to_match_) {
      LOG(FATAL) << "Dropping packet " << pkt->ToString() << " at " << id();
    }

    return;
  }

  if (action->tag() != kNullPacketTag) {
    pkt->set_tag(action->tag());
  }

  if (!pkt->preferential_drop() && action->preferential_drop()) {
    pkt->set_preferential_drop(true);
  }

  if (!pkt->DecrementTTL()) {
    LOG(FATAL) << "TTL exceeded at " << id() << " " << pkt->ToString();
  }

  const auto& it = port_number_to_port_.find(action->output_port());
  CHECK(it != port_number_to_port_.end())
      << "Unable to find port " << Substitute("$0", action->output_port().Raw())
      << " at " << id();

  if (sample_prob_ != 0 && action->sample()) {
    if (distribution_(generator_) <= sample_prob_) {
      PacketPtr new_pkt = pkt->Duplicate();
      sample_handler_->HandlePacket(std::move(new_pkt));
    }
  }

  Port* output_port = it->second.get();
  if (input_port->internal_ && !output_port->internal_ &&
      internal_external_observer_) {
    internal_external_observer_->ObservePacket(*pkt);
  } else if (!input_port->internal_ && output_port->internal_ &&
             external_internal_observer_) {
    external_internal_observer_->ObservePacket(*pkt);
  }

  output_port->SendPacketOut(std::move(pkt));
}

void Device::AddInternalExternalObserver(PacketObserver* observer) {
  CHECK(internal_external_observer_ == nullptr ||
        internal_external_observer_ == observer);
  CHECK(observer != nullptr);
  internal_external_observer_ = observer;
}

void Device::AddExternalInternalObserver(PacketObserver* observer) {
  CHECK(external_internal_observer_ == nullptr ||
        external_internal_observer_ == observer);
  CHECK(observer != nullptr);
  external_internal_observer_ = observer;
}

Network::Network(EventQueueTime tcp_retx_scan_period, EventQueue* event_queue)
    : SimComponent("network", event_queue) {
  tcp_retx_timer_ = make_unique<TCPRtxTimer>("tcp_retx_timer",
                                             tcp_retx_scan_period, event_queue);
}

void Network::AddDevice(Device* device) {
  id_to_device_.emplace(device->id(), device);
  device->set_network(this);
}

void Network::AddLink(Queue* queue, Pipe* pipe, bool internal) {
  const net::GraphLink* link = pipe->graph_link();
  CHECK(link->src() != link->dst()) << "Link source same as destination";

  Device& src = FindDeviceOrDie(link->src_node()->id());
  Device& dst = FindDeviceOrDie(link->dst_node()->id());

  Port* src_port = src.FindOrCreatePort(link->src_port());
  src_port->set_internal(internal);

  Port* dst_port = dst.FindOrCreatePort(link->dst_port());
  dst_port->set_internal(internal);

  // Connect the queue to the pipe and the source port to the queue
  src_port->Connect(queue);
  queue->Connect(pipe);
  pipe->Connect(dst_port);

  LOG(INFO) << Substitute("Added queue $0:$1 -> $2:$3.", link->src_node()->id(),
                          link->src_port().Raw(), link->dst_node()->id(),
                          link->dst_port().Raw());
  LOG(INFO) << Substitute("Added pipe $0:$1 -> $2:$3.", link->src_node()->id(),
                          link->src_port().Raw(), link->dst_node()->id(),
                          link->dst_port().Raw());
}

void Network::RegisterTCPSourceWithRetxTimer(TCPSource* src) {
  tcp_retx_timer_->RegisterTCPSource(src);
}

}  // namespace htsim
}  // namespace ncode
