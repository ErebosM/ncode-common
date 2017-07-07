#ifndef NCODE_HTSIM_TCP_H
#define NCODE_HTSIM_TCP_H

#include <cstdint>
#include <iostream>
#include <list>
#include <memory>
#include <stdexcept>
#include <vector>

#include "../common.h"
#include "../logging.h"
#include "packet.h"

namespace nc {
namespace htsim {

// Configuration of a TCPSource.
struct TCPSourceConfig {
  // Maximum segment size.
  uint16_t mss = 1500;

  // Maximum congestion window.
  uint32_t maxcwnd = 2000000;

  // How many packets to initially open the congestion window to.
  uint32_t inital_cwnd_size = 4;
};

class TCPSource : public Connection {
 public:
  TCPSource(const std::string& id, const net::FiveTuple& five_tuple,
            const TCPSourceConfig& tcp_source_config, PacketHandler* out,
            EventQueue* event_queue);

  void ReceivePacket(PacketPtr pkt) override;

  void AddData(uint64_t data_bytes) override;

  void Close() override;

  void RtxTimerHook(EventQueueTime now);

  void set_complection_times_callback(
      std::function<void(EventQueueTime, uint64_t)> complectionTimesCallback) {
    complection_times_callback_ = complectionTimesCallback;
  }

  void set_fast_retx_callback(std::function<void(uint64_t)> fastRetxCallback) {
    fast_retx_callback_ = fastRetxCallback;
  }

  void set_retx_timeout_callback(
      std::function<void(uint64_t)> retxTimeoutCallback) {
    retx_timeout_callback_ = retxTimeoutCallback;
  }

 private:
  void UpdateCompletionTime();

  void InflateWindow();

  void RetransmitPacket();

  void SendPackets();

  const uint16_t mss_;
  const uint32_t maxcwnd_;
  EventQueueTime last_sent_time_;

  uint64_t highest_seqno_sent_;
  uint64_t highest_seqno_sent_real_;
  uint32_t cwnd_;
  uint64_t last_acked_;
  uint16_t dupacks_;
  uint32_t ssthresh_;

  uint64_t rtt_, rto_, mdev_;
  uint64_t rtt_avg_, rtt_cum_;
  int sawtooth_;

  uint64_t recoverq_;

  bool in_fast_recovery_;

  // How many bytes there are to be sent.
  uint64_t send_buffer_;

  // How long does it take to add one byte to the buffer.
  EventQueueTime time_to_add_byte_to_buffer_;

  // The sequence of every pkt retx as a fast retransmission is logged here.
  std::function<void(uint64_t)> fast_retx_callback_;

  // The sequence of every pkt retx due to a timeout is logged here.
  std::function<void(uint64_t)> retx_timeout_callback_;

  // Completion times for each flow.
  std::function<void(EventQueueTime, uint64_t)> complection_times_callback_;

  // Time the first packet in the flow is sent.
  EventQueueTime first_sent_time_;

  // Number of times Close() has been called.
  uint64_t close_count_;

  // Size (in pkts) of the initial congestion window.
  uint32_t initial_cwnd_size_;

  DISALLOW_COPY_AND_ASSIGN(TCPSource);
};

class TCPSink : public Connection {
 public:
  TCPSink(const std::string& id, const net::FiveTuple& five_tuple,
          PacketHandler* out_handler, EventQueue* event_queue)
      : Connection(id, five_tuple, out_handler, event_queue),
        cumulative_ack_(0),
        last_seen_incoming_tag_(PacketTag::Max()),
        tag_change_count_(0) {}

  void AddData(uint64_t bytes) override {
    Unused(bytes);
    LOG(FATAL) << "Attempted to add data to TCP sink.";
  }

  void Close() override { LOG(FATAL) << "Attempted to close a TCP sink."; }

  void ReceivePacket(PacketPtr pkt) override;

  void SendAck(EventQueueTime time_sent);

  void set_incoming_tag_change_callback(
      std::function<void(uint32_t)> incomingTagChangeCallback) {
    incoming_tag_change_callback_ = incomingTagChangeCallback;
  }

 private:
  // Resets the sink's state. Will be called when the first packet in the flow
  // is received.
  void Reset();

  uint64_t cumulative_ack_;
  std::list<uint64_t> received_;

  // Each incoming packet can be tagged. This variable stores the tag of the
  // last received packet and can be used to update the metric that counts the
  // number of times tags have been changed. This signifies a change in the path
  // taken by the flow. Set to uint64_t::max if no packet has been seen yet.
  PacketTag last_seen_incoming_tag_;
  uint64_t tag_change_count_;
  std::function<void(uint32_t)> incoming_tag_change_callback_;

  DISALLOW_COPY_AND_ASSIGN(TCPSink);
};

// All TCP flows share the same rtx timer, this reduces the overall number of
// events in the queue, but may cause synchronization in pathological
// situations. Usually there should be very few TCP retx timeouts.
class TCPRtxTimer : public SimComponent, public EventConsumer {
 public:
  TCPRtxTimer(const std::string& id, EventQueueTime scan_period,
              EventQueue* event_queue);

  void RegisterTCPSource(TCPSource* tcp_source);

  void HandleEvent() override;

 private:
  // How often to check if the rtx timer expired.
  const EventQueueTime scan_period_;

  // Sources to check for rtx.
  std::vector<TCPSource*> tcp_sources_;

  DISALLOW_COPY_AND_ASSIGN(TCPRtxTimer);
};

}  // namespace htsim
}  // namespace ncode

#endif
