#include "tcp.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <fstream>
#include <limits>
#include <string>
#include <type_traits>

#include "../substitute.h"

namespace nc {
namespace htsim {

using namespace std::chrono;

// Size of an ACK packet.
static constexpr size_t kAckSize = 40;

TCPSource::TCPSource(const std::string& id, const net::FiveTuple& five_tuple,
                     uint16_t mss, uint32_t maxcwnd, PacketHandler* out,
                     EventQueue* event_queue)
    : Connection(id, five_tuple, out, event_queue),
      mss_(mss),
      maxcwnd_(maxcwnd),
      close_count_(0) {
  Close();
}

void TCPSource::Close() {
  last_sent_time_ = EventQueueTime::ZeroTime();
  first_sent_time_ = EventQueueTime::MaxTime();
  highest_seqno_sent_ = 0;
  highest_seqno_sent_real_ = 0;
  cwnd_ = 0;
  last_acked_ = 0;
  dupacks_ = 0;
  ssthresh_ = 0xffffffff;
  rtt_ = 0;
  mdev_ = 0;
  rtt_avg_ = 0;
  rtt_cum_ = 0;
  sawtooth_ = 0;
  recoverq_ = 0;
  in_fast_recovery_ = false;
  send_buffer_ = 0;
  rto_ = event_queue_->ToTime(seconds(1)).Raw();
  ++close_count_;
}

void TCPSource::UpdateCompletionTime() {
  EventQueueTime completion_time =
      event_queue_->CurrentTime() - first_sent_time_;
  if (complection_times_callback_) {
    complection_times_callback_(completion_time, close_count_);
  }
}

void TCPSource::ReceivePacket(PacketPtr pkt) {
  // The packet must be a TCP ack. We only know how to handle ACKs (all flows
  // are unidirectional).
  const TCPPacket* ack_packet = static_cast<const TCPPacket*>(pkt.get());

  uint64_t seqno = ack_packet->sequence().Raw();
  EventQueueTime time_ack_sent = ack_packet->time_sent();
  if (time_ack_sent < first_sent_time_) {
    // Ignore the ACK.
    return;
  }

  if (seqno < last_acked_) {
    // Treat it as a dupack.
    seqno = last_acked_;
  }

  // Compute RTT
  int64_t m = (event_queue_->CurrentTime() - time_ack_sent).Raw();
  if (m != 0) {
    if (rtt_ > 0) {
      m -= (rtt_ >> 3);
      rtt_ += m;

      if (m < 0) {
        m = -m;
        m -= mdev_ >> 2;
        if (m > 0) {
          m >>= 3;
        }
      } else {
        m -= mdev_ >> 2;
      }

      mdev_ += m;
    } else {
      rtt_ = m << 3;
      mdev_ = m << 1;
    }
  }

  rto_ = (rtt_ >> 3) + mdev_;

  // In Linux the RTO is at least Hz/5 (200ms).
  EventQueueTime min_rtt = event_queue_->ToTime(milliseconds(200));
  if (rto_ < min_rtt.Raw()) {
    rto_ = min_rtt.Raw();
  }

  uint64_t t1 = event_queue_->ToTime(seconds(5)).Raw();
  uint64_t t2 = event_queue_->ToTime(seconds(2)).Raw();
  if (rto_ > t1) {
    rto_ = t2;
  }

  if (seqno > last_acked_) {  // a brand new ack
    // Best behavior: proper ack of a new packet, when we were expecting it
    if (!in_fast_recovery_) {
      last_acked_ = seqno;
      dupacks_ = 0;
      InflateWindow();
      SendPackets();
      return;
    }

    // We're in fast recovery, i.e. one packet has been
    // dropped but we're pretending it's not serious
    if (seqno >= recoverq_) {
      // got ACKs for all the "recovery window": resume
      // normal service
      uint32_t flightsize = highest_seqno_sent_ - seqno;
      cwnd_ = std::min(ssthresh_, flightsize + mss_);
      last_acked_ = seqno;
      dupacks_ = 0;
      in_fast_recovery_ = false;
      SendPackets();
      return;
    }

    // In fast recovery, and still getting ACKs for the
    // "recovery window"
    // This is dangerous. It means that several packets
    // got lost, not just the one that triggered FR.
    uint32_t new_data = seqno - last_acked_;
    last_acked_ = seqno;
    if (new_data < cwnd_) {
      cwnd_ -= new_data;
    } else {
      cwnd_ = 0;
    }

    cwnd_ += mss_;

    RetransmitPacket();
    if (fast_retx_callback_) {
      fast_retx_callback_(last_acked_ + 1);
    }

    SendPackets();
    return;
  }

  // It's a dup ack
  if (in_fast_recovery_) {
    // Still in fast recovery; hopefully the prodigal ACK is on it's way.
    cwnd_ += mss_;
    if (cwnd_ > maxcwnd_) {
      cwnd_ = maxcwnd_;
    }
    SendPackets();
    return;
  }

  // Not yet in fast recovery. What should we do instead?
  dupacks_++;
  if (dupacks_ != 3) {  // not yet serious worry
    SendPackets();
    return;
  }

  if (last_acked_ < recoverq_) {  // See RFC 3782: if we haven't
    // recovered from timeouts
    // etc. don't do fast recovery
    //    std::cout << "RFC 3782, retx suppressed\n";
    return;
  }

  // begin fast recovery
  ssthresh_ = std::max(cwnd_ / 2, static_cast<uint32_t>(2 * mss_));

  if (sawtooth_ > 0) {
    rtt_avg_ = rtt_cum_ / sawtooth_;
  } else {
    rtt_avg_ = 0;
  }

  sawtooth_ = 0;
  rtt_cum_ = 0;

  //  std::cout << "fast retx " << (last_acked_ + 1) << "\n";

  RetransmitPacket();
  if (fast_retx_callback_) {
    fast_retx_callback_(last_acked_ + 1);
  }

  cwnd_ = ssthresh_ + 3 * mss_;
  in_fast_recovery_ = true;
  recoverq_ = highest_seqno_sent_;  // _recoverq is the value of the
  // first ACK that tells us things
  // are back on track
}

void TCPSource::InflateWindow() {
  int newly_acked = (last_acked_ + cwnd_) - highest_seqno_sent_;
  // be very conservative - possibly not the best we can do, but
  // the alternative has bad side effects.
  if (newly_acked > mss_) {
    newly_acked = mss_;
  }
  if (newly_acked < 0) {
    return;
  }

  if (cwnd_ < ssthresh_) {  // slow start
    uint32_t increase =
        std::min(ssthresh_ - cwnd_, static_cast<uint32_t>(newly_acked));
    cwnd_ += increase;

    if (cwnd_ > maxcwnd_) {
      cwnd_ = maxcwnd_;
    }

    newly_acked -= increase;
  }

  // additive increase
  else {
    uint32_t pkts = cwnd_ / mss_;

    uint32_t increase = (newly_acked * mss_) / cwnd_;
    if (increase == 0) {
      increase = 1;
    }

    cwnd_ += increase;  // XXX beware large windows
    if (pkts != cwnd_ / mss_) {
      rtt_cum_ += rtt_;
      sawtooth_++;
    }
  }
}

void TCPSource::RtxTimerHook(EventQueueTime now) {
  if (highest_seqno_sent_ == 0) {
    return;
  }

  if (last_acked_ >= highest_seqno_sent_real_) {
    if (on_send_buffer_drained_) {
      UpdateCompletionTime();
      on_send_buffer_drained_();
      on_send_buffer_drained_ = nullptr;
    }

    return;
  }

  if (now.Raw() <= last_sent_time_.Raw() + rto_) {
    return;
  }

  //  LOG(ERROR) << "Retx timeout last sent "
  //             << event_queue_->TimeToRawMillis(last_sent_time_) << " rto "
  //             << event_queue_->TimeToRawMillis(
  //                    ncode::common::EventQueueTime(rto_))
  //             << " at " << id();

  if (in_fast_recovery_) {
    uint32_t flightsize = highest_seqno_sent_ - last_acked_;
    cwnd_ = std::min(ssthresh_, flightsize + mss_);
  }

  ssthresh_ = std::max(cwnd_ / 2, static_cast<uint32_t>(2 * mss_));

  if (sawtooth_ > 0) {
    rtt_avg_ = rtt_cum_ / sawtooth_;
  } else {
    rtt_avg_ = 0;
  }

  sawtooth_ = 0;
  rtt_cum_ = 0;

  cwnd_ = mss_;
  in_fast_recovery_ = false;
  recoverq_ = highest_seqno_sent_;
  highest_seqno_sent_ = last_acked_ + mss_;
  dupacks_ = 0;

  RetransmitPacket();
  if (retx_timeout_callback_) {
    retx_timeout_callback_(last_acked_ + 1);
  }
}

void TCPSource::RetransmitPacket() {
  EventQueueTime now = event_queue_->CurrentTime();
  auto pkt_ptr = GetFreeList<TCPPacket>().New(five_tuple_, mss_, now,
                                              SeqNum(last_acked_ + 1));

  last_sent_time_ = now;
  SendPacket(std::move(pkt_ptr));
}

void TCPSource::SendPackets() {
  if (last_acked_ >= highest_seqno_sent_real_ && send_buffer_ == 0 &&
      on_send_buffer_drained_) {
    UpdateCompletionTime();
    on_send_buffer_drained_();
    on_send_buffer_drained_ = nullptr;
    return;
  }

  EventQueueTime now = event_queue_->CurrentTime();
  while (last_acked_ + cwnd_ >= highest_seqno_sent_ + mss_) {
    if (send_buffer_ == 0) {
      break;
    }

    if (highest_seqno_sent_ == 0) {
      first_sent_time_ = now;
    }

    size_t to_tx = std::min(static_cast<uint64_t>(mss_), send_buffer_);
    auto pkt_ptr = GetFreeList<TCPPacket>().New(
        five_tuple_, to_tx, now, SeqNum(highest_seqno_sent_ + 1));

    send_buffer_ -= to_tx;
    highest_seqno_sent_ += to_tx;  // XX beware wrapping
    highest_seqno_sent_real_ += to_tx;

    last_sent_time_ = now;
    SendPacket(std::move(pkt_ptr));
  }
}

void TCPSource::AddData(uint64_t bytes) {
  if (send_buffer_ < mss_) {
    Close();
  }

  // If the flow does not have any outstanding data its congestion window will
  // be 0 (set by Close()). In that case we will set it to its initial value.
  if (cwnd_ == 0) {
    cwnd_ = kInitialCWNDMultiplier * mss_;
  }

  if (send_buffer_ + bytes < send_buffer_) {
    // Send buffer will overflow. Ignore the new data.
    send_buffer_ = std::numeric_limits<uint64_t>::max();
  } else {
    send_buffer_ += bytes;
  }

  SendPackets();
}

TCPRtxTimer::TCPRtxTimer(const std::string& id, EventQueueTime scan_period,
                         EventQueue* event_queue)
    : SimComponent(id, event_queue),
      EventConsumer(id, event_queue),
      scan_period_(scan_period) {}

void TCPRtxTimer::HandleEvent() {
  EventQueueTime now = event_queue_->CurrentTime();
  for (const auto& tcp_ptr : tcp_sources_) {
    tcp_ptr->RtxTimerHook(now);
  }

  EnqueueIn(scan_period_);
}

void TCPRtxTimer::RegisterTCPSource(TCPSource* tcp_source) {
  tcp_sources_.emplace_back(tcp_source);
  if (tcp_sources_.size() == 1) {
    EnqueueIn(scan_period_);
  }
}

void TCPSink::Reset() {
  cumulative_ack_ = 0;
  last_seen_incoming_tag_ = PacketTag::Max();
  received_.clear();
}

void TCPSink::ReceivePacket(PacketPtr pkt) {
  using namespace std::chrono;

  const TCPPacket* tcp_packet = static_cast<const TCPPacket*>(pkt.get());

  uint64_t seqno = tcp_packet->sequence().Raw();
  size_t size_bytes = pkt->size_bytes();
  if (seqno == 1) {
    // This is the first packet in the flow.
    Reset();
  }

  if (last_seen_incoming_tag_ != pkt->tag()) {
    ++tag_change_count_;
    if (incoming_tag_change_callback_) {
      incoming_tag_change_callback_(tag_change_count_);
    }
    last_seen_incoming_tag_ = pkt->tag();
  }

  if (seqno == cumulative_ack_ + 1) {  // it's the next expected seq no
    cumulative_ack_ = seqno + size_bytes - 1;
    // are there any additional received packets we can now ack?
    while (!received_.empty() && (received_.front() == cumulative_ack_ + 1)) {
      received_.pop_front();
      cumulative_ack_ += size_bytes;
    }
  } else if (seqno < cumulative_ack_ + 1) {
  }       // must have been a bad retransmit
  else {  // it's not the next expected sequence number
    if (received_.empty()) {
      received_.push_front(seqno);
    } else if (seqno > received_.back()) {  // likely case
      received_.push_back(seqno);
    } else {  // uncommon case - it fills a hole
      for (auto it = received_.begin(); it != received_.end(); ++it) {
        if (seqno == (*it)) break;  // it's a bad retransmit
        if (seqno < (*it)) {
          received_.insert(it, seqno);
          break;
        }
      }
    }
  }

  SendAck(pkt->time_sent());
}

void TCPSink::SendAck(EventQueueTime time_sent) {
  auto pkt_ptr = GetFreeList<TCPPacket>().New(five_tuple_, kAckSize, time_sent,
                                              SeqNum(cumulative_ack_));
  SendPacket(std::move(pkt_ptr));
}

}  // namespace htsim
}  // namespace ncode
