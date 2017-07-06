#include "match.h"

#include "../logging.h"
#include "../map_util.h"
#include "../substitute.h"

namespace nc {
namespace htsim {

bool operator<(const MatchRuleKey& lhs, const MatchRuleKey& rhs) {
  return std::tie(lhs.input_port_, lhs.tag_, lhs.five_tuples_) <
         std::tie(rhs.input_port_, rhs.tag_, rhs.five_tuples_);
}

std::string MatchRuleKey::ToString() const {
  std::string out;
  std::function<std::string(const net::FiveTuple&)> formatter = [](
      const net::FiveTuple& five_tuple) { return five_tuple.ToString(); };
  SubstituteAndAppend(&out, "sp: $0, tag: $1, tuples: [$2]", input_port_.Raw(),
                      tag_.Raw(), Join(five_tuples_, ",", formatter));
  return out;
}

std::ostream& operator<<(std::ostream& output, const MatchRuleKey& op) {
  output << op.ToString();
  return output;
}

bool operator==(const MatchRuleKey& lhs, const MatchRuleKey& rhs) {
  return lhs.tag() == rhs.tag() && lhs.input_port() == rhs.input_port() &&
         lhs.five_tuples() == rhs.five_tuples();
}

MatchRuleAction::MatchRuleAction(net::DevicePortNumber output_port,
                                 PacketTag tag, uint32_t weight)
    : parent_rule_(nullptr),
      output_port_(output_port),
      tag_(tag),
      weight_(weight),
      stats_(output_port, tag),
      sample_prob_(0),
      generator_(output_port.Raw() + weight),
      distribution_(0, 1.0),
      preferential_drop_(false) {}

MatchRuleAction::MatchRuleAction(const MatchRuleAction& other)
    : MatchRuleAction(other.output_port_, other.tag_, other.weight_) {
  preferential_drop_ = other.preferential_drop_;
  if (other.sample_prob_ != 0) {
    CHECK(other.flow_counter_);
    EnableFlowCounter(1 / other.sample_prob_,
                      other.flow_counter_->event_queue());
  }
}

void MatchRuleAction::EnableFlowCounter(size_t n, EventQueue* event_queue) {
  CHECK(n != 0);
  sample_prob_ = 1.0 / n;
  flow_counter_ = make_unique<FlowCounter>(event_queue);
}

ActionStats MatchRuleAction::Stats(bool include_flow_count) const {
  if (!include_flow_count || !flow_counter_) {
    return stats_;
  }

  ActionStats to_return = stats_;
  to_return.flow_count = flow_counter_->EstimateCount();
  return to_return;
}

double MatchRuleAction::FractionOfTraffic() const {
  CHECK(parent_rule_ != nullptr) << "No parent rule set yet!";
  double total_weight = 0;
  for (const MatchRuleAction* action : parent_rule_->actions()) {
    total_weight += action->weight();
  }
  return weight_ / total_weight;
}

void MatchRuleAction::UpdateStats(const Packet& packet) {
  uint64_t prev = stats_.total_bytes_matched;
  stats_.total_bytes_matched += packet.size_bytes();
  stats_.total_pkts_matched += 1;
  CHECK(prev <= stats_.total_bytes_matched) << prev << " matched "
                                            << packet.size_bytes();

  if (sample_prob_ != 0 && flow_counter_) {
    if (distribution_(generator_) <= sample_prob_) {
      flow_counter_->NewPacket(packet.five_tuple());
    }
  }
}

std::string MatchRuleAction::ToString() const {
  std::string out;
  SubstituteAndAppend(&out, "(out: $0, tag: $1, flow counter: $2, ",
                      output_port_.Raw(), tag_.Raw(),
                      flow_counter_.get() != nullptr);
  if (parent_rule_ != nullptr) {
    SubstituteAndAppend(&out, "w: $0)", FractionOfTraffic());
  } else {
    SubstituteAndAppend(&out, "w: $0)", weight());
  }

  return out;
}

std::ostream& operator<<(std::ostream& output, const MatchRuleAction& op) {
  output << op.ToString();
  return output;
}

void MatchRule::set_parent_matcher(Matcher* matcher) {
  CHECK(parent_matcher_ == nullptr);
  parent_matcher_ = matcher;
}

void MatchRule::AddAction(std::unique_ptr<MatchRuleAction> action) {
  action->set_parent_rule(this);
  for (const auto& current_action : actions_) {
    if (current_action->output_port() == action->output_port()) {
      CHECK(current_action->tag() != action->tag())
          << "Duplicate port " << action->output_port() << " and tag "
          << action->tag() << " at "
          << (parent_matcher_ == nullptr ? "UNKNOWN" : parent_matcher_->id());
    }
  }

  actions_.emplace_back(std::move(action));

  total_weight_ = 0;
  for (const auto& current_action : actions_) {
    total_weight_ += current_action->weight();
  }
}

std::vector<const MatchRuleAction*> MatchRule::actions() const {
  std::vector<const MatchRuleAction*> return_vector;
  for (const auto& output_action : actions_) {
    return_vector.emplace_back(output_action.get());
  }

  return return_vector;
}

const MatchRuleAction* MatchRule::ChooseOrNull(const Packet& packet) {
  MatchRuleAction* action = ChooseOrNull(packet.five_tuple());
  if (action != nullptr) {
    action->UpdateStats(packet);
  }

  return action;
}

const MatchRuleAction* MatchRule::ExplicitChooseOrDie(const Packet& packet,
                                                      size_t i) {
  CHECK(i < actions_.size());
  MatchRuleAction* action = actions_[i].get();
  action->UpdateStats(packet);
  return action;
}

MatchRuleAction* MatchRule::ChooseOrNull(const net::FiveTuple& five_tuple) {
  if (actions_.size() == 1) {
    MatchRuleAction* output_action = actions_.front().get();
    return output_action;
  }

  size_t hash = five_tuple.hash();
  if (total_weight_ == 0) {
    // Rule with no actions
    return nullptr;
  }

  hash = hash % total_weight_;
  for (const auto& output_action : actions_) {
    uint32_t weight = output_action->weight();
    if (hash < weight) {
      return output_action.get();
    }

    hash -= weight;
  }

  LOG(FATAL) << "Should not happen";
  return nullptr;
}

std::string MatchRule::ToString() const {
  std::string out;
  std::function<std::string(const std::unique_ptr<MatchRuleAction>&)> f = [](
      const std::unique_ptr<MatchRuleAction>& action) {
    return action->ToString();
  };
  StrAppend(&out, key_.ToString(), " -> [", Join(actions_, ",", f), "]");

  if (parent_matcher_) {
    StrAppend(&out, " at ", parent_matcher_->id());
  }

  return out;
}

std::vector<ActionStats> MatchRule::Stats(bool include_flow_count) const {
  std::vector<ActionStats> out;
  for (const auto& action : actions_) {
    out.emplace_back(action->Stats(include_flow_count));
  }

  return out;
}

void MatchRule::MergeStats(const MatchRule& other_rule) {
  for (auto& action : actions_) {
    for (const MatchRuleAction* other_action : other_rule.actions()) {
      if (action->tag() == other_action->tag() &&
          action->output_port() == other_action->output_port()) {
        action->MergeStats(other_action->Stats(false));
      }
    }
  }
}

std::ostream& operator<<(std::ostream& output, const MatchRule& op) {
  output << op.ToString();
  return output;
}

Matcher::Matcher(const std::string& id) : id_(id) {}

static const MatchRuleAction* GetActionOrNull(const Packet& packet,
                                              MatchRule* rule) {
  const MatchRuleAction* action_chosen = rule->ChooseOrNull(packet);
  if (action_chosen == nullptr) {
    return nullptr;
  }

  return action_chosen;
}

const MatchRuleAction* Matcher::MatchOrNull(const Packet& pkt,
                                            net::DevicePortNumber input_port) {
  CHECK(input_port != kWildDevicePortNumber) << "Bad input port in MatchOrNull";

  MatchRule* rule = root_.MatchOrNull(pkt.five_tuple(), input_port, pkt.tag());
  if (rule == nullptr) {
    return nullptr;
  }

  return GetActionOrNull(pkt, rule);
}

void Matcher::AddRule(std::unique_ptr<MatchRule> rule) {
  const MatchRuleKey& key = rule->key();
  rule->set_parent_matcher(this);
  MatchRule* rule_raw_ptr = rule.get();

  // Rule with no actions will cause the current rule with the same key to be
  // deleted.
  bool delete_rule = rule->actions().empty();
  if (!delete_rule) {
    for (const net::FiveTuple& five_tuple : key.five_tuples()) {
      root_.InsertOrUpdate(five_tuple, key.input_port(), key.tag(),
                           rule_raw_ptr);
    }
  }

  MatchRule* to_clear = FindSmartPtrOrNull(all_rules_, key);
  if (to_clear != nullptr) {
    root_.ClearRule(to_clear);
  }

  if (delete_rule) {
    all_rules_.erase(key);
  } else {
    all_rules_[key] = std::move(rule);
  }

  std::string prefix = to_clear == nullptr ? "Added" : "Updated";
  LOG(INFO) << prefix << " rule " << rule_raw_ptr->ToString() << " at " << id_;
}

void Matcher::PopulateSSCPStats(bool include_flow_counts,
                                SSCPStatsReply* stats_reply) const {
  for (const auto& key_and_rule : all_rules_) {
    stats_reply->AddStats(key_and_rule.first,
                          key_and_rule.second->Stats(include_flow_counts));
  }
}

std::unique_ptr<MatchRule> MatchRule::Clone() const {
  auto clone = make_unique<MatchRule>(key_);
  for (const auto& action : actions_) {
    auto action_clone = make_unique<MatchRuleAction>(*action);
    clone->AddAction(std::move(action_clone));
  }

  return clone;
}

void MatchRuleAction::MergeStats(const ActionStats& stats) {
  uint64_t prev_bytes = stats_.total_bytes_matched;
  uint64_t prev_pkts = stats_.total_pkts_matched;
  stats_.total_bytes_matched += stats.total_bytes_matched;
  stats_.total_pkts_matched += stats.total_pkts_matched;

  CHECK(stats_.total_bytes_matched >= prev_bytes) << stats_.total_bytes_matched
                                                  << " vs " << prev_bytes;
  CHECK(stats_.total_pkts_matched >= prev_pkts) << stats_.total_pkts_matched
                                                << " vs " << prev_pkts;
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<0>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(five_tuple);
  Unused(input_tag);
  return {input_port.Raw(), kWildDevicePortNumber.Raw()};
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<1>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(five_tuple);
  Unused(input_port);
  return {input_tag.Raw(), kWildPacketTag.Raw()};
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<2>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(input_port);
  Unused(input_tag);
  return {five_tuple.ip_dst().Raw(), kWildIPAddress.Raw()};
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<3>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(input_port);
  Unused(input_tag);
  return {five_tuple.ip_src().Raw(), kWildIPAddress.Raw()};
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<4>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(input_port);
  Unused(input_tag);
  return {five_tuple.ip_proto().Raw(), kWildIPProto.Raw()};
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<5>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(input_port);
  Unused(input_tag);
  return {five_tuple.src_port().Raw(), kWildAccessLayerPort.Raw()};
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<6>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(input_port);
  Unused(input_tag);
  return {five_tuple.dst_port().Raw(), kWildAccessLayerPort.Raw()};
}

SSCPStatsRequest::SSCPStatsRequest(net::IPAddress ip_src, net::IPAddress ip_dst,
                                   EventQueueTime time_sent,
                                   bool include_flow_counts)
    : SSCPMessage(ip_src, ip_dst, kSSCPStatsRequestType, time_sent),
      include_flow_counts_(include_flow_counts) {}

PacketPtr SSCPStatsRequest::Duplicate() const {
  auto new_msg = GetFreeList<SSCPStatsRequest>().New(
      five_tuple_.ip_src(), five_tuple_.ip_dst(), time_sent_,
      include_flow_counts_);
  return std::move(new_msg);
}

std::string SSCPStatsRequest::ToString() const {
  return Substitute("MSG $0 -> $1 : SSCPStatsRequest, flow counts: $2",
                    net::IPToStringOrDie(five_tuple_.ip_src()),
                    net::IPToStringOrDie(five_tuple_.ip_dst()),
                    include_flow_counts_);
}

SSCPStatsReply::SSCPStatsReply(net::IPAddress ip_src, net::IPAddress ip_dst,
                               EventQueueTime time_sent)
    : SSCPMessage(ip_src, ip_dst, kSSCPStatsReplyType, time_sent) {}

PacketPtr SSCPStatsReply::Duplicate() const {
  auto new_msg = GetFreeList<SSCPStatsReply>().New(
      five_tuple_.ip_src(), five_tuple_.ip_dst(), time_sent_);
  new_msg->stats_ = stats_;
  return std::move(new_msg);
}

std::string SSCPStatsReply::ToString() const {
  return Substitute("MSG $0 -> $1 : SSCPStatsReply",
                    net::IPToStringOrDie(five_tuple_.ip_src()),
                    net::IPToStringOrDie(five_tuple_.ip_dst()));
}

}  // namepsace htsim
}  // namespace ncode
