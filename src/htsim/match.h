#ifndef NCODE_HTSIM_MATCH_H
#define NCODE_HTSIM_MATCH_H

#include <cstdint>
#include <limits>
#include <unordered_map>
#include <map>
#include <unordered_map>
#include <sstream>
#include <vector>

#include "../net/net_common.h"
#include "packet.h"

namespace nc {
namespace htsim {

// Define wildcards that can match any value.
static constexpr PacketTag kWildPacketTag = PacketTag(0);
static constexpr net::AccessLayerPort kWildAccessLayerPort =
    net::AccessLayerPort(0);
static constexpr net::DevicePortNumber kWildDevicePortNumber =
    net::DevicePortNumber(0);
static constexpr net::IPAddress kWildIPAddress = net::IPAddress(0);
static constexpr net::IPProto kWildIPProto = net::IPProto(0);

// A special value that can be used in rules if the incoming tag value should
// not be changed.
static constexpr PacketTag kNullPacketTag =
    PacketTag(std::numeric_limits<uint32_t>::max());

// A key that can be used to match on. Traffic can be matched on input port, tag
// and a number of 5-tuples. If more than one tuple is specified any tuple can
// match. Tag set to kNoTag is a wildcard -- any tag will match. Input port set
// to kNoInputPort likewise.
class MatchRuleKey {
 public:
  MatchRuleKey(PacketTag tag, net::DevicePortNumber input_port,
               const std::vector<net::FiveTuple>& five_tuples)
      : tag_(tag), input_port_(input_port), five_tuples_(five_tuples) {}

  net::DevicePortNumber input_port() const { return input_port_; }
  PacketTag tag() const { return tag_; }
  const std::vector<net::FiveTuple>& five_tuples() const {
    return five_tuples_;
  }

  // Prints the key.
  std::string ToString() const;

  friend bool operator<(const MatchRuleKey& lsh, const MatchRuleKey& rhs);
  friend std::ostream& operator<<(std::ostream& output, const MatchRuleKey& op);
  friend bool operator==(const MatchRuleKey& lhs, const MatchRuleKey& rhs);

 private:
  // Tag that the key matches on.
  PacketTag tag_;

  // Input port that the key matches on.
  net::DevicePortNumber input_port_;

  // Tuples that the key matches on.
  std::vector<net::FiveTuple> five_tuples_;
};

// Statistics about a MatchRuleAction (below).
struct ActionStats {
  ActionStats(net::DevicePortNumber output_port, PacketTag tag)
      : output_port(output_port),
        tag(tag),
        total_bytes_matched(0),
        total_pkts_matched(0) {}
  net::DevicePortNumber output_port;
  PacketTag tag;

  uint64_t total_bytes_matched;
  uint64_t total_pkts_matched;
};

class MatchRule;

// An action to take once a rule is matched.
class MatchRuleAction {
 public:
  MatchRuleAction(net::DevicePortNumber output_port, PacketTag tag,
                  uint32_t weight);

  // Updates the stats of this action.
  void UpdateStats(uint32_t bytes_matched);

  inline net::DevicePortNumber output_port() const { return output_port_; }
  inline PacketTag tag() const { return tag_; }
  inline uint32_t weight() const { return weight_; }
  const ActionStats& Stats() const { return stats_; }

  // Sets the parent rule of this action. Will be called automatically when the
  // action is added to a rule.
  void set_parent_rule(MatchRule* rule) { parent_rule_ = rule; }

  // Returns the fraction of the parent rule's traffic that flows over this
  // action. Will die if there is no parent rule set.
  double FractionOfTraffic() const;

  // Prints the action.
  std::string ToString() const;

  friend std::ostream& operator<<(std::ostream& output,
                                  const MatchRuleAction& op);

  bool sample() const { return sample_; }
  void set_sample(bool sample) { sample_ = sample; }

  bool preferential_drop() const { return preferential_drop_; }
  void set_preferential_drop(bool prefererential_drop) {
    preferential_drop_ = prefererential_drop;
  }

  const MatchRule* parent_rule() const { return parent_rule_; }

  void MergeStats(const ActionStats& stats);

 private:
  // Non-owning pointer to the rule this action is part of.
  MatchRule* parent_rule_;

  // Incoming packets will be sent out this port. If set to 0 this rule is a
  // drop rule.
  net::DevicePortNumber output_port_;

  // How to tag the incoming packet.
  PacketTag tag_;

  // How much of the total weight of the rule is allocated to this action.
  uint32_t weight_;

  // Statistics.
  ActionStats stats_;

  // If sampling is enabled on the device this action is installed on then
  // packets that match this action will be sampled by the device.
  bool sample_;

  // If true packets matched by this action will have the preferential_drop flag
  // set. Once set the flag cannot be cleared by other rules.
  bool preferential_drop_;
};

class Matcher;

// A match rule will match certain packets and send them out one or more ports.
class MatchRule {
 public:
  MatchRule(MatchRuleKey key)
      : key_(key), total_weight_(0), parent_matcher_(nullptr) {}

  // Adds a new action to this rule.
  void AddAction(std::unique_ptr<MatchRuleAction> action);

  // Chooses an action from this rule's actions and updates the statistics. The
  // choice should be stable among 5-tuples and spread traffic among the actions
  // according to weight.
  MatchRuleAction* ChooseOrNull(const Packet& packet);

  // Like above, but only needs a 5-tuple and does not update the statistics.
  MatchRuleAction* ChooseOrNull(const net::FiveTuple& five_tuple);

  // Returns the registered actions. The pointers owned by this object.
  std::vector<const MatchRuleAction*> actions() const;

  // Returns the key of this rule.
  const MatchRuleKey& key() const { return key_; }

  // Prints the rule.
  std::string ToString() const;

  friend std::ostream& operator<<(std::ostream& output, const MatchRule& op);

  // Clones this rule.
  std::unique_ptr<MatchRule> Clone() const;

  // Will return stats for each action.
  std::vector<ActionStats> Stats() const;

  void set_parent_matcher(Matcher* matcher);

  // Combines this rule's stats with the other rule's stats.
  void MergeStats(const MatchRule& other_rule);

 private:
  // Each match rule has a key.
  MatchRuleKey key_;

  // Total weight of all actions.
  uint32_t total_weight_;

  // The actions.
  std::vector<std::unique_ptr<MatchRuleAction>> actions_;

  // The parent matcher or null if rule not installed yet.
  Matcher* parent_matcher_;

  DISALLOW_COPY_AND_ASSIGN(MatchRule);
};

template <size_t Index>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag) {
  Unused(five_tuple);
  Unused(input_port);
  Unused(input_tag);

  LOG(FATAL) << "Should never happen";
  return {0, 0};
}

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<0>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag);

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<1>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag);

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<2>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag);

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<3>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag);

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<4>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag);

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<5>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag);

template <>
std::pair<uint32_t, uint32_t> GetKeyAndWildcard<6>(
    const net::FiveTuple& five_tuple, net::DevicePortNumber input_port,
    PacketTag input_tag);

template <size_t Index, size_t MaxIndex>
class MatchNode {
 public:
  void ClearRule(MatchRule* rule) {
    for (auto& key_and_child : children_) {
      MatchNode<Index + 1, MaxIndex>& child = key_and_child.second;
      child.ClearRule(rule);
    }

    if (wildcard_child_) {
      wildcard_child_->ClearRule(rule);
    }
  }

  MatchRule* MatchOrNull(const net::FiveTuple& five_tuple,
                         net::DevicePortNumber input_port,
                         PacketTag input_tag) {
    uint32_t key;
    uint32_t wildcard;
    std::tie(key, wildcard) =
        GetKeyAndWildcard<Index>(five_tuple, input_port, input_tag);

    MatchNode<Index + 1, MaxIndex>* child = FindOrNull(children_, key);
    if (child != nullptr) {
      MatchRule* exact_match =
          child->MatchOrNull(five_tuple, input_port, input_tag);
      if (exact_match != nullptr) {
        return exact_match;
      }
    }

    if (wildcard_child_) {
      return wildcard_child_->MatchOrNull(five_tuple, input_port, input_tag);
    }
    return nullptr;
  }

  void InsertOrUpdate(const net::FiveTuple& five_tuple,
                      net::DevicePortNumber input_port, PacketTag input_tag,
                      MatchRule* rule) {
    uint32_t key;
    uint32_t wildcard;
    std::tie(key, wildcard) =
        GetKeyAndWildcard<Index>(five_tuple, input_port, input_tag);

    MatchNode<Index + 1, MaxIndex>* child;
    if (key == wildcard) {
      if (!wildcard_child_) {
        wildcard_child_ = make_unique<MatchNode<Index + 1, MaxIndex>>();
      }
      child = wildcard_child_.get();
    } else {
      child = &children_[key];
    }
    child->InsertOrUpdate(five_tuple, input_port, input_tag, rule);
  }

 private:
  std::unordered_map<uint32_t, MatchNode<Index + 1, MaxIndex>> children_;
  std::unique_ptr<MatchNode<Index + 1, MaxIndex>> wildcard_child_;
};

// This is the leaf node.
template <>
class MatchNode<7, 7> {
 public:
  MatchNode<7, 7>() : rule_(nullptr) {}

  void InsertOrUpdate(const net::FiveTuple& five_tuple,
                      net::DevicePortNumber input_port, PacketTag input_tag,
                      MatchRule* rule) {
    Unused(five_tuple);
    Unused(input_port);
    Unused(input_tag);
    if (rule_) {
      rule->MergeStats(*rule_);
    }

    rule_ = rule;
    return;
  }

  MatchRule* MatchOrNull(const net::FiveTuple& five_tuple,
                         net::DevicePortNumber input_port,
                         PacketTag input_tag) {
    Unused(five_tuple);
    Unused(input_port);
    Unused(input_tag);
    return rule_;
  }

  void ClearRule(MatchRule* rule) {
    if (rule_ == rule) {
      rule_ = nullptr;
    }
  }

 private:
  // Only used if this is a leaf.
  MatchRule* rule_;
};

// A request that causes the router to return statistics for each rule.
class SSCPStatsRequest : public SSCPMessage {
 public:
  static constexpr uint8_t kSSCPStatsRequestType = 253;

  SSCPStatsRequest(net::IPAddress ip_src, net::IPAddress ip_dst,
                   EventQueueTime time_sent);

  PacketPtr Duplicate() const override;

  std::string ToString() const override;
};

class SSCPStatsReply : public SSCPMessage {
 public:
  static constexpr uint8_t kSSCPStatsReplyType = 252;

  SSCPStatsReply(net::IPAddress ip_src, net::IPAddress ip_dst,
                 EventQueueTime time_sent);

  void AddStats(const MatchRuleKey& key,
                const std::vector<ActionStats>& action_stats) {
    InsertOrDie(&stats_, key, action_stats);
  }

  PacketPtr Duplicate() const override;

  std::string ToString() const override;

  const std::map<MatchRuleKey, std::vector<ActionStats>>& stats() const {
    return stats_;
  }

 private:
  // For each rule a list of stats.
  std::map<MatchRuleKey, std::vector<ActionStats>> stats_;
};

// A matcher can match incoming packets against a set of rules.
class Matcher {
 public:
  static constexpr uint32_t kMaxTag = 1 << 16;
  static constexpr uint64_t kDummyCookie = std::numeric_limits<uint64_t>::max();

  Matcher(const std::string& id);

  // Attempts to match the header against the current set of rules. Will return
  // nullptr on failure, or a pointer to the action to be performed. Will also
  // update the action's stats upon a successful match.
  const MatchRuleAction* MatchOrNull(const Packet& packet,
                                     net::DevicePortNumber input_port);

  // Adds a new match rule to the current rule set.
  void AddRule(std::unique_ptr<MatchRule> rule);

  // Number of all rules in this matcher.
  size_t NumRules() const { return all_rules_.size(); }

  const std::string& id() const { return id_; }

  // Populates the stats in a StatsReply.
  void PopulateSSCPStats(SSCPStatsReply* stats_reply) const;

 private:
  MatchRule* MatchOrNullFromList(const Packet& pkt,
                                 net::DevicePortNumber input_port);

  // Human-readable identifier.
  const std::string id_;

  // Performs the actual matching. The root of the match tree.
  MatchNode<0, 7> root_;

  // Stores all rules that are owned by this object.
  std::map<MatchRuleKey, std::unique_ptr<MatchRule>> all_rules_;
};

}  // namespace htsim
}  // namesapce nc

#endif
