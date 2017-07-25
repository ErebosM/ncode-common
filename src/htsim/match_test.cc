#include "gtest/gtest.h"
#include "packet.h"
#include "match.h"

#include "../free_list.h"

namespace nc {
namespace htsim {
namespace {

using net::IPAddress;
using net::IPProto;
using net::AccessLayerPort;
using net::DevicePortNumber;

static constexpr IPAddress kSrc = IPAddress(1);
static constexpr IPAddress kOtherSrc = IPAddress(3);
static constexpr IPAddress kDst = IPAddress(2);
static constexpr IPAddress kOtherDst = IPAddress(4);
static constexpr AccessLayerPort kSrcPort = AccessLayerPort(100);
static constexpr AccessLayerPort kOtherSrcPort = AccessLayerPort(300);
static constexpr AccessLayerPort kDstPort = AccessLayerPort(200);
static constexpr AccessLayerPort kOtherDstPort = AccessLayerPort(400);
static constexpr IPProto kProto = IPProto(1);
static constexpr IPProto kOtherProto = IPProto(5);
static constexpr DevicePortNumber kDeviceInputPort = DevicePortNumber(1);
static constexpr DevicePortNumber kOtherDeviceInputPort = DevicePortNumber(3);
static constexpr DevicePortNumber kDeviceOutputPort = DevicePortNumber(10);
static constexpr DevicePortNumber kOtherDeviceOutputPort = DevicePortNumber(2);
static constexpr PacketTag kPacketInputTag = PacketTag(5);
static constexpr PacketTag kOtherPacketInputTag = PacketTag(55);
static constexpr PacketTag kPacketOutputTag = PacketTag(50);

TEST(Match, Empty) {
  Matcher matcher("test");

  net::FiveTuple five_tuple(kSrc, kDst, kProto, kSrcPort, kDstPort);
  auto pkt = GetFreeList<TCPPacket>().New(five_tuple, 10, EventQueueTime(100),
                                          SeqNum(0));
  ASSERT_EQ(nullptr, matcher.MatchOrNull(*pkt, kDeviceInputPort));
}

class MatchFixture : public ::testing::Test {
 protected:
  MatchFixture() : matcher_("test") {}

  // Adds a rule with a single action.
  void AddRule(IPAddress match_ip_src, IPAddress match_ip_dst,
               AccessLayerPort match_src_port, AccessLayerPort match_dst_port,
               IPProto match_ip_proto, DevicePortNumber match_in_port,
               PacketTag match_tag, DevicePortNumber out_port,
               PacketTag out_tag) {
    net::FiveTuple tuple(match_ip_src, match_ip_dst, match_ip_proto,
                         match_src_port, match_dst_port);
    MatchRuleKey key(match_tag, match_in_port, {tuple});
    auto rule = make_unique<MatchRule>(key);
    auto action = make_unique<MatchRuleAction>(out_port, out_tag, 1);

    rule->AddAction(std::move(action));
    matcher_.AddRule(std::move(rule));
  }

  // A discard rule is a rule with no actions.
  void AddDiscardRule(IPAddress match_ip_src, IPAddress match_ip_dst,
                      AccessLayerPort match_src_port,
                      AccessLayerPort match_dst_port, IPProto match_ip_proto,
                      DevicePortNumber match_in_port, PacketTag match_tag) {
    net::FiveTuple tuple(match_ip_src, match_ip_dst, match_ip_proto,
                         match_src_port, match_dst_port);
    MatchRuleKey key(match_tag, match_in_port, {tuple});
    auto rule = make_unique<MatchRule>(key);
    matcher_.AddRule(std::move(rule));
  }

  PacketPtr GetPacket(IPAddress ip_src, IPAddress ip_dst,
                      AccessLayerPort src_port, AccessLayerPort dst_port,
                      IPProto ip_proto, PacketTag tag) {
    net::FiveTuple tuple(ip_src, ip_dst, ip_proto, src_port, dst_port);
    auto pkt = GetFreeList<UDPPacket>().New(tuple, 10, EventQueueTime(10));
    pkt->set_tag(tag);
    return std::move(pkt);
  }

  PacketPtr GetRandomPacket(IPAddress ip_src, IPAddress ip_dst,
                            IPProto ip_proto) {
    auto max = std::numeric_limits<uint16_t>::max();
    auto src_port =
        AccessLayerPort((static_cast<double>(rand()) / RAND_MAX) * max);
    auto dst_port =
        AccessLayerPort((static_cast<double>(rand()) / RAND_MAX) * max);
    return GetPacket(ip_src, ip_dst, src_port, dst_port, ip_proto,
                     kWildPacketTag);
  }

  const MatchRuleAction* TryMatch(const PacketPtr& pkt,
                                  DevicePortNumber input_port) {
    return matcher_.MatchOrNull(*pkt, input_port);
  }

  void SetUp() override {}

  Matcher matcher_;
};

TEST_F(MatchFixture, SingleRule) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);
  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, DefaultRule) {
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kWildDevicePortNumber,
          kWildPacketTag, kDeviceOutputPort, kPacketOutputTag);
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kWildDevicePortNumber,
          kWildPacketTag, kOtherDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kOtherDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
  pkt_ptr =
      GetPacket(kOtherSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleBadInputPort) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);
  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);

  // Trying to match an incoming packet with non-valid input port
  ASSERT_DEATH(TryMatch(pkt_ptr, kWildDevicePortNumber), "input port");
}

TEST_F(MatchFixture, SingleRuleDouble) {
  // The active rule should be the most recent one.
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kWildPacketTag);

  // Rule with a different 5-tuple inserted between adding rules with the same
  // 5-tuple.
  AddRule(kSrc, kOtherDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kWildPacketTag);

  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kWildPacketTag);
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kOtherDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kOtherDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kOtherDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleDelete) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kWildPacketTag);
  AddDiscardRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
                 kPacketInputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
  ASSERT_EQ(0ul, matcher_.NumRules());
}

TEST_F(MatchFixture, SingleRuleDeleteWildcards) {
  AddRule(kWildIPAddress, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kWildPacketTag);
  AddDiscardRule(kWildIPAddress, kDst, kSrcPort, kDstPort, kProto,
                 kDeviceInputPort, kPacketInputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
  ASSERT_EQ(0ul, matcher_.NumRules());
}

TEST_F(MatchFixture, BadDelete) {
  AddDiscardRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
                 kPacketInputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
  ASSERT_EQ(0ul, matcher_.NumRules());
}

TEST_F(MatchFixture, WildcardTag) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kWildPacketTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, WildcardPort) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kWildDevicePortNumber,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleWrongPort) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kOtherDeviceInputPort));
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleWrongSrc) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kOtherSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleWildcardSrc) {
  AddRule(kWildIPAddress, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt1 =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  PacketPtr pkt2 =
      GetPacket(kOtherSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt1, kDeviceInputPort)->output_port());
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt2, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleWrongDst) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kOtherDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleWildcardDst) {
  AddRule(kSrc, kWildIPAddress, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt1 =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  PacketPtr pkt2 =
      GetPacket(kSrc, kOtherDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt1, kDeviceInputPort)->output_port());
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt2, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleWrongProto) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kOtherProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleWildcardProto) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kWildIPProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt1 =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  PacketPtr pkt2 =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kOtherProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt1, kDeviceInputPort)->output_port());
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt2, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleWrongSrcPort) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kOtherSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleWildcardSrcPort) {
  AddRule(kSrc, kDst, kWildAccessLayerPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt1 =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  PacketPtr pkt2 =
      GetPacket(kSrc, kDst, kOtherSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt1, kDeviceInputPort)->output_port());
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt2, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleWrongDstPort) {
  AddRule(kSrc, kDst, kSrcPort, kDstPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kOtherDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleWildcardDstPort) {
  AddRule(kSrc, kDst, kSrcPort, kWildAccessLayerPort, kProto, kDeviceInputPort,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  PacketPtr pkt1 =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  PacketPtr pkt2 =
      GetPacket(kSrc, kDst, kSrcPort, kOtherDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt1, kDeviceInputPort)->output_port());
  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt2, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, DefaultTupleMatch) {
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kDeviceInputPort, kWildPacketTag,
          kDeviceOutputPort, kWildPacketTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);

  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kOtherDeviceInputPort));
}

TEST_F(MatchFixture, DefaultTupleMatchTagged) {
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kDeviceInputPort, kPacketInputTag,
          kDeviceOutputPort, kWildPacketTag);

  PacketPtr pkt1 =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  PacketPtr pkt2 = GetPacket(kSrc, kDst, kSrcPort, kOtherDstPort, kProto,
                             kOtherPacketInputTag);

  ASSERT_EQ(kDeviceOutputPort, TryMatch(pkt1, kDeviceInputPort)->output_port());
  ASSERT_EQ(nullptr, TryMatch(pkt2, kDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleTagOnly) {
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kWildDevicePortNumber,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);
  ASSERT_EQ(1ul, matcher_.NumRules());

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());

  // Replace with a different one.
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kWildDevicePortNumber,
          kPacketInputTag, kOtherDeviceOutputPort, kPacketOutputTag);
  ASSERT_EQ(1ul, matcher_.NumRules());

  ASSERT_EQ(kOtherDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleTagOnlyDelete) {
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kWildDevicePortNumber,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);
  AddDiscardRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
                 kWildAccessLayerPort, kWildIPProto, kWildDevicePortNumber,
                 kPacketInputTag);

  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(nullptr, TryMatch(pkt_ptr, kDeviceInputPort));
}

TEST_F(MatchFixture, SingleRuleTagOnlyMoreSpecific) {
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kWildDevicePortNumber,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);
  AddRule(kSrc, kWildIPAddress, kWildAccessLayerPort, kWildAccessLayerPort,
          kWildIPProto, kWildDevicePortNumber, kPacketInputTag,
          kOtherDeviceOutputPort, kPacketOutputTag);

  // Packet with source matching the second rule.
  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kOtherDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());

  // Packet with a different source should match the first rule.
  pkt_ptr =
      GetPacket(kOtherSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, SingleRuleTagOnlyMoreSpecificReverseOrder) {
  AddRule(kSrc, kWildIPAddress, kWildAccessLayerPort, kWildAccessLayerPort,
          kWildIPProto, kWildDevicePortNumber, kPacketInputTag,
          kOtherDeviceOutputPort, kPacketOutputTag);
  AddRule(kWildIPAddress, kWildIPAddress, kWildAccessLayerPort,
          kWildAccessLayerPort, kWildIPProto, kWildDevicePortNumber,
          kPacketInputTag, kDeviceOutputPort, kPacketOutputTag);

  // Packet with source matching the first rule.
  PacketPtr pkt_ptr =
      GetPacket(kSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kOtherDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());

  // Packet with a different source should match the second rule.
  pkt_ptr =
      GetPacket(kOtherSrc, kDst, kSrcPort, kDstPort, kProto, kPacketInputTag);
  ASSERT_EQ(kDeviceOutputPort,
            TryMatch(pkt_ptr, kDeviceInputPort)->output_port());
}

TEST_F(MatchFixture, Balance) {
  net::FiveTuple tuple(kSrc, kDst, kProto, kWildAccessLayerPort,
                       kWildAccessLayerPort);
  MatchRuleKey key(kWildPacketTag, kDeviceInputPort, {tuple});

  auto rule = make_unique<MatchRule>(key);
  matcher_.AddRule(std::move(rule));
  ASSERT_EQ(0ul, matcher_.NumRules());

  for (size_t i = 0; i < 100000; ++i) {
    ASSERT_EQ(nullptr,
              TryMatch(GetRandomPacket(kSrc, kDst, kProto), kDeviceInputPort));
  }

  auto action =
      make_unique<MatchRuleAction>(kDeviceOutputPort, kPacketOutputTag, 100);
  MatchRuleAction* action_raw_ptr = action.get();
  rule = make_unique<MatchRule>(key);
  rule->AddAction(std::move(action));
  matcher_.AddRule(std::move(rule));

  // The new rule should supersede the old one.
  ASSERT_EQ(1ul, matcher_.NumRules());

  for (size_t i = 0; i < 1000000; ++i) {
    const MatchRuleAction* matched =
        TryMatch(GetRandomPacket(kSrc, kDst, kProto), kDeviceInputPort);
    ASSERT_EQ(action_raw_ptr, matched);
  }

  auto action_one =
      make_unique<MatchRuleAction>(kDeviceOutputPort, kPacketOutputTag, 100);
  auto action_two = make_unique<MatchRuleAction>(kOtherDeviceOutputPort,
                                                 kPacketOutputTag, 100);
  rule = make_unique<MatchRule>(key);
  rule->AddAction(std::move(action_one));
  rule->AddAction(std::move(action_two));
  matcher_.AddRule(std::move(rule));
  ASSERT_EQ(1ul, matcher_.NumRules());

  size_t matched_one = 0;
  size_t matched_two = 0;
  for (size_t i = 0; i < 1000000; ++i) {
    const MatchRuleAction* action =
        TryMatch(GetRandomPacket(kSrc, kDst, kProto), kDeviceInputPort);
    if (action->output_port() == kDeviceOutputPort) {
      matched_one++;
    } else if (action->output_port() == kOtherDeviceOutputPort) {
      matched_two++;
    } else {
      ASSERT_TRUE(false);
    }
  }

  ASSERT_NEAR(matched_one, matched_two, 10000);

  // Will update the second action's weight to make it take 3x more traffic than
  // the first one.
  action_one =
      make_unique<MatchRuleAction>(kDeviceOutputPort, kPacketOutputTag, 100);
  action_two = make_unique<MatchRuleAction>(kOtherDeviceOutputPort,
                                            kPacketOutputTag, 300);
  rule = make_unique<MatchRule>(key);
  rule->AddAction(std::move(action_one));
  rule->AddAction(std::move(action_two));
  matcher_.AddRule(std::move(rule));
  ASSERT_EQ(1ul, matcher_.NumRules());

  matched_one = 0;
  matched_two = 0;
  for (size_t i = 0; i < 1000000; ++i) {
    const MatchRuleAction* action =
        TryMatch(GetRandomPacket(kSrc, kDst, kProto), kDeviceInputPort);
    if (action->output_port() == kDeviceOutputPort) {
      matched_one++;
    } else if (action->output_port() == kOtherDeviceOutputPort) {
      matched_two++;
    } else {
      ASSERT_TRUE(false);
    }
  }

  // Diff within 1%
  ASSERT_NEAR(1.0 / 3, static_cast<double>(matched_one) / matched_two, 0.01);
}

TEST_F(MatchFixture, ActionsSameOutPort) {
  net::FiveTuple tuple(kSrc, kDst, kProto, kWildAccessLayerPort,
                       kWildAccessLayerPort);
  MatchRuleKey key(kWildPacketTag, kDeviceInputPort, {tuple});

  auto action_one =
      make_unique<MatchRuleAction>(kDeviceOutputPort, kPacketOutputTag, 100);
  auto action_two =
      make_unique<MatchRuleAction>(kDeviceOutputPort, kWildPacketTag, 100);

  auto rule = make_unique<MatchRule>(key);
  rule->AddAction(std::move(action_one));
  rule->AddAction(std::move(action_two));
  matcher_.AddRule(std::move(rule));

  size_t i1 = 0;
  size_t i2 = 0;
  for (size_t i = 0; i < 1000000; ++i) {
    const MatchRuleAction* matched =
        TryMatch(GetRandomPacket(kSrc, kDst, kProto), kDeviceInputPort);
    if (matched->tag() == kPacketOutputTag) {
      ++i1;
    } else if (matched->tag() == kWildPacketTag) {
      ++i2;
    } else {
      LOG(FATAL) << "Unexpected state";
    }
  }

  ASSERT_NEAR(i1, i2, 10000);
}

TEST_F(MatchFixture, ActionsSameOutPortSameTag) {
  net::FiveTuple tuple(kSrc, kDst, kProto, kWildAccessLayerPort,
                       kWildAccessLayerPort);
  MatchRuleKey key(kWildPacketTag, kDeviceInputPort, {tuple});

  auto action_one =
      make_unique<MatchRuleAction>(kDeviceOutputPort, kPacketOutputTag, 100);
  auto action_two =
      make_unique<MatchRuleAction>(kDeviceOutputPort, kPacketOutputTag, 100);

  auto rule = make_unique<MatchRule>(key);
  rule->AddAction(std::move(action_one));
  ASSERT_DEATH(rule->AddAction(std::move(action_two)), ".*");
}

}  // namespace
}  // namespace test
}  // namespace ht2sim
