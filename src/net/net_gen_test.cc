#include "net_gen.h"

#include <stddef.h>
#include <algorithm>
#include <set>
#include <string>

#include "gtest/gtest.h"
#include "../strutil.h"

namespace nc {
namespace net {

using namespace std::chrono;
static constexpr Bandwidth kBandwidth = Bandwidth::FromBitsPerSecond(10000);

static std::set<std::string> GetEndpoints(const GraphBuilder& builder) {
  std::set<std::string> endpoints;
  for (const auto& link : builder.links()) {
    endpoints.insert(link.src_id());
    endpoints.insert(link.dst_id());

    // All links should have src and dst ports set to positive numbers.
    CHECK(DevicePortNumber::Zero() != link.src_port());
    CHECK(DevicePortNumber::Zero() != link.dst_port());
  }

  return endpoints;
}

TEST(HE, Generate) {
  GraphBuilder builder = GenerateHE(kBandwidth, Delay(0), 1);
  // All links should be bidirectional -- 2x64 in total.
  ASSERT_EQ(112ul, builder.links().size());
  std::set<std::string> endpoints = GetEndpoints(builder);

  ASSERT_EQ(31ul, endpoints.size());
}

TEST(NTT, Generate) {
  GraphBuilder builder = GenerateNTT();
  ASSERT_EQ(176ul, builder.links().size());

  std::set<std::string> endpoints = GetEndpoints(builder);
  ASSERT_EQ(43ul, endpoints.size());
}

TEST(HE, GenerateDelayAdd) {
  GraphBuilder builder = GenerateHE(kBandwidth, Delay(0), 1);
  GraphBuilder builder_plus = GenerateHE(kBandwidth, Delay(10000), 1);
  for (size_t i = 0; i < builder.links().size(); ++i) {
    const auto& link = builder.links()[i];
    const auto& link_plus = builder_plus.links()[i];
    ASSERT_EQ(link_plus.delay(), link.delay() + Delay(10000));
  }
}

TEST(HE, GenerateDelayMultiply) {
  GraphBuilder builder = GenerateHE(kBandwidth, Delay(0), 1);
  GraphBuilder builder_times_1000 = GenerateHE(kBandwidth, Delay(0), 1000);
  for (size_t i = 0; i < builder.links().size(); ++i) {
    const auto& link = builder.links()[i];
    const auto& link_times_1000 = builder_times_1000.links()[i];
    ASSERT_EQ(link.delay(), std::max(Delay(1), link_times_1000.delay() / 1000));
  }
}

TEST(Ladder, NoLevels) {
  ASSERT_DEATH(GenerateLadder(0, kBandwidth, Delay(10000)), ".*");
}

TEST(Ladder, SingleLevel) {
  GraphBuilder builder_ladder = GenerateLadder(1, kBandwidth, Delay(10000));
  GraphBuilder builder = GenerateFullGraph(2, kBandwidth, Delay(10000));
  ASSERT_EQ(builder.links(), builder_ladder.links());
}

TEST(Ladder, MultiLevel) {
  GraphBuilder builder = GenerateLadder(10, kBandwidth, Delay(10000));
  ASSERT_EQ((1 + 9 * 5) * 2ul, builder.links().size());

  std::set<std::string> nodes = GetEndpoints(builder);
  ASSERT_EQ(20ul + 18ul, nodes.size());
}

TEST(Ladder, MultiLevelCentralBandwidth) {
  GraphBuilder builder = GenerateLadder(10, kBandwidth, Delay(10000), 0.5);
  GraphStorage storage(builder);

  for (uint32_t i = 0; i < 10; ++i) {
    std::string src = StrCat("N", i * 4);
    std::string dst = StrCat("N", i * 4 + 1);
    const GraphLink* link = storage.LinkPtrOrDie(src, dst);
    ASSERT_EQ(link->bandwidth().bps(), kBandwidth.bps() * 0.5);
  }
}

TEST(Random, ZeroSize) {
  std::mt19937 random(1.0);

  GraphBuilder builder = GenerateRandom(0, 1.0, Delay(10000), Delay(10000),
                                        kBandwidth, kBandwidth, &random);
  ASSERT_TRUE(builder.links().empty());

  // Even with 0 probability the graph should be connected.
  builder = GenerateRandom(10, 0.0, Delay(10000), Delay(10000), kBandwidth,
                           kBandwidth, &random);
  ASSERT_EQ(18ul, builder.links().size());
}

TEST(Random, BadArgs) {
  std::mt19937 random(1.0);
  ASSERT_DEATH(GenerateRandom(0, 1.0, Delay(20000), Delay(10000),
                              Bandwidth::FromBitsPerSecond(1000),
                              Bandwidth::FromBitsPerSecond(1000), &random),
               ".*");
  ASSERT_DEATH(GenerateRandom(0, 1.0, Delay(10000), Delay(10000),
                              Bandwidth::FromBitsPerSecond(2000),
                              Bandwidth::FromBitsPerSecond(1000), &random),
               ".*");
}

TEST(Random, Random) {
  std::mt19937 random(1.0);
  GraphBuilder builder = GenerateRandom(
      1000, 0.1, Delay(1000), Delay(3000), Bandwidth::FromBitsPerSecond(1000),
      Bandwidth::FromBitsPerSecond(3000), &random);

  // There should be more than 100000 links.
  ASSERT_LT(100000ul, builder.links().size());

  Delay delay_sum = Delay::zero();
  Bandwidth bw_sum = Bandwidth::Zero();
  for (const auto& link : builder.links()) {
    delay_sum += link.delay();
    bw_sum += link.bandwidth();
  }

  double mean_delay =
      duration_cast<duration<double>>(delay_sum / builder.links().size())
          .count();
  ASSERT_NEAR(0.002, mean_delay, 0.001);

  double mean_bw = (bw_sum / builder.links().size()).bps();
  ASSERT_NEAR(2000, mean_bw, 1000);
}

TEST(Load, Repetita) {
  std::string test_topology =
      "NODES 3\n"
      "label x y\n"
      "0_UNIBRAW 1.0 2.0\n"
      "2_KEIO 3.0 4.0\n"
      "7_ITB 5.0 6.0\n"
      "EDGES 4\n"
      "\n"
      "\n"
      "label src dest weight bw delay\n"
      "edge_10 2 1 10 1000000 10\n"
      "edge_0 0 1 10 1000000 10\n"
      "edge_1 1 0 10 1000000 10\n"
      "edge_11 1 2 10 1000000 10\n";

  std::map<std::string, std::pair<double, double>> locations;
  std::vector<std::string> nodes_in_order;
  GraphBuilder builder =
      LoadRepetitaOrDie(test_topology, &nodes_in_order, &locations);

  ASSERT_EQ(4ul, builder.links().size());
  ASSERT_EQ("7_ITB", builder.links().front().src_id());
  ASSERT_EQ("2_KEIO", builder.links().front().dst_id());
  ASSERT_EQ(Bandwidth::FromKBitsPerSecond(1000000),
            builder.links().front().bandwidth());
  ASSERT_EQ(microseconds(10), builder.links().front().delay());
  ASSERT_EQ("7_ITB", builder.links().back().dst_id());

  std::map<std::string, std::pair<double, double>> model_locations;
  model_locations["0_UNIBRAW"] = {1.0, 2.0};
  model_locations["2_KEIO"] = {3.0, 4.0};
  model_locations["7_ITB"] = {5.0, 6.0};
  ASSERT_EQ(model_locations, locations);

  std::vector<std::string> model = {"0_UNIBRAW", "2_KEIO", "7_ITB"};
  ASSERT_EQ(nodes_in_order, model);

  std::string serialized_back = builder.ToRepetita();
  std::string serialized_test_topology =
      "NODES 3\n"
      "label x y\n"
      "0_UNIBRAW 0 0\n"
      "2_KEIO 0 0\n"
      "7_ITB 0 0\n"
      "\n"
      "EDGES 4\n"
      "label src dest weight bw delay\n"
      "edge_0 2 1 0 1000000 10\n"
      "edge_1 0 1 0 1000000 10\n"
      "edge_2 1 0 0 1000000 10\n"
      "edge_3 1 2 0 1000000 10\n";
}

}  // namespace net
}  // namespace ncode
