#include "net_common.h"

#include <limits>
#include <utility>

#include "../substitute.h"
#include "gtest/gtest.h"
#include "net_gen.h"

namespace nc {
namespace net {
namespace {

using namespace std::chrono;

static constexpr char kSrc[] = "A";
static constexpr char kDst[] = "B";
static constexpr DevicePortNumber kSrcNetPort = net::DevicePortNumber(10);
static constexpr DevicePortNumber kDstNetPort = net::DevicePortNumber(20);
static constexpr Bandwidth kBw = net::Bandwidth::FromBitsPerSecond(20000);
static constexpr Delay kDelay = milliseconds(20);
static constexpr IPAddress kSrcIp = IPAddress(1);
static constexpr IPAddress kDstIp = IPAddress(2);
static constexpr IPProto kProto = IPProto(3);
static constexpr AccessLayerPort kSrcPort = AccessLayerPort(4);
static constexpr AccessLayerPort kDstPort = AccessLayerPort(5);

TEST(AddEdgeTest, AddEdgeBad) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  GraphBuilder builder;
  ASSERT_DEATH(builder.AddLink({"A", "", kBw, kDelay}), "missing");
  ASSERT_DEATH(builder.AddLink({"", "A", kBw, kDelay}), "missing");
  ASSERT_DEATH(builder.AddLink({"A", "A", kBw, kDelay}), "same");
  ASSERT_DEATH(builder.AddLink({"A", "B", Bandwidth::Zero(), kDelay}), ".*");
  ASSERT_DEATH(builder.AddLink({"A", "B", kBw, Delay::zero()}), ".*");
}

TEST(AddEdgeTest, AddEdge) {
  GraphBuilder builder;
  builder.AddLink({"A", "B", kBw, kDelay});
  ASSERT_EQ(1ul, builder.links().size());

  // By default the builder will auto-assign ports.
  ASSERT_DEATH(
      builder.AddLink({"A", "", kSrcNetPort, kDstNetPort, kBw, kDelay}), ".*");
}

class GraphStorageTest : public ::testing::Test {
 protected:
  GraphStorageTest()
      : link_base_(kSrc, kDst, kSrcNetPort, kDstNetPort, kBw, kDelay) {}

  // Just a random valid link.
  GraphLinkBase link_base_;
};

TEST_F(GraphStorageTest, Init) {
  GraphBuilder builder(false);
  builder.AddLink(link_base_);
  GraphStorage storage(builder);

  const GraphLink* link = storage.LinkPtrOrDie(kSrc, kDst);
  ASSERT_EQ(kSrc, link->src_node()->id());
  ASSERT_EQ(kDst, link->dst_node()->id());
  ASSERT_EQ(kBw, link->bandwidth());
  ASSERT_EQ(kDelay, link->delay());
  ASSERT_EQ(kDstNetPort, link->dst_port());
  ASSERT_EQ(kSrcNetPort, link->src_port());

  ASSERT_EQ(0ul, storage.Stats().multiple_links);
  ASSERT_EQ(1ul, storage.Stats().unidirectional_links);
}

TEST_F(GraphStorageTest, Stats) {
  GraphBuilder builer = GenerateBraess(kBw);
  GraphStorage storage(builer);

  ASSERT_EQ(0ul, storage.Stats().multiple_links);
  ASSERT_EQ(1ul, storage.Stats().unidirectional_links);
}

TEST_F(GraphStorageTest, SameLink) {
  GraphBuilder builder(false);
  builder.AddLink(link_base_);
  GraphStorage storage(builder);

  const GraphLink* link = storage.LinkPtrOrDie(kSrc, kDst);
  const GraphLink* link_two = storage.LinkPtrOrDie(kSrc, kDst);
  ASSERT_EQ(link, link_two);
}

TEST_F(GraphStorageTest, AdjList) {
  GraphBuilder builder(false);
  builder.AddLink(link_base_);
  GraphStorage storage(builder);

  GraphNodeIndex src_index = storage.NodeFromStringOrDie(kSrc);
  GraphNodeIndex dst_index = storage.NodeFromStringOrDie(kDst);
  GraphLinkIndex link_index = storage.LinkOrDie(kSrc, kDst);
  const std::vector<AdjacencyList::LinkInfo>& neighbor_links =
      storage.AdjacencyList().GetNeighbors(src_index);

  ASSERT_EQ(1ul, neighbor_links.size());
  ASSERT_EQ(link_index, neighbor_links[0].link_index);
  ASSERT_FALSE(storage.AdjacencyList().GetNeighbors(src_index).empty());
  ASSERT_TRUE(storage.AdjacencyList().GetNeighbors(dst_index).empty());
}

TEST_F(GraphStorageTest, LinkToString) {
  GraphBuilder builder(false);
  builder.AddLink(link_base_);
  GraphStorage storage(builder);

  const GraphLink* link = storage.LinkPtrOrDie(kSrc, kDst);
  ASSERT_EQ(Substitute("$0:$1->$2:$3", kSrc, kSrcNetPort.Raw(), kDst,
                       kDstNetPort.Raw()),
            link->ToString());
}

TEST_F(GraphStorageTest, FindInverse) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  GraphBuilder builder(false);
  builder.AddLink(link_base_);
  GraphStorage storage_one(builder);

  const GraphLink* link = storage_one.LinkPtrOrDie(kSrc, kDst);
  ASSERT_DEATH(storage_one.FindUniqueInverseOrDie(link), ".*");

  builder.AddLink({kDst, kSrc, kSrcNetPort, kDstNetPort, kBw, kDelay});
  GraphStorage storage_two(builder);

  GraphLinkIndex inverse_link = storage_two.LinkOrDie(kDst, kSrc);
  ASSERT_EQ(inverse_link, storage_two.FindUniqueInverseOrDie(link));

  builder.AddLink({kDst, kSrc, kDstNetPort, kSrcNetPort, kBw, kDelay});
  GraphStorage storage_three(builder);

  ASSERT_DEATH(storage_three.FindUniqueInverseOrDie(link), ".*");
}

TEST_F(GraphStorageTest, Empty) {
  GraphStorage storage((GraphBuilder()));

  Walk walk;
  ASSERT_EQ(0ul, walk.size());
  ASSERT_LT(0ul, walk.InMemBytesEstimate());
  ASSERT_TRUE(walk.empty());
  ASSERT_EQ("[]", walk.ToString(storage));
}

TEST_F(GraphStorageTest, WalkSingleLink) {
  GraphBuilder builder(false);
  builder.AddLink(link_base_);
  GraphStorage storage(builder);

  GraphLinkIndex link_index = storage.LinkOrDie(kSrc, kDst);
  const GraphLink* link = storage.GetLink(link_index);
  Links links = {link_index};

  Walk walk(links, TotalDelayOfLinks(links, storage));
  ASSERT_EQ(1ul, walk.size());
  ASSERT_LT(8ul, walk.InMemBytesEstimate());
  ASSERT_FALSE(walk.empty());
  ASSERT_EQ(Substitute("[$0]", link->ToString()), walk.ToString(storage));
  ASSERT_EQ("[A->B] 20ms", walk.ToStringNoPorts(storage));

  ASSERT_TRUE(walk.IsTrail());
  ASSERT_TRUE(walk.IsPath(storage));
}

TEST_F(GraphStorageTest, WalkDuplicateLink) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  GraphBuilder builder(false);
  builder.AddLink(link_base_);
  GraphStorage storage(builder);

  GraphLinkIndex link = storage.LinkOrDie(kSrc, kDst);
  Links links = {link, link};
  Walk sequence(links, TotalDelayOfLinks(links, storage));
  ASSERT_FALSE(sequence.IsPath(storage));
  ASSERT_FALSE(sequence.IsTrail());
}

class FiveTupleTest : public ::testing::Test {
 protected:
  static void CheckFiveTuple(const FiveTuple& five_tuple) {
    ASSERT_EQ(kSrcIp, five_tuple.ip_src());
    ASSERT_EQ(kDstIp, five_tuple.ip_dst());
    ASSERT_EQ(kProto, five_tuple.ip_proto());
    ASSERT_EQ(kSrcPort, five_tuple.src_port());
    ASSERT_EQ(kDstPort, five_tuple.dst_port());
  }
};

TEST_F(FiveTupleTest, Init) {
  FiveTuple five_tuple(kSrcIp, kDstIp, kProto, kSrcPort, kDstPort);
  CheckFiveTuple(five_tuple);
}

TEST(IPRange, BadRanges) {
  ASSERT_DEATH(IPRange r1("1.2.3.4/sdf/32"), ".*");
  ASSERT_DEATH(IPRange r2("1.2.3.4/32fd"), ".*");
  ASSERT_DEATH(IPRange r3("1.2.3.4/56"), ".*");
  ASSERT_DEATH(IPRange r3("1.asdf2.3.4/56"), ".*");
}

TEST(IPRange, Slash32) {
  IPRange ip_range("1.2.3.4/32");
  ASSERT_EQ(32ul, ip_range.mask_len());
  ASSERT_EQ(StringToIPOrDie("1.2.3.4"), ip_range.base_address());
}

TEST(IPRange, Slash16) {
  IPRange ip_range("1.2.3.4/16");
  ASSERT_EQ(16ul, ip_range.mask_len());
  ASSERT_EQ(StringToIPOrDie("1.2.0.0"), ip_range.base_address());
}

TEST(IPRange, MaskToString) {
  IPRange ip_range("192.176.1.1/12");
  ASSERT_EQ("192.176.0.0/12", ip_range.ToString());
}

TEST(IPRange, Contains) {
  ASSERT_TRUE(IPRange("1.2.0.0/16").Contains(IPRange("1.2.0.0/16")));
  ASSERT_TRUE(IPRange("1.2.0.0/16").Contains(IPRange("1.2.0.0/24")));
  ASSERT_TRUE(IPRange("1.2.0.0/16").Contains(IPRange("1.2.3.0/24")));
  ASSERT_FALSE(IPRange("1.2.0.0/16").Contains(IPRange("1.2.3.0/8")));
  ASSERT_FALSE(IPRange("1.2.0.0/16").Contains(IPRange("1.3.0.0/16")));
  ASSERT_FALSE(IPRange("1.2.0.0/16").Contains(IPRange("1.3.3.0/24")));
  ASSERT_TRUE(IPRange("0.0.0.0/0").Contains(IPRange("10.0.0.0/7")));
}

TEST(IPRange, PrefixLen) {
  ASSERT_EQ(16, IPRange("1.2.0.0/16").PrefixMatchLen(IPRange("1.2.0.0/16")));
  ASSERT_EQ(16, IPRange("1.2.0.0/16").PrefixMatchLen(IPRange("1.2.0.0/24")));
  ASSERT_EQ(8, IPRange("1.2.0.0/8").PrefixMatchLen(IPRange("1.2.0.0/16")));
  ASSERT_EQ(15, IPRange("1.3.0.0/16").PrefixMatchLen(IPRange("1.2.0.0/16")));
  ASSERT_EQ(13, IPRange("1.4.0.0/16").PrefixMatchLen(IPRange("1.2.0.0/16")));
  ASSERT_EQ(
      11, IPRange("192.176.0.0/12").PrefixMatchLen(IPRange("192.168.0.0/16")));
}

TEST(DisjointSets, Empty) {
  std::map<std::string, std::vector<std::string>> key_to_values = {};
  std::vector<std::set<std::string>> out = GetDisjointSets(key_to_values);
  ASSERT_TRUE(out.empty());
}

TEST(DisjointSets, EmptyValues) {
  std::map<std::string, std::vector<uint64_t>> key_to_values = {{"A", {}}};
  ASSERT_DEATH(GetDisjointSets(key_to_values), ".*");
}

TEST(DisjointSets, SingleValue) {
  std::map<std::string, std::vector<uint64_t>> key_to_values = {{"A", {1ul}}};
  std::vector<std::set<std::string>> out = GetDisjointSets(key_to_values);
  std::vector<std::set<std::string>> model = {{"A"}};
  ASSERT_EQ(model, out);
}

TEST(DisjointSets, SingleKey) {
  std::map<std::string, std::vector<uint64_t>> key_to_values = {
      {"A", {1ul, 2ul}}};
  std::vector<std::set<std::string>> out = GetDisjointSets(key_to_values);
  std::vector<std::set<std::string>> model = {{"A"}};
  ASSERT_EQ(model, out);
}

TEST(DisjointSets, MultiKey) {
  std::map<std::string, std::vector<uint64_t>> key_to_values = {
      {"A", {1ul, 2ul}}, {"B", {1ul}}};
  std::vector<std::set<std::string>> out = GetDisjointSets(key_to_values);
  std::vector<std::set<std::string>> model = {{"A", "B"}};
  ASSERT_EQ(model, out);
}

TEST(DisjointSets, MultiKeyDisjoint) {
  std::map<std::string, std::vector<uint64_t>> key_to_values = {
      {"A", {1ul, 2ul}}, {"B", {1ul}}, {"C", {3ul}}};
  std::vector<std::set<std::string>> out = GetDisjointSets(key_to_values);
  std::vector<std::set<std::string>> model = {{"A", "B"}, {"C"}};
  ASSERT_EQ(model, out);
}

TEST(DisjointSets, MultiKeyDisjointTransitive) {
  std::map<std::string, std::vector<uint64_t>> key_to_values = {
      {"A", {1ul, 2ul}}, {"B", {1ul, 3ul}}, {"C", {3ul, 4ul}}, {"D", {5ul}}};
  std::vector<std::set<std::string>> out = GetDisjointSets(key_to_values);
  std::vector<std::set<std::string>> model = {{"A", "B", "C"}, {"D"}};
  ASSERT_EQ(model, out);
}

TEST(DisjointSets, MultiKeyDisjointTransitiveTwo) {
  std::map<std::string, std::vector<uint64_t>> key_to_values = {
      {"A", {1ul, 2ul}},
      {"B", {1ul, 3ul}},
      {"C", {3ul, 4ul}},
      {"D", {5ul, 6ul, 7ul, 8ul}},
      {"E", {5ul, 7ul, 8ul}}};
  std::vector<std::set<std::string>> out = GetDisjointSets(key_to_values);
  std::vector<std::set<std::string>> model = {{"A", "B", "C"}, {"D", "E"}};
  ASSERT_EQ(model, out);
}

class DetourTest : public ::testing::Test {
 protected:
  static GraphBuilder SetUpGraph() {
    GraphBuilder builder;
    builder.AddLink({"A", "B", kBw, kDelay});
    builder.AddLink({"B", "C", kBw, kDelay});
    builder.AddLink({"C", "D", kBw, kDelay});
    builder.AddLink({"D", "E", kBw, kDelay});
    builder.AddLink({"B", "F", kBw, kDelay});
    builder.AddLink({"F", "C", kBw, kDelay});
    builder.AddLink({"F", "G", kBw, kDelay});
    builder.AddLink({"G", "D", kBw, kDelay});
    builder.AddLink({"G", "C", kBw, kDelay});
    builder.AddLink({"A", "F", kBw, kDelay});
    builder.AddLink({"G", "E", kBw, kDelay});

    return builder;
  }

  Links GetLinks(const std::string& str) {
    return storage_.WalkFromStringOrDie(str)->links();
  }

  DetourTest() : storage_(SetUpGraph()) {}
  GraphStorage storage_;
};

TEST_F(DetourTest, NoDetour) {
  std::pair<size_t, size_t> model = {std::numeric_limits<size_t>::max(),
                                     std::numeric_limits<size_t>::max()};
  ASSERT_DEATH(LinksDetour({}, {}), ".*");
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C, C->D]"),
                               GetLinks("[A->B, B->C, C->D]")));
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B]"), GetLinks("[A->B]")));
}

TEST_F(DetourTest, SingleNodeDetour) {
  std::pair<size_t, size_t> model = {1, 2};

  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C, C->D, D->E]"),
                               GetLinks("[A->B, B->F, F->C, C->D, D->E]")));
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C, C->D]"),
                               GetLinks("[A->B, B->F, F->C, C->D]")));
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C]"),
                               GetLinks("[A->B, B->F, F->C]")));
}

TEST_F(DetourTest, MultiNodeDetour) {
  std::pair<size_t, size_t> model = {1, 3};
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C, C->D, D->E]"),
                               GetLinks("[A->B, B->F, F->G, G->D, D->E]")));
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C, C->D]"),
                               GetLinks("[A->B, B->F, F->G, G->D]")));
}

TEST_F(DetourTest, MultiNodeDetourTwo) {
  std::pair<size_t, size_t> model = {1, 3};
  ASSERT_EQ(model,
            LinksDetour(GetLinks("[A->B, B->C, C->D, D->E]"),
                        GetLinks("[A->B, B->F, F->G, G->C, C->D, D->E]")));
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C, C->D]"),
                               GetLinks("[A->B, B->F, F->G, G->C, C->D]")));
}

TEST_F(DetourTest, FullDetour) {
  std::pair<size_t, size_t> model = {0, 2};
  ASSERT_EQ(model, LinksDetour(GetLinks("[A->B, B->C, C->D, D->E]"),
                               GetLinks("[A->F, F->G, G->E]")));
}

}  // namespace
}  // namespace net
}  // namespace nc
