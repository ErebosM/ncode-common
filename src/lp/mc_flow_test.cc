#include "../net/net_gen.h"
#include "gtest/gtest.h"
#include "mc_flow.h"

namespace nc {
namespace lp {
namespace {

using namespace std::chrono;

static constexpr net::Bandwidth kBw1 = net::Bandwidth::FromBitsPerSecond(10000);
static constexpr net::Bandwidth kBw2 =
    net::Bandwidth::FromBitsPerSecond(10000000000);

static std::unique_ptr<net::Walk> GetPath(
    const std::string& path, const net::GraphStorage& graph_storage) {
  return graph_storage.WalkFromStringOrDie(path);
}

// Shorthand for net::Bandwidth::FromBitsPerSecond(x)
static net::Bandwidth BW(uint64_t x) {
  return net::Bandwidth::FromBitsPerSecond(x);
}

static SrcAndDst SD(const std::string& src, const std::string& dst,
                    const net::GraphStorage& graph_storage) {
  return {graph_storage.NodeFromStringOrDie(src),
          graph_storage.NodeFromStringOrDie(dst)};
}

static net::GraphLinkMap<double> GetCapacities(
    const net::GraphStorage& graph, const net::GraphLinkSet& to_exclude = {}) {
  net::GraphLinkMap<double> out;
  for (net::GraphLinkIndex link : graph.AllLinks()) {
    if (to_exclude.Contains(link)) {
      continue;
    }

    double capacity = graph.GetLink(link)->bandwidth().Mbps();
    out[link] = capacity;
  }

  return out;
}

TEST(MCTest, UnidirectionalLink) {
  net::GraphBuilder builder = net::GenerateFullGraph(2, kBw1, microseconds(10));
  builder.AddLink({"N1", "N2", kBw1, std::chrono::milliseconds(100)});

  net::GraphStorage graph(builder);
  MaxFlowSingleCommodityFlowProblem flow_problem(GetCapacities(graph), &graph);
  flow_problem.AddDemand("N0", "N2");

  double max_flow = 0;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(kBw1.Mbps(), max_flow);

  // The path should be N0->N1->N2
  std::map<SrcAndDst, std::vector<FlowAndPath>> model_paths;
  std::vector<FlowAndPath>& fp = model_paths[SD("N0", "N2", graph)];
  fp.emplace_back(kBw1.Mbps(), GetPath("[N0->N1, N1->N2]", graph));

  std::map<SrcAndDst, std::vector<FlowAndPath>> paths;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow, &paths));
  ASSERT_EQ(kBw1.Mbps(), max_flow);
  ASSERT_EQ(model_paths, paths);
}

TEST(MCTest, Simple) {
  net::GraphBuilder builder = net::GenerateFullGraph(2, kBw1, microseconds(10));
  net::GraphStorage graph(builder);

  double max_flow = 0;
  MaxFlowSingleCommodityFlowProblem flow_problem(GetCapacities(graph), &graph);
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(0, max_flow);

  flow_problem.AddDemand("N0", "N1");
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(kBw1.Mbps(), max_flow);

  std::map<SrcAndDst, std::vector<FlowAndPath>> model_paths;
  std::vector<FlowAndPath>& fp = model_paths[SD("N0", "N1", graph)];
  fp.emplace_back(kBw1.Mbps(), GetPath("[N0->N1]", graph));

  std::map<SrcAndDst, std::vector<FlowAndPath>> paths;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow, &paths));
  ASSERT_EQ(kBw1.Mbps(), max_flow);
  ASSERT_EQ(model_paths, paths);
}

TEST(MCTest, SimpleTwoCommodities) {
  net::GraphBuilder builder = net::GenerateFullGraph(2, kBw1, microseconds(10));
  net::GraphStorage graph(builder);

  MaxFlowSingleCommodityFlowProblem flow_problem(GetCapacities(graph), &graph);
  flow_problem.AddDemand("N0", "N1");
  flow_problem.AddDemand("N1", "N0");

  double max_flow = 0;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(2 * kBw1.Mbps(), max_flow);

  std::map<SrcAndDst, std::vector<FlowAndPath>> model_paths;
  std::vector<FlowAndPath>& fp_one = model_paths[SD("N1", "N0", graph)];
  fp_one.emplace_back(kBw1.Mbps(), GetPath("[N1->N0]", graph));

  std::vector<FlowAndPath>& fp_two = model_paths[SD("N0", "N1", graph)];
  fp_two.emplace_back(kBw1.Mbps(), GetPath("[N0->N1]", graph));

  std::map<SrcAndDst, std::vector<FlowAndPath>> paths;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow, &paths));
  ASSERT_EQ(2 * kBw1.Mbps(), max_flow);
  ASSERT_EQ(model_paths, paths);
}

TEST(MCTest, Triangle) {
  net::GraphBuilder builder = net::GenerateFullGraph(3, kBw1, microseconds(10));
  net::GraphStorage graph(builder);

  MaxFlowSingleCommodityFlowProblem flow_problem(GetCapacities(graph), &graph);
  flow_problem.AddDemand("N0", "N2");

  double max_flow = 0;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(2 * kBw1.Mbps(), max_flow);

  std::map<SrcAndDst, std::vector<FlowAndPath>> model_paths;
  std::vector<FlowAndPath>& fp = model_paths[SD("N0", "N2", graph)];
  fp.emplace_back(kBw1.Mbps(), GetPath("[N0->N2]", graph));
  fp.emplace_back(kBw1.Mbps(), GetPath("[N0->N1, N1->N2]", graph));

  std::map<SrcAndDst, std::vector<FlowAndPath>> paths;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow, &paths));
  ASSERT_EQ(2 * kBw1.Mbps(), max_flow);

  for (auto& id_and_paths : paths) {
    std::vector<FlowAndPath>& paths_for_id = id_and_paths.second;
    std::sort(paths_for_id.begin(), paths_for_id.end(),
              [](const FlowAndPath& lhs, const FlowAndPath& rhs) {
                return lhs.path().delay() < rhs.path().delay();
              });
  }
  ASSERT_EQ(model_paths, paths);

  flow_problem.AddDemand("N1", "N2");
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(2 * kBw1.Mbps(), max_flow);
}

TEST(MCTest, TriangleSameDest) {
  net::GraphBuilder builder = net::GenerateFullGraph(3, kBw1, microseconds(10));
  net::GraphStorage graph(builder);

  MaxFlowSingleCommodityFlowProblem flow_problem(GetCapacities(graph), &graph);
  flow_problem.AddDemand("N0", "N2");
  flow_problem.AddDemand("N1", "N2");

  double max_flow = 0;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(2 * kBw1.Mbps(), max_flow);

  std::map<SrcAndDst, std::vector<FlowAndPath>> model_paths;
  std::vector<FlowAndPath>& fp_one = model_paths[SD("N0", "N2", graph)];
  fp_one.emplace_back(kBw1.Mbps(), GetPath("[N0->N2]", graph));

  std::vector<FlowAndPath>& fp_two = model_paths[SD("N1", "N2", graph)];
  fp_two.emplace_back(kBw1.Mbps(), GetPath("[N1->N2]", graph));

  std::map<SrcAndDst, std::vector<FlowAndPath>> paths;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow, &paths));
  ASSERT_EQ(2 * kBw1.Mbps(), max_flow);
  ASSERT_EQ(model_paths, paths);
}

TEST(MCTest, TriangleOneExclude) {
  net::GraphBuilder builder = net::GenerateFullGraph(3, kBw1, microseconds(10));
  net::GraphStorage graph(builder);
  net::GraphLinkIndex l1 = graph.LinkOrDie("N0", "N2");
  net::GraphLinkIndex l2 = graph.LinkOrDie("N2", "N0");

  MaxFlowSingleCommodityFlowProblem flow_problem(GetCapacities(graph, {l1, l2}),
                                                 &graph);
  flow_problem.AddDemand("N0", "N2");

  double max_flow = 0;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow));
  ASSERT_EQ(kBw1.Mbps(), max_flow);

  std::map<SrcAndDst, std::vector<FlowAndPath>> model_paths;
  std::vector<FlowAndPath>& fp_one = model_paths[SD("N0", "N2", graph)];
  fp_one.emplace_back(kBw1.Mbps(), GetPath("[N0->N1, N1->N2]", graph));

  std::map<SrcAndDst, std::vector<FlowAndPath>> paths;
  ASSERT_TRUE(flow_problem.GetMaxFlow(&max_flow, &paths));
  ASSERT_EQ(model_paths, paths);
}

TEST(MCTest, TriangleNoFit) {
  net::GraphBuilder builder = net::GenerateFullGraph(3, kBw1, microseconds(10));
  net::GraphStorage graph(builder);

  MaxFlowSingleCommodityFlowProblem flow_problem(GetCapacities(graph), &graph);
  flow_problem.AddDemand("N0", "N2", kBw1.Mbps() * 3);

  double max_flow;
  ASSERT_FALSE(flow_problem.GetMaxFlow(&max_flow));
}

TEST(MCTest, SimpleFeasible) {
  net::GraphBuilder builder = net::GenerateFullGraph(2, kBw1, microseconds(10));
  net::GraphStorage graph(builder);

  SingleCommodityFlowProblem problem(GetCapacities(graph), &graph);
  ASSERT_TRUE(problem.IsFeasible());

  problem.AddDemand("N0", "N1", BW(10000).Mbps());
  ASSERT_TRUE(problem.IsFeasible());

  // The optimizer optimizes in Mbps, the default CPLEX feasibility tolerance is
  // not low enough to detect a 1 bit infeasibility (BW(10001) will not work).
  problem.AddDemand("N1", "N0", BW(11000).Mbps());
  ASSERT_FALSE(problem.IsFeasible());
}

TEST(MCTest, SimpleScaleFactor) {
  net::GraphBuilder builder = net::GenerateFullGraph(2, kBw2, microseconds(10));
  net::GraphStorage graph(builder);

  SingleCommodityFlowProblem problem(GetCapacities(graph), &graph);
  ASSERT_EQ(0, problem.MaxDemandScaleFactor());

  problem.AddDemand("N0", "N1");
  ASSERT_EQ(0, problem.MaxDemandScaleFactor());

  problem.AddDemand("N1", "N0", BW(8000).Mbps());
  ASSERT_NEAR(1250000, problem.MaxDemandScaleFactor(), 0.1);
}

TEST(MCTest, SimpleIncrement) {
  net::GraphBuilder builder = net::GenerateFullGraph(2, kBw2, microseconds(10));
  net::GraphStorage graph(builder);

  SingleCommodityFlowProblem problem(GetCapacities(graph), &graph);
  ASSERT_EQ(0, problem.MaxDemandIncrement());

  problem.AddDemand("N0", "N1");
  ASSERT_NEAR(10000, problem.MaxDemandIncrement(), 10);
}

}  // namespace
}  // namespace lp
}  // namespace ncode
