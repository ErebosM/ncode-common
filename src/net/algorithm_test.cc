#include "algorithm.h"

#include <chrono>
#include <initializer_list>
#include <vector>

#include "test_common.h"
#include "gtest/gtest.h"
#include "net_gen.h"

namespace nc {
namespace net {
namespace {

static constexpr Bandwidth kBw = Bandwidth::FromBitsPerSecond(100);

TEST(SimpleGraph, DoubleEdge) {
  GraphBuilder builder;
  builder.AddLink({"A", "B", kBw, Delay(100)});
  builder.AddLink({"A", "B", kBw, Delay(10)});

  GraphStorage graph_storage(builder);
  ASSERT_FALSE(graph_storage.IsSimple());
}

class SingleLink : public ::testing::Test, public test::Base {
 protected:
  static GraphBuilder GetBuilder() {
    GraphBuilder out;
    out.AddLink({"A", "B", kBw, Delay(100)});
    return out;
  }

  SingleLink() : Base(GetBuilder()) {}
};

TEST_F(SingleLink, SubGraphNoExclusion) {
  ConstraintSet constraints;

  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("A"), graph_, constraints));
  ASSERT_EQ(P("[A->B]"),
            ShortestPathWithConstraints(N("A"), N("B"), graph_, constraints)
                ->links());
}

TEST_F(SingleLink, SubGraph) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("A", "B")});

  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("A"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("B"), graph_, constraints));
}

class ThreeEdges : public ::testing::Test, public test::Base {
 protected:
  static GraphBuilder GetBuilder() {
    GraphBuilder out;
    out.AddLink({"A", "B", kBw, Delay(100)});
    out.AddLink({"B", "C", kBw, Delay(100)});
    out.AddLink({"C", "B", kBw, Delay(100)});
    return out;
  }

  ThreeEdges() : Base(GetBuilder()) {}
};

TEST_F(ThreeEdges, ShortestPath) {
  ConstraintSet constraints;

  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("A"), graph_, constraints));
  ASSERT_EQ(P("[A->B, B->C]"),
            ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[C->B]"),
            ShortestPathWithConstraints(N("C"), N("B"), graph_, constraints)
                ->links());
}

TEST_F(ThreeEdges, SubGraphOne) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("A", "B")});

  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("A"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("B"), graph_, constraints));
  ASSERT_EQ(P("[B->C]"),
            ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[C->B]"),
            ShortestPathWithConstraints(N("C"), N("B"), graph_, constraints)
                ->links());
}

TEST_F(ThreeEdges, SubGraphTwo) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("B", "C")});

  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints));
  ASSERT_EQ(P("[A->B]"),
            ShortestPathWithConstraints(N("A"), N("B"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[C->B]"),
            ShortestPathWithConstraints(N("C"), N("B"), graph_, constraints)
                ->links());
}

TEST_F(ThreeEdges, SubGraphThree) {
  ConstraintSet constraints;
  constraints.Exclude().Nodes({N("B")});

  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("C"), N("B"), graph_, constraints));
}

class FourEdges : public ::testing::Test, public test::Base {
 protected:
  static GraphBuilder GetBuilder() {
    GraphBuilder out;
    out.AddLink({"A", "B", kBw, Delay(100)});
    out.AddLink({"B", "C", kBw, Delay(100)});
    out.AddLink({"C", "D", kBw, Delay(100)});
    out.AddLink({"A", "D", kBw, Delay(100)});
    return out;
  }

  FourEdges() : Base(GetBuilder()) {}
};

TEST_F(FourEdges, VisitConstraintsMet) {
  ConstraintSet constraints;
  ASSERT_EQ(0ul, constraints.MinVisit(P("[A->D]"), graph_));

  constraints.AddToVisitSet({N("B")});
  ASSERT_EQ(0ul, constraints.MinVisit(P("[A->D]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B, B->C]"), graph_));

  constraints.AddToVisitSet({N("C")});
  ASSERT_EQ(0ul, constraints.MinVisit(P("[A->D]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B]"), graph_));
  ASSERT_EQ(2ul, constraints.MinVisit(P("[A->B, B->C]"), graph_));

  constraints.AddToVisitSet({N("D")});
  ASSERT_EQ(0ul, constraints.MinVisit({}, graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B]"), graph_));
  ASSERT_EQ(2ul, constraints.MinVisit(P("[A->B, B->C]"), graph_));
  ASSERT_EQ(3ul, constraints.MinVisit(P("[A->B, B->C, C->D]"), graph_));
}

TEST_F(FourEdges, VisitConstraintsMetTwo) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("B"), N("C")});

  ASSERT_EQ(0ul, constraints.MinVisit(P("[A->D]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B, B->C]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[B->C]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B, B->C, C->D]"), graph_));
}

TEST_F(FourEdges, VisitConstraintsMetThree) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("A")});
  constraints.AddToVisitSet({N("C")});

  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->D]"), graph_));
  ASSERT_EQ(1ul, constraints.MinVisit(P("[A->B]"), graph_));
  ASSERT_EQ(2ul, constraints.MinVisit(P("[A->B, B->C]"), graph_));
  ASSERT_EQ(0ul, constraints.MinVisit(P("[B->C]"), graph_));
  ASSERT_EQ(2ul, constraints.MinVisit(P("[A->B, B->C, C->D]"), graph_));
}

TEST_F(FourEdges, WaypointsEmpty) {
  ConstraintSet constraints;
  ASSERT_TRUE(constraints.Waypoints(P("[]"), graph_).empty());
  ASSERT_TRUE(constraints.Waypoints(P("[A->B, B->C, C->D]"), graph_).empty());
}

TEST_F(FourEdges, Waypoints) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("A")});
  constraints.AddToVisitSet({N("C")});

  std::vector<GraphNodeIndex> waypoints =
      constraints.Waypoints(P("[A->B, B->C, C->D]"), graph_);
  std::vector<GraphNodeIndex> model = {N("A"), N("C")};
  ASSERT_EQ(model, waypoints);
}

TEST_F(FourEdges, WaypointsTwo) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("A"), N("C")});
  constraints.AddToVisitSet({N("D")});

  std::vector<GraphNodeIndex> waypoints =
      constraints.Waypoints(P("[A->B, B->C, C->D]"), graph_);
  std::vector<GraphNodeIndex> model_one = {N("A"), N("D")};
  std::vector<GraphNodeIndex> model_two = {N("C"), N("D")};
  ASSERT_TRUE(model_one == waypoints || model_two == waypoints);
}

TEST_F(FourEdges, ShortestPath) {
  ConstraintSet constraints;

  ASSERT_EQ(P("[A->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[B->C]"),
            ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[A->B, B->C]"),
            ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints)
                ->links());
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("C"), N("A"), graph_, constraints));
}

TEST_F(FourEdges, ShortestPathVisitConstraint) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("B")});

  ASSERT_EQ(P("[A->B, B->C, C->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[B->C]"),
            ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints)
                ->links());
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("C"), N("A"), graph_, constraints));
  ASSERT_EQ(P("[A->B, B->C]"),
            ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints)
                ->links());
}

TEST_F(FourEdges, ShortestPathVisitConstraintTwoNodes) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("B"), N("C")});

  ASSERT_EQ(P("[A->B, B->C, C->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[B->C]"),
            ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints)
                ->links());
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("C"), N("A"), graph_, constraints));
  ASSERT_EQ(P("[A->B, B->C]"),
            ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints)
                ->links());
}

TEST_F(FourEdges, ShortestPathVisitConstraintFull) {
  // Any path through the graph is valid.
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("A"), N("B"), N("C"), N("D")});

  ASSERT_EQ(P("[A->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[B->C]"),
            ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[A->B, B->C]"),
            ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints)
                ->links());
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("C"), N("A"), graph_, constraints));
}

TEST_F(FourEdges, ShortestPathVisitConstraintSpecific) {
  // Only this path is valid.
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("A")});
  constraints.AddToVisitSet({N("B")});
  constraints.AddToVisitSet({N("C")});
  constraints.AddToVisitSet({N("D")});

  ASSERT_EQ(P("[A->B, B->C, C->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("D"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("C"), N("A"), graph_, constraints));
}

TEST_F(FourEdges, ShortestPathVisitConstraintSpecificTwo) {
  // Only this path is valid.
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("B")});
  constraints.AddToVisitSet({N("C")});
  constraints.AddToVisitSet({N("D")});

  ASSERT_EQ(P("[A->B, B->C, C->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[B->C, C->D]"),
            ShortestPathWithConstraints(N("B"), N("D"), graph_, constraints)
                ->links());
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("B"), N("C"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints));
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("C"), N("A"), graph_, constraints));
}

TEST_F(FourEdges, ShortestPathVisitConstraintTwoSets) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("A"), N("D")});
  constraints.AddToVisitSet({N("B"), N("C")});

  ASSERT_EQ(P("[A->B, B->C, C->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[A->B, B->C]"),
            ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints)
                ->links());
  ASSERT_EQ(P("[A->B]"),
            ShortestPathWithConstraints(N("A"), N("B"), graph_, constraints)
                ->links());
}

TEST_F(FourEdges, SubGraph) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("A", "D")});

  ASSERT_EQ(P("[A->B, B->C, C->D]"),
            ShortestPathWithConstraints(N("A"), N("D"), graph_, constraints)
                ->links());
}

TEST_F(FourEdges, SubGraphTwo) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("A", "B")});

  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("B"), graph_, constraints));
}

TEST_F(FourEdges, KSPImpossible) {
  ConstraintSet constraints;

  KShortestPathsGenerator ksp(N("D"), N("A"), graph_, constraints);
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(0));
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(1));
}

TEST_F(FourEdges, KSPConstraintImpossible) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("A")});

  KShortestPathsGenerator ksp(N("C"), N("D"), graph_, constraints);
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(0));
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(1));
}

class Ring : public ::testing::Test, public test::Base {
 protected:
  static GraphBuilder GetBuilder() {
    GraphBuilder out;
    out.AddLink({"A", "B", kBw, Delay(100)});
    out.AddLink({"B", "C", kBw, Delay(100)});
    out.AddLink({"C", "D", kBw, Delay(100)});
    out.AddLink({"D", "A", kBw, Delay(100)});
    out.AddLink({"B", "E", kBw, Delay(100)});
    out.AddLink({"C", "E", kBw, Delay(100000)});
    return out;
  }

  Ring() : Base(GetBuilder()) {}
};

TEST_F(Ring, DuplicateLink) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("C")});

  // The shortest way to get to E from A via C would repeat A->B.
  ASSERT_EQ(P("[A->B, B->C, C->D, D->A, A->B, B->E]"),
            ShortestPathWithConstraints(N("A"), N("E"), graph_, constraints)
                ->links());
}

TEST_F(Ring, DuplicateLinkNoAvoid) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("C", "D")});
  constraints.AddToVisitSet({N("C")});

  // The second best path.
  ASSERT_EQ(P("[A->B, B->C, C->E]"),
            ShortestPathWithConstraints(N("A"), N("E"), graph_, constraints)
                ->links());
}

class Braess : public ::testing::Test, public test::Base {
 protected:
  Braess() : Base(GenerateBraess(kBw)) {}
};

TEST_F(Braess, ShortestPathVisitConstraintDstExclude) {
  ConstraintSet constraints;
  constraints.Exclude().Nodes({N("B")});
  constraints.AddToVisitSet({N("D")});

  // The shortest path from A to D is ACD which goes via the destination.
  ASSERT_FALSE(
      ShortestPathWithConstraints(N("A"), N("C"), graph_, constraints));
}

TEST_F(Braess, DFS) {
  ConstraintSet constraints;

  std::vector<std::unique_ptr<Walk>> paths;
  Paths(N("A"), N("D"), [&paths](std::unique_ptr<Walk> path) {
    paths.emplace_back(std::move(path));
  }, graph_, constraints);

  std::sort(paths.begin(), paths.end(), [](const std::unique_ptr<Walk>& lhs,
                                           const std::unique_ptr<Walk>& rhs) {
    return lhs->delay() < rhs->delay();
  });
  ASSERT_EQ(3ul, paths.size());
  ASSERT_EQ(P("[A->C, C->D]"), paths[0]->links());
  ASSERT_EQ(P("[A->B, B->D]"), paths[1]->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), paths[2]->links());
}

TEST_F(Braess, DFSNotSimple) {
  ConstraintSet constraints;

  DFSConfig dfs_config;
  dfs_config.simple = false;

  std::vector<std::unique_ptr<Walk>> paths;
  Paths(N("A"), N("D"), [&paths](std::unique_ptr<Walk> path) {
    paths.emplace_back(std::move(path));
  }, graph_, constraints, dfs_config);

  std::sort(paths.begin(), paths.end(), [](const std::unique_ptr<Walk>& lhs,
                                           const std::unique_ptr<Walk>& rhs) {
    return lhs->delay() < rhs->delay();
  });
  ASSERT_EQ(7ul, paths.size());
  ASSERT_EQ(P("[A->C, C->D]"), paths[0]->links());
  ASSERT_EQ(P("[A->B, B->D]"), paths[1]->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), paths[2]->links());
  ASSERT_EQ(P("[A->C, C->A, A->B, B->D]"), paths[3]->links());
  ASSERT_EQ(P("[A->B, B->C, C->A, A->C, C->D]"), paths[4]->links());
  ASSERT_EQ(P("[A->C, C->A, A->B, B->C, C->D]"), paths[5]->links());
  ASSERT_EQ(P("[A->B, B->A, A->C, C->D]"), paths[6]->links());
}

TEST_F(Braess, DFSConstraint) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("A", "B")});

  std::vector<std::unique_ptr<Walk>> paths;
  Paths(N("A"), N("D"), [&paths](std::unique_ptr<Walk> path) {
    paths.emplace_back(std::move(path));
  }, graph_, constraints);

  ASSERT_EQ(1ul, paths.size());
  ASSERT_EQ(P("[A->C, C->D]"), paths[0]->links());
}

TEST_F(Braess, DFSVisitConstraint) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("C")});

  std::vector<std::unique_ptr<Walk>> paths;
  Paths(N("A"), N("D"), [&paths](std::unique_ptr<Walk> path) {
    paths.emplace_back(std::move(path));
  }, graph_, constraints);

  std::sort(paths.begin(), paths.end(), [](const std::unique_ptr<Walk>& lhs,
                                           const std::unique_ptr<Walk>& rhs) {
    return lhs->delay() < rhs->delay();
  });
  ASSERT_EQ(2ul, paths.size());
  ASSERT_EQ(P("[A->C, C->D]"), paths[0]->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), paths[1]->links());
}

TEST_F(Braess, KSP) {
  ConstraintSet constraints;

  KShortestPathsGenerator ksp(N("A"), N("D"), graph_, constraints);
  ASSERT_EQ(P("[A->C, C->D]"), ksp.KthShortestPathOrNull(0)->links());
  ASSERT_EQ(P("[A->B, B->D]"), ksp.KthShortestPathOrNull(1)->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), ksp.KthShortestPathOrNull(2)->links());
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(3));
}

TEST_F(Braess, KSPConstraint) {
  ConstraintSet constraints;
  constraints.Exclude().Links({L("A", "C")});

  KShortestPathsGenerator ksp(N("A"), N("D"), graph_, constraints);
  ASSERT_EQ(P("[A->B, B->D]"), ksp.KthShortestPathOrNull(0)->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), ksp.KthShortestPathOrNull(1)->links());
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(2));
}

TEST_F(Braess, KSPVistConstraint) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("C")});

  KShortestPathsGenerator ksp(N("A"), N("D"), graph_, constraints);
  ASSERT_EQ(P("[A->C, C->D]"), ksp.KthShortestPathOrNull(0)->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), ksp.KthShortestPathOrNull(1)->links());
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(2));
}

TEST_F(Braess, KSPVisitConstraintNonSimple) {
  ConstraintSet constraints;
  constraints.AddToVisitSet({N("C")});
  constraints.AddToVisitSet({N("B")});

  KShortestPathsGenerator ksp(N("A"), N("D"), graph_, constraints);
  ASSERT_EQ(P("[A->C, C->A, A->B, B->D]"),
            ksp.KthShortestPathOrNull(0)->links());
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(1));
}

TEST_F(Braess, KSPVistConstraintDisjunct) {
  ConstraintSet constraints_one;
  constraints_one.AddToVisitSet({N("C")});

  ConstraintSet constraints_two;
  constraints_two.AddToVisitSet({N("C")});

  // We are OR-ing two identical constraints---the result should be the same as
  // with a single constraint.
  DisjunctKShortestPathsGenerator ksp(N("A"), N("D"), graph_,
                                      {constraints_one, constraints_two});
  ASSERT_EQ(P("[A->C, C->D]"), ksp.KthShortestPathOrNull(0)->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), ksp.KthShortestPathOrNull(1)->links());
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(2));

  size_t gen_index = 1;
  ksp.KthShortestPathOrNull(0, &gen_index);
  ASSERT_EQ(0ul, gen_index);
  ksp.KthShortestPathOrNull(1, &gen_index);
  ASSERT_EQ(0ul, gen_index);
}

TEST_F(Braess, KSPVistConstraintDisjunctTwo) {
  ConstraintSet constraints_one;
  constraints_one.AddToVisitSet({N("C")});

  ConstraintSet constraints_two;
  constraints_two.AddToVisitSet({N("B")});

  // Going through either C or B should yield all possible paths.
  DisjunctKShortestPathsGenerator ksp(N("A"), N("D"), graph_,
                                      {constraints_one, constraints_two});
  ASSERT_EQ(P("[A->C, C->D]"), ksp.KthShortestPathOrNull(0)->links());
  ASSERT_EQ(P("[A->B, B->D]"), ksp.KthShortestPathOrNull(1)->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), ksp.KthShortestPathOrNull(2)->links());
  ASSERT_EQ(nullptr, ksp.KthShortestPathOrNull(3));
}

TEST_F(Braess, WaypointCombineSingle) {
  std::vector<std::vector<GraphNodeIndex>> waypoints = {{N("B"), N("C")}};
  std::vector<GraphNodeIndex> model = {N("B"), N("C")};

  ASSERT_EQ(model, CombineWaypoints(N("A"), N("D"), {}, graph_.AdjacencyList(),
                                    waypoints));
}

TEST_F(Braess, WaypointCombine) {
  std::vector<std::vector<GraphNodeIndex>> waypoints = {{N("C")}, {N("B")}};
  std::vector<GraphNodeIndex> model = {N("B"), N("C")};

  ASSERT_EQ(model, CombineWaypoints(N("A"), N("D"), {}, graph_.AdjacencyList(),
                                    waypoints));
}

TEST_F(Braess, WaypointCombineReverse) {
  std::vector<std::vector<GraphNodeIndex>> waypoints = {{N("B")}, {N("C")}};
  std::vector<GraphNodeIndex> model = {N("B"), N("C")};

  ASSERT_EQ(model, CombineWaypoints(N("A"), N("D"), {}, graph_.AdjacencyList(),
                                    waypoints));
}

}  // namespace
}  // namespace net
}  // namespace ncode
