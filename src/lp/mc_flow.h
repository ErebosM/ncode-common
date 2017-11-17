#ifndef NCODE_MC_FLOW_H
#define NCODE_MC_FLOW_H

#include <cstdint>
#include <iostream>
#include <vector>

#include "../net/net_common.h"
#include "lp.h"

namespace nc {
namespace lp {

// Path and flow on a path.
class FlowAndPath {
 public:
  FlowAndPath(double flow, std::unique_ptr<net::Walk> path)
      : flow_(flow), path_(std::move(path)) {}

  double flow() const { return flow_; }

  const net::Walk& path() const { return *path_; }

  std::unique_ptr<net::Walk> TakeOwnershipOfPath() { return std::move(path_); }

  friend bool operator==(const FlowAndPath& lhs, const FlowAndPath& rhs) {
    return std::tie(lhs.flow_, *lhs.path_) == std::tie(rhs.flow_, *rhs.path_);
  }

 private:
  double flow_;
  std::unique_ptr<net::Walk> path_;
};

// A source node and flow out of that node.
using SrcAndLoad = std::pair<net::GraphNodeIndex, double>;

// Source and destination nodes.
using SrcAndDst = std::pair<net::GraphNodeIndex, net::GraphNodeIndex>;

class FlowProblem {
 public:
  // For each node will keep a list of the edges going out of the node and the
  // edges coming into the node.
  using AdjacencyMap =
      net::GraphNodeMap<std::pair<std::vector<net::GraphLinkIndex>,
                                  std::vector<net::GraphLinkIndex>>>;

  FlowProblem(const net::GraphLinkMap<double>& link_capacities,
              const net::GraphStorage* graph);

  const net::GraphLinkMap<double>& link_capacities() const {
    return link_capacities_;
  }

  const AdjacencyMap& adjacency_map() const { return adjacency_map_; }

 protected:
  // Where the graph is stored. Not owned by this object.
  const net::GraphStorage* graph_;

 private:
  // Per-link capacities.
  net::GraphLinkMap<double> link_capacities_;

  AdjacencyMap adjacency_map_;
};

// A single-commodity flow problem. Edge capacities will be taken from the
// bandwidth values of the links in the graph this object is constructed with
// times a multiplier.
class SingleCommodityFlowProblem : public FlowProblem {
 public:
  // For each link a map from destination node index to LP variable.
  using VarMap = net::GraphLinkMap<net::GraphNodeMap<VariableIndex>>;

  SingleCommodityFlowProblem(const net::GraphLinkMap<double>& link_capacities,
                             const net::GraphStorage* graph);

  // Adds demand to the network, with a given source and sink. If
  // the demand is not specified it is assumed to be infinite.
  void AddDemand(const std::string& source, const std::string& sink,
                 double demand = 0);
  void AddDemand(net::GraphNodeIndex source, net::GraphNodeIndex sink,
                 double demand = 0);

  // Returns true if the MC problem is feasible -- if the commodities/demands
  // can fit in the network.
  bool IsFeasible();

  // If all commodities' demands are multiplied by the returned number the
  // problem will be close to being infeasible. Returns 0 if the problem is
  // currently infeasible or all commodities have 0 demands.
  double MaxDemandScaleFactor();

  // If the returned demand is added to all commodities the problem will be very
  // close to being infeasible.
  double MaxDemandIncrement();

 protected:
  // Returns a map from a graph link to a list of one variable per demand
  // destination.
  VarMap GetLinkToVariableMap(Problem* problem,
                              std::vector<ProblemMatrixElement>* problem_matrix,
                              bool add_link_constraints);

  // Adds flow conservation constraints to the problem.
  void AddFlowConservationConstraints(
      const VarMap& link_to_variables, Problem* problem,
      std::vector<ProblemMatrixElement>* problem_matrix);

  // Recovers the paths from a solution by constructing a
  // SingleCommoditySingleSinkFlowProblem for each destination.
  std::map<SrcAndDst, std::vector<FlowAndPath>> RecoverPathsFromSolution(
      const VarMap& link_to_variables, const lp::Solution& solution);

  // The demands, grouped by destination.
  net::GraphNodeMap<std::vector<SrcAndLoad>> demands_;

 private:
  // Returns the same problem, but with all demands multiplied by
  // the given scale factor and increased by 'increment'.
  SingleCommodityFlowProblem(const SingleCommodityFlowProblem& other,
                             double scale_factor, double increment);

  DISALLOW_COPY_AND_ASSIGN(SingleCommodityFlowProblem);
};

class SingleCommoditySingleSinkFlowProblem : public FlowProblem {
 public:
  SingleCommoditySingleSinkFlowProblem(
      const net::GraphLinkMap<double>& link_capacities,
      net::GraphNodeIndex sink, const net::GraphStorage* graph)
      : FlowProblem(link_capacities, graph), sink_(sink) {}

  void AddDemand(const std::string& source, double demand);
  void AddDemand(net::GraphNodeIndex source, double demand);

  // Solves the problem and returns the shortest paths (in terms of total delay)
  // that satisfy the demands. Returned grouped by source.
  net::GraphNodeMap<std::vector<FlowAndPath>> GetShortestPaths();

 private:
  // For each link a map from source node index to LP variable.
  using VarMap = net::GraphLinkMap<net::GraphNodeMap<VariableIndex>>;

  // Returns a map from a graph link to a list of one variable per source.
  VarMap GetLinkToVariableMap(
      Problem* problem, std::vector<ProblemMatrixElement>* problem_matrix);

  // Adds flow conservation constraints to the problem.
  void AddFlowConservationConstraints(
      const VarMap& link_to_variables, Problem* problem,
      std::vector<ProblemMatrixElement>* problem_matrix);

  // Recovers the paths from the flow problem. Returns for each demand the
  // paths and fractions of demand over each path.
  net::GraphNodeMap<std::vector<FlowAndPath>> RecoverPaths(
      const VarMap& link_to_variables, const lp::Solution& solution) const;

  // Helper function for RecoverPaths.
  double RecoverPathsRecursive(double demand, net::GraphNodeIndex at_node,
                               double overall_flow,
                               net::GraphLinkMap<double>* flow_over_links,
                               net::Links* links_so_far,
                               net::GraphNodeSet* nodes_so_far_set,
                               std::vector<FlowAndPath>* out) const;

  // The sink.
  net::GraphNodeIndex sink_;

  // The demands, grouped by source.
  net::GraphNodeMap<double> demands_;

  DISALLOW_COPY_AND_ASSIGN(SingleCommoditySingleSinkFlowProblem);
};

// Solves the multi-commodity max flow problem.
class MaxFlowSingleCommodityFlowProblem : public SingleCommodityFlowProblem {
 public:
  MaxFlowSingleCommodityFlowProblem(
      const net::GraphLinkMap<double>& link_capacities,
      const net::GraphStorage* graph);

  // Populates the maximum flow (in the same units as edge bandwidth *
  // cpacity_multiplier_) for all commodities. If 'paths' is supplied will also
  // populate it with the actual paths for each commodity that will result in
  // the max flow value. If there are commodities that cannot satisfy their
  // demands false is returned and neither 'max_flow' nor 'paths' are modified.
  bool GetMaxFlow(
      double* max_flow,
      std::map<SrcAndDst, std::vector<FlowAndPath>>* paths = nullptr);
};

// Minimizes the maximum link utilization.
class MinMaxProblem : public SingleCommodityFlowProblem {
 public:
  MinMaxProblem(const nc::net::GraphStorage* graph,
                const net::GraphLinkMap<double>& link_capacities,
                bool also_minimize_delay)
      : SingleCommodityFlowProblem(link_capacities, graph),
        also_minimize_delay_(also_minimize_delay) {}

  double Solve(std::map<SrcAndDst, std::vector<FlowAndPath>>* paths = nullptr);

  bool also_minimize_delay_;
};

}  // namespace lp
}  // namespace ncode
#endif
