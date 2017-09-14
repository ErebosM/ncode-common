#ifndef NC_DEMAND_MATRIX_H
#define NC_DEMAND_MATRIX_H

#include <stddef.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "../common.h"
#include "../net/net_common.h"

namespace nc {
namespace lp {

// A single demand--source, destination and traffic volume.
struct DemandMatrixElement {
  DemandMatrixElement(net::GraphNodeIndex src, net::GraphNodeIndex dst,
                      net::Bandwidth demand)
      : src(src), dst(dst), demand(demand) {}

  net::GraphNodeIndex src;
  net::GraphNodeIndex dst;
  net::Bandwidth demand;
};

// A collection of demands.
class DemandMatrix {
 public:
  using NodePair = std::pair<net::GraphNodeIndex, net::GraphNodeIndex>;

  // Loads a TraffixMatrix from a string in the format used by
  // https://bitbucket.org/StevenGay/repetita/src. Will return empty unique
  // pointer if there is a mismatch between the topology and the TM. Will die if
  // there is a parsing error.
  static std::unique_ptr<DemandMatrix> LoadRepetitaOrDie(
      const std::string& matrix_string,
      const std::vector<std::string>& node_names,
      const net::GraphStorage* graph);

  DemandMatrix(const std::vector<DemandMatrixElement>& elements,
               const net::GraphStorage* graph)
      : elements_(elements), graph_(graph) {
    CHECK(graph_ != nullptr);
  }

  DemandMatrix(std::vector<DemandMatrixElement>&& elements,
               const net::GraphStorage* graph)
      : elements_(std::move(elements)), graph_(graph) {
    CHECK(graph_ != nullptr);
  }

  // Changes the graph a demand matrix is tied to.
  DemandMatrix(const DemandMatrix& other, const net::GraphStorage* new_graph)
      : graph_(new_graph) {
    CHECK(graph_ != nullptr);

    const nc::net::GraphStorage* other_graph = other.graph();
    for (const auto& element : other.elements()) {
      const std::string& src_id = other_graph->GetNode(element.src)->id();
      const std::string& dst_id = other_graph->GetNode(element.dst)->id();

      nc::net::GraphNodeIndex new_src = new_graph->NodeFromStringOrDie(src_id);
      nc::net::GraphNodeIndex new_dst = new_graph->NodeFromStringOrDie(dst_id);
      elements_.emplace_back(new_src, new_dst, element.demand);
    }
  }

  // The elements of this demand matrix.
  const std::vector<DemandMatrixElement>& elements() const { return elements_; }

  // Per-link utilization, when everything is routed over the shortest path.
  net::GraphLinkMap<double> SPUtilization() const;

  double SPMaxUtilization() const;

  // Number of links that, when routing over the SP, will be overloaded.
  size_t OverloadedSPLinkCount() const;

  // For each ingress-egress pair returns the number of hops on the SP and the
  // delay of the path.
  std::map<NodePair, std::pair<size_t, net::Delay>> SPStats() const;

  // Returns the load that the ingress sends to the egress.
  net::Bandwidth Load(const NodePair& node_pair) const;

  // Total load for all ingress-egress pairs.
  net::Bandwidth TotalLoad() const;

  // Returns the sum of the load over all links divided by the sum of all links'
  // capacity when all demands are routed over their shortest paths.
  double SPGlobalUtilization() const;

  // Returns <max_flow, scale_factor>, where max flow is the max flow through
  // the network, and scale_factor is a number by which all demands can be
  // multiplied to get to max flow.
  std::pair<net::Bandwidth, double> GetMaxFlow(
      const net::GraphLinkSet& to_exclude) const;

  // True if demand can be routed through the network.
  bool IsFeasible(const net::GraphLinkSet& to_exclude) const;

  // Returns the max commodity scale factor for this matrix.
  double MaxCommodityScaleFractor() const;

  // Returns true if the demand matrix is resilient to any single link failure
  // (the load can still fit).
  bool ResilientToFailures() const;

  bool empty() const { return elements_.empty(); }

  // Returns a demand matrix with the same pairs, but with load of all pairs
  // scaled by 'factor'.
  std::unique_ptr<DemandMatrix> Scale(double factor) const;

  // Returns a demand matrix that contains only the largest demand from this
  // demand matrix.
  std::unique_ptr<DemandMatrix> IsolateLargest() const;

  // Removes a set of demands from the matrix.
  std::unique_ptr<DemandMatrix> RemovePairs(
      const std::set<NodePair>& pairs) const;

  // Prints the matrix.
  std::string ToString() const;

  // Serializes this demand matrix into the format from
  // https://bitbucket.org/StevenGay/repetita/src.
  std::string ToRepetita(const std::vector<std::string>& node_names) const;

  const net::GraphStorage* graph() const { return graph_; }

 private:
  std::vector<DemandMatrixElement> elements_;
  const net::GraphStorage* graph_;

  DISALLOW_COPY_AND_ASSIGN(DemandMatrix);
};

// A class that generates demand matrices based on a set of constraints.
class DemandGenerator {
 public:
  DemandGenerator(uint64_t seed, net::GraphStorage* graph)
      : graph_(graph),
        max_global_utilization_(std::numeric_limits<double>::max()),
        rnd_(seed),
        min_scale_factor_(1.0),
        min_overloaded_link_count_(1) {}

  // Adds a constraint that makes 'fraction' of all links have utilization less
  // than or equal to 'utilization' when demands are routed over their shortest
  // paths.
  void AddUtilizationConstraint(double fraction, double utilization);

  // Adds a constraint that makes 'fraction' of all load go over ie pairs whose
  // shortest paths are hop count or longer.
  void AddHopCountLocalityConstraint(double fraction, size_t hop_count);

  // Adds a constraint that makes 'fraction' of all load go over ie pairs whose
  // shortest paths are distance delay or longer.
  void AddDistanceLocalityConstraint(double fraction,
                                     std::chrono::milliseconds delay);

  // Sets the max global utilization.
  void SetMaxGlobalUtilization(double fraction);

  void SetMinScaleFactor(double factor);

  void SetMinOverloadedLinkCount(size_t link_count);

  // Adds a constraint that makes 'fraction' of all demands use up to
  // 'out_fraction' of their respective total outgoing capacity.
  void AddOutgoingFractionConstraint(double fraction, double out_fraction);

  // Generates a matrix. Will generate num_tries matrices and pick the one with
  // the highest cost. The matrix will be scaled by the scale
  // argument after generation (but this function guarantees that it will return
  // a feasible matrix).
  std::unique_ptr<DemandMatrix> GenerateMatrix(
      size_t num_tries, double scale,
      std::function<double(const DemandMatrix&)> cost_function);

 private:
  // Called by GenerateMatrix to generate a single matrix, if the matrix does
  // not satisfy the global utilization constraint it is discarded and this
  // function is called again.
  std::unique_ptr<DemandMatrix> GenerateMatrixPrivate();

  // The graph.
  net::GraphStorage* graph_;

  // Define the overall distribution of link utilizations.
  using FractionAndUtilization = std::pair<double, double>;
  std::vector<FractionAndUtilization> utilization_constraints_;

  using FractionAndDistance = std::pair<double, std::chrono::milliseconds>;
  std::vector<FractionAndDistance> locality_delay_constraints_;

  using FractionAndHopCount = std::pair<double, size_t>;
  std::vector<FractionAndHopCount> locality_hop_constraints_;

  using FractionAndOutgoingFraction = std::pair<double, double>;
  std::vector<FractionAndOutgoingFraction> outgoing_fraction_constraints_;

  // The sum of all load that crosses all links divided by the sum of all link
  // capacities will be less than this number.
  double max_global_utilization_;

  // Randomness is needed when links are shuffled before solving the problem.
  std::mt19937 rnd_;

  // All demands in the generated matrix should be scaleable by at least this
  // much, while keeping the matrix feasible.
  double min_scale_factor_;

  // Minimum number of links that should be overloaded when routing over the
  // shortest path.
  size_t min_overloaded_link_count_;

  DISALLOW_COPY_AND_ASSIGN(DemandGenerator);
};

}  // namespace lp
}  // namespace nc

#endif
