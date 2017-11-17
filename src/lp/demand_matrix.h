#ifndef NC_DEMAND_MATRIX_H
#define NC_DEMAND_MATRIX_H

#include <stddef.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../common.h"
#include "../logging.h"
#include "../net/algorithm.h"
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

  // Returns true if the demand matrix can be satisfied by routing each demand
  // on its shortest path.
  bool IsTriviallySatisfiable() const;

  // Returns <max_flow, scale_factor>, where max flow is the max flow through
  // the network, and scale_factor is a number by which all demands can be
  // multiplied to get to max flow.
  std::pair<net::Bandwidth, double> GetMaxFlow(
      const net::GraphLinkSet& to_exclude, double capacity_multiplier) const;

  // True if demand can be routed through the network.
  bool IsFeasible(const net::GraphLinkSet& to_exclude,
                  double capacity_multiplier) const;

  // Returns the max commodity scale factor for this matrix.
  double MaxCommodityScaleFractor(const net::GraphLinkSet& to_exclude,
                                  double capacity_multiplier) const;

  // Returns true if the demand matrix is resilient to any single link failure
  // (the load can still fit).
  bool ResilientToFailures() const;

  bool empty() const { return elements_.empty(); }

  // Returns a demand matrix with the same pairs, but with load of all pairs
  // scaled by 'factor'.
  std::unique_ptr<DemandMatrix> Scale(double factor) const;

  // Removes all aggregates for which a filter function returns true.
  std::unique_ptr<DemandMatrix> Filter(
      std::function<bool(const DemandMatrixElement& element)> filter) const;

  // Returns a demand matrix that contains the top N demands that are
  // responsible for 'fraction' of the total demand.
  std::unique_ptr<DemandMatrix> IsolateFraction(double fraction) const;

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

}  // namespace lp
}  // namespace nc

#endif
