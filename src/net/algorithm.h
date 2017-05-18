#ifndef NCODE_NET_ALGO_H
#define NCODE_NET_ALGO_H

#include <stdint.h>
#include <limits>
#include <queue>

#include "net_common.h"
#include "trie.h"

namespace nc {
namespace net {

// Nodes/links that should be excluded.
class ExclusionSet {
 public:
  // Adds a new set of links to be excluded.
  void Links(const GraphLinkSet& set) { links_to_exclude_.InsertAll(set); }

  // Adds a new set of nodes to be excluded.
  void Nodes(const GraphNodeSet& set) { nodes_to_exclude_.InsertAll(set); }

  // Returns true if the link should be excluded.
  bool ShouldExcludeLink(const GraphLinkIndex link) const;

  // Returns true if the node should be excluded.
  bool ShouldExcludeNode(const GraphNodeIndex node) const;

  const GraphLinkSet& links_to_exclude() const { return links_to_exclude_; }

  const GraphNodeSet& nodes_to_exclude() const { return nodes_to_exclude_; }

  std::string ToString(const GraphStorage& storage) const {
    return Substitute("exclude links: $0, exclude nodes: $1",
                      GraphLinkSetToString(links_to_exclude_, storage),
                      GraphNodeSetToString(nodes_to_exclude_, storage));
  }

  // Adds all elements from another set to this one.
  void AddAll(const ExclusionSet& other);

  friend bool operator==(const ExclusionSet& lhs, const ExclusionSet& rhs) {
    return lhs.links_to_exclude_ == rhs.links_to_exclude_ &&
           lhs.nodes_to_exclude_ == rhs.nodes_to_exclude_;
  }

  friend bool operator<(const ExclusionSet& lhs, const ExclusionSet& rhs) {
    return std::tie(lhs.links_to_exclude_, lhs.nodes_to_exclude_) <
           std::tie(rhs.links_to_exclude_, rhs.nodes_to_exclude_);
  }

 private:
  // Links/nodes that will be excluded from the graph.
  GraphLinkSet links_to_exclude_;
  GraphNodeSet nodes_to_exclude_;
};

// A set of constraints.
class ConstraintSet {
 public:
  ConstraintSet(const ExclusionSet& exclusion_set,
                const std::vector<GraphNodeSet>& to_visit)
      : exclusion_set_(exclusion_set) {
    for (const GraphNodeSet& set_to_visit : to_visit) {
      AddToVisitSet(set_to_visit);
    }
  }

  ConstraintSet() {}

  // The exclusion set.
  ExclusionSet& Exclude() { return exclusion_set_; }

  const ExclusionSet& exclusion_set() const { return exclusion_set_; }

  // Adds a new set of nodes to be visited.
  void AddToVisitSet(const GraphNodeSet& set);

  // Adds a set of nodes to the first set of nodes to be visited. If there is no
  // first set of nodes to be visited, or if there are multiple sets of nodes to
  // be visited will die.
  void ExtendFirstSetToVisitOrDie(const GraphNodeSet& set);

  // If the links partially satisfy the visit constraints will return the index
  // of the last constraint that the links satisfy + 1. If the links fully
  // satisfy the constraints will return to_visit().size(). If the links do not
  // satisfy the constraints, or there are no visit constraints will return 0.
  // Assumes that the links form a path.
  size_t MinVisit(const Links& links, const GraphStorage& graph_storage) const;

  // For a compliant walk this will return a single node from each set that the
  // path visits. Those are the nodes that make the path compliant. Any path
  // that visits those nodes in the same order will also be compliant.
  std::vector<GraphNodeIndex> Waypoints(
      const Links& links, const GraphStorage& graph_storage) const;

  // If this constraint says that a set of nodes should be visited returns the
  // set a node is in. Null otherwise.
  const GraphNodeSet* FindSetToVisitOrNull(const GraphNodeIndex node) const;

  bool ShouldExcludeLink(const GraphLinkIndex link) const {
    return exclusion_set_.ShouldExcludeLink(link);
  }

  bool ShouldExcludeNode(const GraphNodeIndex node) const {
    return exclusion_set_.ShouldExcludeNode(node);
  }

  // Sets of nodes to visit, in the order given.
  const std::vector<GraphNodeSet>& to_visit() const { return to_visit_; }

  // Will make sure that the constrains do not have 'src' in the first set to
  // visit and 'dst' in the last set to visit.
  ConstraintSet SanitizeConstraints(GraphNodeIndex src,
                                    GraphNodeIndex dst) const;

  std::string ToString(const GraphStorage& storage) const;

  friend bool operator==(const ConstraintSet& lhs, const ConstraintSet& rhs) {
    return lhs.exclusion_set_ == rhs.exclusion_set_ &&
           lhs.to_visit_ == rhs.to_visit_;
  }

  friend bool operator<(const ConstraintSet& lhs, const ConstraintSet& rhs) {
    return std::tie(lhs.exclusion_set_, lhs.to_visit_) <
           std::tie(rhs.exclusion_set_, rhs.to_visit_);
  }

 private:
  // Links/nodes that will be excluded from the graph.
  ExclusionSet exclusion_set_;

  // Sets of nodes to visit.
  std::vector<GraphNodeSet> to_visit_;

  // Maps from a node index to the index of the node's set in to_visit_.
  // Building this map also helps figure out if each node is in at most one set.
  GraphNodeMap<size_t> node_to_visit_index_;
};

// Single source shortest path tree from a source to a set of nodes.
class ShortestPath {
 public:
  ShortestPath(GraphNodeIndex src, const GraphNodeSet& dst_nodes,
               const ExclusionSet& exclusion_set, const AdjacencyList& adj_list,
               const GraphNodeSet* additional_nodes_to_avoid = nullptr,
               const GraphLinkSet* additional_links_to_avoid = nullptr)
      : src_(src), destinations_(dst_nodes) {
    ComputePaths(exclusion_set, adj_list, additional_nodes_to_avoid,
                 additional_links_to_avoid);
  }

  // Returns the shortest path to the destination.
  std::unique_ptr<Walk> GetPath(GraphNodeIndex dst) const;

  // Returns the distance from the source to a destination.
  Delay GetPathDistance(GraphNodeIndex dst) const;

  // Returns the nodes and  the links that are part of this tree.
  std::pair<GraphNodeSet, GraphLinkSet> ElementsInTree() const;

  friend bool operator<(const ShortestPath& a, const ShortestPath& b) {
    return std::tie(a.src_, a.previous_, a.min_delays_, a.destinations_) <
           std::tie(b.src_, b.previous_, b.min_delays_, b.destinations_);
  }

  friend bool operator==(const ShortestPath& a, const ShortestPath& b) {
    return std::tie(a.src_, a.previous_, a.min_delays_, a.destinations_) ==
           std::tie(b.src_, b.previous_, b.min_delays_, b.destinations_);
  }

 private:
  struct DistanceFromSource {
    DistanceFromSource() : distance(Delay::max()) {}
    Delay distance;

    friend bool operator<(const DistanceFromSource& a,
                          const DistanceFromSource& b) {
      return a.distance < b.distance;
    }

    friend bool operator==(const DistanceFromSource& a,
                           const DistanceFromSource& b) {
      return a.distance == b.distance;
    }
  };

  void ComputePaths(const ExclusionSet& exclusion_set,
                    const AdjacencyList& adj_list,
                    const GraphNodeSet* additional_nodes_to_avoid,
                    const GraphLinkSet* additional_links_to_avoid);

  // The source.
  GraphNodeIndex src_;

  // For each node, the link that leads to it in the SP tree.
  GraphNodeMap<const AdjacencyList::LinkInfo*> previous_;

  // Delays from the source to each destination node.
  GraphNodeMap<DistanceFromSource> min_delays_;

  // The destinations.
  GraphNodeSet destinations_;
};

// Computes shortest paths between all pairs of nodes, can also be used to
// figure out if the graph is partitioned.
class AllPairShortestPath {
 public:
  AllPairShortestPath(const ExclusionSet& exclusion_set,
                      const AdjacencyList& adj_list,
                      const GraphNodeSet* additional_nodes_to_avoid,
                      const GraphLinkSet* additional_links_to_avoid) {
    ComputePaths(exclusion_set, adj_list, additional_nodes_to_avoid,
                 additional_links_to_avoid);
  }

  // Returns the shortest path between src and dst.
  std::unique_ptr<Walk> GetPath(GraphNodeIndex src, GraphNodeIndex dst) const;

  // Returns the length of the shortest path between src and dst.
  Delay GetDistance(GraphNodeIndex src, GraphNodeIndex dst) const;

 private:
  struct SPData {
    SPData() : distance(Delay::max()) {}

    // Distance between the 2 endpoints.
    Delay distance;

    // Successor in the SP.
    GraphLinkIndex next_link;
    GraphNodeIndex next_node;
  };

  // Populates data_.
  void ComputePaths(const ExclusionSet& exclusion_set,
                    const AdjacencyList& adj_list,
                    const GraphNodeSet* additional_nodes_to_avoid,
                    const GraphLinkSet* additional_links_to_avoid);

  // Distances to the destination.
  GraphNodeMap<GraphNodeMap<SPData>> data_;
};

// Each node can belong to one of up to 64 groups. By assigning nodes to groups,
// constraints can be later specified for paths with respect to groups (e.g,
// paths should always cross a member of a group of nodes).
struct NodeGroupTag {};
using NodeGroup = TypesafeUintWrapper<NodeGroupTag, uint8_t>;

// Configuration for a DFS.
struct DFSConfig {
  bool simple = true;  // Whether or not to only consider simple paths.
  Delay max_distance = Delay::max();
  size_t max_hops = std::numeric_limits<size_t>::max();
};

// Calls a callback with all paths between a source and a destination.
void Paths(GraphNodeIndex src, GraphNodeIndex dst,
           std::function<void(std::unique_ptr<Walk>)> path_callback,
           const GraphStorage& graph, const ConstraintSet& constraints,
           const DFSConfig& dfs_config = {});

// The set of nodes that are reachable from a given node.
GraphNodeSet ReachableNodes(GraphNodeIndex src, const GraphStorage& graph,
                            const ExclusionSet& exclusion_set);

// The shortest path between two nodes, subject to constraints.
std::unique_ptr<Walk> ShortestPathWithConstraints(
    GraphNodeIndex src, GraphNodeIndex dst, const GraphStorage& graph,
    const ConstraintSet& constraints);

struct KShortestPathGeneratorStats {
  // Number of shortest paths kept in memory.
  size_t k = 0;

  // The number of bytes occupied by the K paths.
  size_t paths_size_bytes = 0;

  // Stats of the trie that lets the KSP algorithm do quick path prefix lookups.
  TrieStats trie_stats;

  // Number of candidate paths.
  size_t candidate_count = 0;

  // Total memory occupied by this KSP generator, includes the list of k paths,
  // the candidates, and the trie.
  size_t total_size_bytes = 0;

  // Delay of k=0
  Delay min_path_delay = Delay::zero();

  // Delay of kth path.
  Delay max_path_delay = Delay::zero();

  // Delay of the longest candidate path. The candidate paths are *not*
  // exhaustive.
  Delay max_candidate_path_delay = Delay::zero();

  std::string ToString() {
    return Substitute(
        "k: $0 ($1 bytes), trie: $2, candidates: $3, total: $4 bytes, k=0 "
        "delay: $5μs, k=k delay: $6μs, max candidate delay: $7μs",
        k, paths_size_bytes, trie_stats.ToString(), candidate_count,
        total_size_bytes, min_path_delay.count(), max_path_delay.count(),
        max_candidate_path_delay.count());
  }
};

// Generates shortest paths in increasing order.
class KShortestPathsGenerator {
 public:
  KShortestPathsGenerator(GraphNodeIndex src, GraphNodeIndex dst,
                          const GraphStorage& graph,
                          const ConstraintSet& constraints)
      : src_(src),
        dst_(dst),
        constraints_(constraints.SanitizeConstraints(src, dst)),
        storage_(&graph) {}

  // Returns the Kth shortest path. The path is owned by this object.
  const Walk* KthShortestPathOrNull(size_t k);

  // Returns the status of the path generator.
  KShortestPathGeneratorStats GetStats() const;

  size_t k() const { return k_paths_.size(); }

  // Returns the shortest path that complies with the constraints and avoids a
  // set of nodes/links.
  std::unique_ptr<Walk> ShortestPathThatAvoids(
      const GraphNodeSet& nodes_to_avoid,
      const GraphLinkSet& links_to_avoid) const;

  const GraphStorage* graph() const { return storage_; }

  const ConstraintSet& constraints() const { return constraints_; }

 private:
  using PathAndStartIndex = std::pair<std::unique_ptr<Walk>, size_t>;
  struct PathAndStartIndexComparator {
    bool operator()(const PathAndStartIndex& a, const PathAndStartIndex& b) {
      return *(a.first) > *(b.first);
    }
  };

  static size_t PathContainerSize(
      const std::vector<PathAndStartIndex>& container);

  // Shortest path between two nodes.
  std::unique_ptr<Walk> ShortestPath(GraphNodeIndex src, GraphNodeIndex dst,
                                     const GraphNodeSet& nodes_to_avoid,
                                     const GraphLinkSet& links_to_avoid,
                                     const Links& links_so_far) const;

  // Adds the next shortest paths to the K shortest paths list. Returns true if
  // no more shortest paths exist.
  bool NextPath();

  // Returns a set of links that contains: for any path in k_paths_ that starts
  // with the same links as root_path pick the next link -- the one after.
  void GetLinkExclusionSet(const Links& root_path, GraphLinkSet* out);

  // Stores the K shortest paths in order.
  std::vector<PathAndStartIndex> k_paths_;

  // The K shortest paths, in a trie for quick prefix lookup.
  Trie<GraphLinkIndex, uint32_t> k_paths_trie_;

  // Stores candidates for K shortest paths.
  VectorPriorityQueue<PathAndStartIndex, PathAndStartIndexComparator>
      candidates_;

  // The source.
  GraphNodeIndex src_;

  // The destination.
  GraphNodeIndex dst_;

  // The sanitized constraints.
  const ConstraintSet constraints_;

  // The graph.
  const GraphStorage* storage_;
};

// Like KShortestPathsGenerator above, but handles multiple constraint sets.
class DisjunctKShortestPathsGenerator {
 public:
  DisjunctKShortestPathsGenerator(GraphNodeIndex src, GraphNodeIndex dst,
                                  const GraphStorage& graph,
                                  const std::set<ConstraintSet>& constraints);

  // Returns the Kth shortest path. If 'gen_index' is not null will populate it
  // with the index of the generator that the path comes from.
  const Walk* KthShortestPathOrNull(size_t k, size_t* gen_index = nullptr);

  // Returns the generator at a given index.
  const KShortestPathsGenerator* ksp_generator(size_t i) const {
    CHECK(i < ksp_generators_.size());
    return ksp_generators_[i].get();
  }

  // Returns the K shortest paths.
  const std::vector<const Walk*>& k_paths() const { return k_paths_; }

  // Returns the shortest path that complies with the constraints and avoids a
  // set of nodes/links.
  std::unique_ptr<Walk> ShortestPathThatAvoids(
      const GraphNodeSet& nodes_to_avoid, const GraphLinkSet& links_to_avoid);

 private:
  struct PathGenAndPath {
    PathGenAndPath(size_t generator_i, const Walk* candidate)
        : generator_i(generator_i), candidate(candidate) {}

    size_t generator_i;
    const Walk* candidate;
  };

  struct Comparator {
    bool operator()(const PathGenAndPath& lhs, const PathGenAndPath& rhs) {
      return lhs.candidate->delay() > rhs.candidate->delay();
    }
  };

  // Returns the next shortest path, starting at the shortest.
  const Walk* Next(size_t* generator_index);

  // Pops the shortest path from the queue and adds the next one. Will also
  // populate generator_index with the index of the generator from which the
  // path comes.
  const Walk* PopAndEnqueue(size_t* generator_index);

  // A priority queue that has as many elements as there are KSP generators.
  // When the next path is generated the minimum of the queue is taken and
  // another path is generated from the generator that generated the minimum.
  VectorPriorityQueue<PathGenAndPath, Comparator> queue_;

  // The generators.
  std::vector<std::unique_ptr<KShortestPathsGenerator>> ksp_generators_;

  // The K shortest paths, owned by their respective generators.
  std::vector<const Walk*> k_paths_;

  // For the K-th shortest path the index of the generator that generated the
  // path.
  std::vector<size_t> k_path_generator_indices_;
};

// Combines multiple waypoint lists into one. The resulting list will obey all
// original waypoint lists. The delay map is assumed to contain the distances
// between all waypoints.
std::vector<GraphNodeIndex> CombineWaypoints(
    GraphNodeIndex src, GraphNodeIndex dst, const ExclusionSet& exclusion_set,
    const AdjacencyList& adj_list,
    const std::vector<std::vector<GraphNodeIndex>>& waypoints);

}  // namespace net
}  // namespace nc

#endif
