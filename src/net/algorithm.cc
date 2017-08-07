#include "algorithm.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <utility>
#include <vector>

#include "../common.h"
#include "../logging.h"
#include "../perfect_hash.h"
#include "../strutil.h"

namespace nc {
namespace net {

bool ExclusionSet::ShouldExcludeLink(const GraphLinkIndex link) const {
  return links_to_exclude_.Contains(link);
}

bool ExclusionSet::ShouldExcludeNode(const GraphNodeIndex node) const {
  return nodes_to_exclude_.Contains(node);
}

void ConstraintSet::AddToVisitSet(const GraphNodeSet& set) {
  size_t set_index = to_visit_.size();
  to_visit_.emplace_back(set);

  for (GraphNodeIndex node_index : set) {
    CHECK(!node_to_visit_index_.HasValue(node_index));
    node_to_visit_index_[node_index] = set_index;
  }
}

void ConstraintSet::ExtendFirstSetToVisitOrDie(const GraphNodeSet& set) {
  CHECK(to_visit_.size() == 1);
  GraphNodeSet current_to_visit = to_visit_[0];
  current_to_visit.InsertAll(set);

  to_visit_.clear();
  node_to_visit_index_.Clear();
  AddToVisitSet(current_to_visit);
}

void ExclusionSet::AddAll(const ExclusionSet& other) {
  links_to_exclude_.InsertAll(other.links_to_exclude_);
  nodes_to_exclude_.InsertAll(other.nodes_to_exclude_);
}

size_t ConstraintSet::MinVisit(const Links& links,
                               const GraphStorage& graph_storage) const {
  int current_index = -1;
  if (links.empty()) {
    return 0;
  }

  const GraphLink* link_ptr = graph_storage.GetLink(links.front());
  GraphNodeIndex path_src = link_ptr->src();
  if (node_to_visit_index_.HasValue(path_src)) {
    int i = node_to_visit_index_.GetValueOrDie(path_src);
    if (current_index != i && (current_index + 1) != i) {
      return current_index + 1;
    }

    current_index = i;
  }

  for (GraphLinkIndex link : links) {
    const GraphLink* link_ptr = graph_storage.GetLink(link);
    GraphNodeIndex dst = link_ptr->dst();

    if (node_to_visit_index_.HasValue(dst)) {
      int i = node_to_visit_index_.GetValueOrDie(dst);
      if (i < current_index) {
        return i + 1;
      }

      if (current_index != i && (current_index + 1) != i) {
        return current_index + 1;
      }

      current_index = i;
    }
  }

  return current_index + 1;
}

std::vector<GraphNodeIndex> ConstraintSet::Waypoints(
    const Links& links, const GraphStorage& graph_storage) const {
  std::vector<GraphNodeIndex> out;
  if (links.empty()) {
    return {};
  }

  int last_i = -1;
  const GraphLink* link_ptr = graph_storage.GetLink(links.front());
  GraphNodeIndex path_src = link_ptr->src();
  if (node_to_visit_index_.HasValue(path_src)) {
    int i = node_to_visit_index_.GetValueOrDie(path_src);
    CHECK(i >= last_i);
    if (last_i != i) {
      out.emplace_back(path_src);
      last_i = i;
    }
  }

  for (GraphLinkIndex link : links) {
    const GraphLink* link_ptr = graph_storage.GetLink(link);
    GraphNodeIndex dst = link_ptr->dst();

    if (node_to_visit_index_.HasValue(dst)) {
      int i = node_to_visit_index_.GetValueOrDie(dst);
      CHECK(i >= last_i);
      if (last_i != i) {
        out.emplace_back(dst);
        last_i = i;
      }
    }
  }

  return out;
}

const GraphNodeSet* ConstraintSet::FindSetToVisitOrNull(
    const GraphNodeIndex node) const {
  if (!node_to_visit_index_.HasValue(node)) {
    return nullptr;
  }

  return &(to_visit_[node_to_visit_index_[node]]);
}

ConstraintSet ConstraintSet::SanitizeConstraints(GraphNodeIndex src,
                                                 GraphNodeIndex dst) const {
  if (to_visit_.empty()) {
    return *this;
  }

  ConstraintSet to_return;
  to_return.Exclude().Links(exclusion_set_.links_to_exclude());
  to_return.Exclude().Nodes(exclusion_set_.nodes_to_exclude());

  for (size_t i = 0; i < to_visit_.size(); ++i) {
    GraphNodeSet set = to_visit_[i];

    if (i == 0 && set.Contains(src)) {
      set.Clear();
    } else if (i == to_visit_.size() - 1 && set.Contains(dst)) {
      set.Clear();
    } else {
      CHECK(!set.Contains(src) || !set.Contains(dst))
          << "src/dst can only be part of the first/last set";
    }

    if (set.Empty()) {
      continue;
    }

    to_return.AddToVisitSet(set);
  }

  return to_return;
}

std::string ConstraintSet::ToString(const GraphStorage& storage) const {
  std::vector<std::string> visit_constraints;
  for (const auto& set_to_visit : to_visit_) {
    visit_constraints.emplace_back(GraphNodeSetToString(set_to_visit, storage));
  }

  return Substitute("$0, visit nodes $1", exclusion_set_.ToString(storage),
                    Join(visit_constraints, "->"));
}

static void PathsRecursive(
    const DFSConfig& config, GraphNodeIndex at, GraphNodeIndex dst,
    std::function<void(std::unique_ptr<Walk>)> path_callback,
    const GraphStorage& graph, const ConstraintSet& constraints,
    GraphLinkSet* links_seen, GraphNodeSet* nodes_seen, Links* current,
    Delay* total_distance) {
  if (current->size() > config.max_hops) {
    return;
  }

  if (at == dst) {
    size_t min_v = constraints.MinVisit(*current, graph);
    if (min_v != constraints.to_visit().size()) {
      return;
    }

    auto new_walk = make_unique<Walk>(*current, *total_distance);
    path_callback(std::move(new_walk));
    return;
  }

  Delay min_distance = *total_distance;
  if (min_distance > config.max_distance) {
    return;
  }

  const AdjacencyList& adjacency_list = graph.AdjacencyList();
  const std::vector<AdjacencyList::LinkInfo>& outgoing_links =
      adjacency_list.GetNeighbors(at);

  if (config.simple) {
    if (nodes_seen->Contains(at)) {
      return;
    }
    nodes_seen->Insert(at);
  }

  for (const AdjacencyList::LinkInfo& out_link_info : outgoing_links) {
    GraphLinkIndex link_index = out_link_info.link_index;
    if (constraints.ShouldExcludeLink(link_index)) {
      continue;
    }

    GraphNodeIndex next_hop = out_link_info.dst_index;
    if (constraints.ShouldExcludeNode(next_hop)) {
      continue;
    }

    if (!config.simple) {
      if (links_seen->Contains(link_index)) {
        continue;
      }
      links_seen->Insert(link_index);
    }

    current->push_back(link_index);
    *total_distance += out_link_info.delay;
    PathsRecursive(config, next_hop, dst, path_callback, graph, constraints,
                   links_seen, nodes_seen, current, total_distance);
    *total_distance -= out_link_info.delay;
    current->pop_back();

    if (!config.simple) {
      links_seen->Remove(link_index);
    }
  }

  if (config.simple) {
    nodes_seen->Remove(at);
  }
}

void Paths(GraphNodeIndex src, GraphNodeIndex dst,
           std::function<void(std::unique_ptr<Walk>)> path_callback,
           const GraphStorage& graph, const ConstraintSet& constraints,
           const DFSConfig& config) {
  Delay total_distance = Delay::zero();
  GraphLinkSet links_seen;
  GraphNodeSet nodes_seen;
  Links scratch_path;
  PathsRecursive(config, src, dst, path_callback, graph, constraints,
                 &links_seen, &nodes_seen, &scratch_path, &total_distance);
}

static void ReachableNodesRecursive(GraphNodeIndex at,
                                    const GraphStorage& graph,
                                    const ExclusionSet& exclusion_set,
                                    GraphNodeSet* nodes_seen) {
  if (nodes_seen->Contains(at)) {
    return;
  }
  nodes_seen->Insert(at);

  const AdjacencyList& adjacency_list = graph.AdjacencyList();
  const std::vector<AdjacencyList::LinkInfo>& outgoing_links =
      adjacency_list.GetNeighbors(at);

  for (const AdjacencyList::LinkInfo& out_link_info : outgoing_links) {
    if (exclusion_set.ShouldExcludeLink(out_link_info.link_index)) {
      continue;
    }

    GraphNodeIndex next_hop = out_link_info.dst_index;
    if (exclusion_set.ShouldExcludeNode(next_hop)) {
      continue;
    }

    ReachableNodesRecursive(next_hop, graph, exclusion_set, nodes_seen);
  }
}

GraphNodeSet ReachableNodes(GraphNodeIndex src, const GraphStorage& graph,
                            const ExclusionSet& exclusion_set) {
  GraphNodeSet set;
  ReachableNodesRecursive(src, graph, exclusion_set, &set);
  return set;
}

static Links RecoverPath(
    GraphNodeIndex src, GraphNodeIndex dst,
    const GraphNodeMap<const AdjacencyList::LinkInfo*>& previous) {
  Links links_reverse;

  GraphNodeIndex current = dst;
  while (current != src) {
    if (!previous.HasValue(current)) {
      return {};
    }

    const AdjacencyList::LinkInfo* link_info = previous[current];

    links_reverse.emplace_back(link_info->link_index);
    current = link_info->src_index;
  }

  std::reverse(links_reverse.begin(), links_reverse.end());
  return links_reverse;
}

std::unique_ptr<Walk> ShortestPath::GetPath(GraphNodeIndex dst) const {
  CHECK(destinations_.Contains(dst)) << "Bad destination";

  Links links = RecoverPath(src_, dst, previous_);
  if (links.empty()) {
    return {};
  }

  Delay distance = min_delays_[dst].distance;
  return make_unique<Walk>(links, distance);
}

Delay ShortestPath::GetPathDistance(GraphNodeIndex dst) const {
  CHECK(destinations_.Contains(dst)) << "Bad destination";

  // This is the tree that starts at 'src_'. It may be possible that there are
  // nodes in the graph that are not reachable from 'src_'. Those nodes will
  // not have a distance set in 'min_delays_'.
  if (!min_delays_.HasValue(dst)) {
    return Delay::max();
  }

  return min_delays_.UnsafeAccess(dst).distance;
}

static bool CanExcludeNode(GraphNodeIndex node, const ExclusionSet& constraints,
                           const GraphNodeSet* additional_nodes_to_avoid) {
  if (additional_nodes_to_avoid != nullptr &&
      additional_nodes_to_avoid->Contains(node)) {
    return true;
  }

  return constraints.ShouldExcludeNode(node);
}

static bool CanExcludeLink(GraphLinkIndex link, const ExclusionSet& constraints,
                           const GraphLinkSet* additional_links_to_avoid) {
  if (additional_links_to_avoid != nullptr &&
      additional_links_to_avoid->Contains(link)) {
    return true;
  }

  return constraints.ShouldExcludeLink(link);
}

void ShortestPath::ComputePaths(const ExclusionSet& exclusion_set,
                                const AdjacencyList& adj_list,
                                const GraphNodeSet* additional_nodes_to_avoid,
                                const GraphLinkSet* additional_links_to_avoid) {
  using DelayAndIndex = std::pair<Delay, GraphNodeIndex>;
  std::priority_queue<DelayAndIndex, std::vector<DelayAndIndex>,
                      std::greater<DelayAndIndex>> vertex_queue;

  min_delays_.Resize(adj_list.AllNodes().Count());
  previous_.Resize(adj_list.AllNodes().Count());

  if (CanExcludeNode(src_, exclusion_set, additional_nodes_to_avoid)) {
    return;
  }

  min_delays_[src_].distance = Delay::zero();
  vertex_queue.emplace(Delay::zero(), src_);

  size_t destinations_remaining = destinations_.Count();
  while (!vertex_queue.empty()) {
    Delay distance;
    GraphNodeIndex current;
    std::tie(distance, current) = vertex_queue.top();
    vertex_queue.pop();

    if (distance > min_delays_[current].distance) {
      // Bogus leftover node, since we never delete nodes from the heap.
      continue;
    }

    if (destinations_.Contains(current)) {
      --destinations_remaining;
      if (destinations_remaining == 0) {
        break;
      }
    }

    const std::vector<AdjacencyList::LinkInfo>& neighbors =
        adj_list.GetNeighbors(current);
    for (const AdjacencyList::LinkInfo& out_link_info : neighbors) {
      GraphLinkIndex out_link = out_link_info.link_index;
      if (CanExcludeLink(out_link, exclusion_set, additional_links_to_avoid)) {
        continue;
      }

      GraphNodeIndex neighbor_node = out_link_info.dst_index;
      if (CanExcludeNode(neighbor_node, exclusion_set,
                         additional_nodes_to_avoid)) {
        continue;
      }

      const Delay link_delay = out_link_info.delay;
      const Delay distance_via_neighbor = distance + link_delay;
      Delay& curr_min_distance = min_delays_[neighbor_node].distance;

      if (distance_via_neighbor < curr_min_distance) {
        curr_min_distance = distance_via_neighbor;
        previous_[neighbor_node] = &out_link_info;
        vertex_queue.emplace(curr_min_distance, neighbor_node);
      }
    }
  }
}

std::unique_ptr<Walk> AllPairShortestPath::GetPath(GraphNodeIndex src,
                                                   GraphNodeIndex dst) const {
  Delay dist = data_[src][dst].distance;
  if (dist == Delay::max()) {
    return {};
  }

  Links links;
  GraphNodeIndex next = src;
  while (next != dst) {
    const SPData& datum = data_[next][dst];
    links.emplace_back(datum.next_link);
    next = datum.next_node;
  }

  return make_unique<Walk>(links, dist);
}

Delay AllPairShortestPath::GetDistance(GraphNodeIndex src,
                                       GraphNodeIndex dst) const {
  return data_[src][dst].distance;
}

void AllPairShortestPath::ComputePaths(
    const ExclusionSet& exclusion_set, const AdjacencyList& adj_list,
    const GraphNodeSet* additional_nodes_to_avoid,
    const GraphLinkSet* additional_links_to_avoid) {
  const GraphNodeSet nodes = adj_list.AllNodes();
  for (GraphNodeIndex node : nodes) {
    if (CanExcludeNode(node, exclusion_set, additional_nodes_to_avoid)) {
      continue;
    }

    SPData& node_data = data_[node][node];
    node_data.distance = Delay::zero();
  }

  for (const auto& node_and_neighbors : adj_list.Adjacencies()) {
    for (const AdjacencyList::LinkInfo& link_info :
         *node_and_neighbors.second) {
      GraphLinkIndex link = link_info.link_index;
      if (CanExcludeLink(link, exclusion_set, additional_links_to_avoid)) {
        continue;
      }

      Delay distance = link_info.delay;
      SPData& sp_data = data_[link_info.src_index][link_info.dst_index];
      sp_data.distance = distance;
      sp_data.next_link = link;
      sp_data.next_node = link_info.dst_index;
    }
  }

  for (GraphNodeIndex k : nodes) {
    for (GraphNodeIndex i : nodes) {
      for (GraphNodeIndex j : nodes) {
        Delay i_k = data_[i][k].distance;
        Delay k_j = data_[k][j].distance;

        bool any_max = (i_k == Delay::max() || k_j == Delay::max());
        Delay alt_distance = any_max ? Delay::max() : i_k + k_j;

        SPData& i_j_data = data_[i][j];
        if (alt_distance < i_j_data.distance) {
          i_j_data.distance = alt_distance;

          const SPData& i_k_data = data_[i][k];
          i_j_data.next_link = i_k_data.next_link;
          i_j_data.next_node = i_k_data.next_node;
        }
      }
    }
  }
}

std::pair<GraphNodeSet, GraphLinkSet> ShortestPath::ElementsInTree() const {
  GraphNodeSet nodes;
  GraphLinkSet links;

  for (const auto& node_and_info : previous_) {
    const AdjacencyList::LinkInfo* link_info = *node_and_info.second;
    nodes.Insert(link_info->src_index);
    nodes.Insert(link_info->dst_index);
    links.Insert(link_info->link_index);
  }

  return {nodes, links};
}

using VisitList = std::vector<GraphNodeSet>;

static std::unique_ptr<Walk> ShortestPathStatic(
    const GraphNodeIndex src, const GraphNodeIndex dst,
    const GraphNodeSet& nodes_to_avoid, const GraphLinkSet& links_to_avoid,
    const ExclusionSet& exclusion_set, VisitList::const_iterator to_visit_from,
    VisitList::const_iterator to_visit_to, const AdjacencyList& adj_list) {
  size_t to_visit_count = std::distance(to_visit_from, to_visit_to);
  if (to_visit_count == 0) {
    net::ShortestPath sp_tree(src, {dst}, exclusion_set, adj_list,
                              &nodes_to_avoid, &links_to_avoid);
    return sp_tree.GetPath(dst);
  }

  // Nodes to exclude.
  GraphNodeSet to_exclude;

  // The adjacency list for the graph that is created from shortest paths from
  // each node in 'to_visit' to destinations.
  AdjacencyList path_graph_adj_list;

  // Generates sequential link indices.
  size_t path_graph_link_index_gen = -1;

  // Shortest path trees.
  GraphNodeMap<std::unique_ptr<net::ShortestPath>> sp_trees;

  // Maps a pair of src, dst with the link that represents the shortest path
  // between them in the new graph. The link index is not into the original
  // graph (from graph_storage) but one of the ones generated by
  // path_graph_link_index_gen.
  GraphLinkMap<std::pair<GraphNodeIndex, GraphNodeIndex>> link_map;

  // Will assume that the front/back do not contain the src/dst, as it makes it
  // easier to reason about order. This is enforced elsewhere.
  for (size_t i = -1; i != to_visit_count; ++i) {
    // For each set we will compute the SP trees rooted at each node. Each of
    // those SP trees should avoid nodes from other sets, except for the next
    // set.
    to_exclude.Clear();
    for (size_t j = 0; j < to_visit_count; ++j) {
      if (i != j && (i + 1) != j) {
        to_exclude.InsertAll(*std::next(to_visit_from, j));
      }
    }

    if (i != to_visit_count - 1) {
      to_exclude.Insert(dst);
    }

    GraphNodeSet destinations;
    if (i == to_visit_count - 1) {
      destinations.Insert(dst);
    } else {
      destinations.InsertAll(*std::next(to_visit_from, i + 1));
    }

    GraphNodeSet sources;
    if (i == static_cast<size_t>(-1)) {
      sources.Insert(src);
    } else {
      const GraphNodeSet& set_to_visit = *std::next(to_visit_from, i);
      sources.InsertAll(set_to_visit);
    }

    CHECK(!sources.Empty());
    CHECK(!destinations.Empty());
    to_exclude.InsertAll(nodes_to_avoid);

    bool no_path_found = true;
    for (GraphNodeIndex node_to_visit : sources) {
      std::unique_ptr<net::ShortestPath>& tree = sp_trees[node_to_visit];
      tree = make_unique<net::ShortestPath>(node_to_visit, destinations,
                                            exclusion_set, adj_list,
                                            &to_exclude, &links_to_avoid);

      for (GraphNodeIndex destination : destinations) {
        GraphLinkIndex new_link_index(++path_graph_link_index_gen);

        Delay sp_delay = tree->GetPathDistance(destination);
        if (sp_delay == Delay::max()) {
          continue;
        }

        path_graph_adj_list.AddLink(new_link_index, node_to_visit, destination,
                                    sp_delay);
        link_map[new_link_index] = {node_to_visit, destination};
        no_path_found = false;
      }
    }

    if (no_path_found) {
      return {};
    }
  }

  // Now we can find out the shortest path through the new graph, the links of
  // which will tell us which paths we need to stitch together in order to form
  // the final end-to-end path.
  ExclusionSet dummy;
  net::ShortestPath path_graph_sp(src, {dst}, dummy, path_graph_adj_list,
                                  nullptr, nullptr);
  std::unique_ptr<Walk> sp = path_graph_sp.GetPath(dst);
  if (!sp) {
    return {};
  }

  Links final_path;
  Delay total_delay = Delay::zero();
  for (GraphLinkIndex path_graph_link : sp->links()) {
    GraphNodeIndex sub_path_from;
    GraphNodeIndex sub_path_to;
    std::tie(sub_path_from, sub_path_to) = link_map[path_graph_link];

    const net::ShortestPath* sp_tree =
        sp_trees.GetValueOrDie(sub_path_from).get();
    std::unique_ptr<Walk> sub_path = sp_tree->GetPath(sub_path_to);
    CHECK(sub_path);
    final_path.insert(final_path.end(), sub_path->links().begin(),
                      sub_path->links().end());
    total_delay += sub_path->delay();
  }

  return make_unique<Walk>(final_path, total_delay);
}

std::unique_ptr<Walk> ShortestPathWithConstraints(
    GraphNodeIndex src, GraphNodeIndex dst, const GraphStorage& graph,
    const ConstraintSet& constraints) {
  ConstraintSet sanitized_constraints =
      constraints.SanitizeConstraints(src, dst);

  return ShortestPathStatic(
      src, dst, {}, {}, sanitized_constraints.exclusion_set(),
      sanitized_constraints.to_visit().begin(),
      sanitized_constraints.to_visit().end(), graph.AdjacencyList());
}

std::unique_ptr<Walk> KShortestPathsGenerator::ShortestPath(
    GraphNodeIndex src, GraphNodeIndex dst, const GraphNodeSet& nodes_to_avoid,
    const GraphLinkSet& links_to_avoid, const Links& links_so_far) const {
  size_t to_visit_index = constraints_.MinVisit(links_so_far, *storage_);
  const VisitList& to_visit = constraints_.to_visit();
  auto start_it = std::next(to_visit.begin(), to_visit_index);

  return ShortestPathStatic(src, dst, nodes_to_avoid, links_to_avoid,
                            constraints_.exclusion_set(), start_it,
                            to_visit.end(), storage_->AdjacencyList());
}

std::unique_ptr<Walk> KShortestPathsGenerator::ShortestPathThatAvoids(
    const GraphNodeSet& nodes_to_avoid,
    const GraphLinkSet& links_to_avoid) const {
  return ShortestPath(src_, dst_, nodes_to_avoid, links_to_avoid, {});
}

bool KShortestPathsGenerator::NextPath() {
  if (k_paths_.empty()) {
    auto path = ShortestPath(src_, dst_, {}, {}, {});
    if (!path) {
      return false;
    }

    k_paths_trie_.Add(path->links(), 0);
    k_paths_.emplace_back(std::move(path), 0);
    return true;
  }

  const PathAndStartIndex& last_path_and_start_index = k_paths_.back();
  const Walk& last_path = *(last_path_and_start_index.first);
  const Links& last_path_links = last_path.links();
  size_t start_index = last_path_and_start_index.second;

  GraphLinkSet links_to_exclude;
  GraphNodeSet nodes_to_exclude;

  Links root_path;
  for (size_t i = 0; i < last_path_links.size(); ++i) {
    GraphLinkIndex link_index = last_path_links[i];
    const GraphLink* link = storage_->GetLink(link_index);
    GraphNodeIndex spur_node = link->src();
    if (i < start_index) {
      nodes_to_exclude.Insert(spur_node);
      root_path.emplace_back(link_index);
      continue;
    }

    GetLinkExclusionSet(root_path, &links_to_exclude);
    auto spur_path = ShortestPath(spur_node, dst_, nodes_to_exclude,
                                  links_to_exclude, root_path);
    if (spur_path) {
      const Links& spur_path_links = spur_path->links();

      Links candidate_links = root_path;
      candidate_links.insert(candidate_links.end(), spur_path_links.begin(),
                             spur_path_links.end());
      auto candidate_path = make_unique<Walk>(candidate_links, *storage_);
      candidates_.emplace(std::move(candidate_path), i);
    }

    nodes_to_exclude.Insert(spur_node);
    root_path.emplace_back(link_index);
  }

  if (candidates_.empty()) {
    return false;
  }

  PathAndStartIndex min_candidate = candidates_.PopTop();
  k_paths_trie_.Add(min_candidate.first->links(), k_paths_.size());
  k_paths_.emplace_back(std::move(min_candidate));

  return true;
}

const Walk* KShortestPathsGenerator::KthShortestPathOrNull(size_t k) {
  if (k < k_paths_.size()) {
    return k_paths_[k].first.get();
  }

  size_t delta = k + 1 - k_paths_.size();
  for (size_t i = 0; i < delta; ++i) {
    if (!NextPath()) {
      return nullptr;
    }
  }

  return k_paths_.back().first.get();
}

size_t KShortestPathsGenerator::KforPath(const Walk& to_look_for,
                                         size_t k_limit) {
  for (size_t k = 0; k < k_limit; ++k) {
    const Walk* path = KthShortestPathOrNull(k);
    CHECK(path->delay() <= to_look_for.delay());

    if (to_look_for == *path) {
      return k;
    }
  }

  return k_limit;
}

KShortestPathGeneratorStats KShortestPathsGenerator::GetStats() const {
  KShortestPathGeneratorStats stats;
  stats.k = k_paths_.size();
  stats.paths_size_bytes = PathContainerSize(k_paths_);

  stats.trie_stats = k_paths_trie_.GetStats();
  stats.candidate_count = candidates_.size();

  size_t candidate_overhead = PathContainerSize(candidates_.containter());
  stats.total_size_bytes = sizeof(*this) + stats.paths_size_bytes +
                           stats.trie_stats.size_bytes + candidate_overhead;

  if (!k_paths_.empty()) {
    stats.min_path_delay = k_paths_.front().first->delay();
    stats.max_path_delay = k_paths_.back().first->delay();

    for (const auto& candidate : candidates_.containter()) {
      stats.max_candidate_path_delay =
          std::max(stats.max_candidate_path_delay, candidate.first->delay());
    }
  }

  return stats;
}

size_t KShortestPathsGenerator::PathContainerSize(
    const std::vector<PathAndStartIndex>& container) {
  size_t total = container.capacity() * sizeof(PathAndStartIndex);
  for (const auto& path : container) {
    total += path.first->InMemBytesEstimate() - sizeof(Walk);
  }

  return total;
}

void KShortestPathsGenerator::GetLinkExclusionSet(const Links& root_path,
                                                  GraphLinkSet* out) {
  if (root_path.empty()) {
    for (const auto& path : k_paths_) {
      const Links& k_path_links = path.first->links();
      out->Insert(k_path_links[0]);
    }

    return;
  }

  const std::vector<uint32_t>& paths_with_same_prefix =
      k_paths_trie_.SequencesWithPrefix(root_path);
  for (uint32_t k : paths_with_same_prefix) {
    const Links& k_path_links = k_paths_[k].first->links();
    CHECK(k_path_links.size() > root_path.size());
    out->Insert(k_path_links[root_path.size()]);
  }
}

const Walk* DisjunctKShortestPathsGenerator::PopAndEnqueue(
    size_t* generator_index) {
  PathGenAndPath top = queue_.PopTop();
  size_t gen_i = top.generator_i;
  if (generator_index != nullptr) {
    *generator_index = gen_i;
  }

  CHECK(top.candidate != nullptr);
  const Walk* to_return = top.candidate;

  KShortestPathsGenerator* ksp_gen = ksp_generators_[gen_i].get();
  top.candidate = ksp_gen->KthShortestPathOrNull(ksp_gen->k());
  if (top.candidate != nullptr) {
    queue_.emplace(top);
  }

  return to_return;
}

const Walk* DisjunctKShortestPathsGenerator::Next(size_t* generator_index) {
  if (queue_.empty()) {
    return nullptr;
  }

  const Walk* to_return = PopAndEnqueue(generator_index);
  while (!queue_.empty() && (*queue_.top().candidate == *to_return)) {
    // Will keep popping from the queue until we get to a different path. Cannot
    // simply compare pointers, as the paths come from different generators.
    PopAndEnqueue(nullptr);
  }

  return to_return;
}

const Walk* DisjunctKShortestPathsGenerator::KthShortestPathOrNull(
    size_t k, size_t* generator_index) {
  if (k < k_paths_.size()) {
    const Walk* path = k_paths_[k];
    if (generator_index != nullptr) {
      *generator_index = k_path_generator_indices_[k];
    }

    return path;
  }

  size_t delta = k + 1 - k_paths_.size();
  for (size_t i = 0; i < delta; ++i) {
    size_t gen_index;
    const Walk* next = Next(&gen_index);
    if (next == nullptr) {
      return nullptr;
    }

    if (generator_index != nullptr) {
      *generator_index = gen_index;
    }

    k_paths_.emplace_back(next);
    k_path_generator_indices_.emplace_back(gen_index);
  }

  return k_paths_.back();
}

DisjunctKShortestPathsGenerator::DisjunctKShortestPathsGenerator(
    GraphNodeIndex src, GraphNodeIndex dst, const GraphStorage& graph,
    const std::set<ConstraintSet>& constraints) {
  for (const ConstraintSet& constraint_set : constraints) {
    ksp_generators_.emplace_back(
        make_unique<KShortestPathsGenerator>(src, dst, graph, constraint_set));
  }

  for (size_t i = 0; i < ksp_generators_.size(); ++i) {
    KShortestPathsGenerator* generator = ksp_generators_[i].get();
    const Walk* walk = generator->KthShortestPathOrNull(generator->k());
    if (walk != nullptr) {
      queue_.emplace(i, walk);
    }
  }
}

std::unique_ptr<Walk> DisjunctKShortestPathsGenerator::ShortestPathThatAvoids(
    const GraphNodeSet& nodes_to_avoid, const GraphLinkSet& links_to_avoid) {
  std::unique_ptr<Walk> current;
  for (const auto& generator : ksp_generators_) {
    auto p = generator->ShortestPathThatAvoids(nodes_to_avoid, links_to_avoid);
    if (!p) {
      continue;
    }

    if (current && p->delay() > current->delay()) {
      continue;
    }

    current = std::move(p);
  }

  return current;
}

static Delay DelayOrDie(
    const GraphNodeMap<std::unique_ptr<ShortestPath>>& sp_trees,
    GraphNodeIndex src, GraphNodeIndex dst) {
  Delay to_return = sp_trees.GetValueOrDie(src)->GetPathDistance(dst);
  CHECK(to_return != Delay::max());
  return to_return;
}

// Returns the index of node that is the closest to 'to' in waypoint_list
// starting at start_index.
static size_t ClosestTo(
    const GraphNodeMap<std::unique_ptr<ShortestPath>>& sp_trees,
    const std::vector<GraphNodeIndex>& waypoint_list,
    GraphNodeIndex new_waypoint, size_t start_index) {
  CHECK(waypoint_list.size() > 1);
  CHECK(start_index < waypoint_list.size() - 1);

  Delay best_delta = Delay::max();
  size_t best_i = 0;
  for (size_t i = start_index; i < waypoint_list.size() - 1; ++i) {
    // Delay between two consecutive waypoints.
    Delay direct_delay =
        DelayOrDie(sp_trees, waypoint_list[i], waypoint_list[i + 1]);

    // Delay if we were to insert the waypoint between this waypoint and the
    // next one.
    Delay indirect_delay =
        DelayOrDie(sp_trees, waypoint_list[i], new_waypoint) +
        DelayOrDie(sp_trees, new_waypoint, waypoint_list[i + 1]);

    CHECK(indirect_delay >= direct_delay);
    Delay delta = indirect_delay - direct_delay;
    if (delta < best_delta) {
      best_delta = delta;
      best_i = i;
    }
  }

  return best_i;
}

// Merges the waypoints from 'to_merge' into 'current_waypoints'.
static void Merge(const std::vector<GraphNodeIndex>& to_merge,
                  const GraphNodeMap<std::unique_ptr<ShortestPath>>& sp_trees,
                  std::vector<GraphNodeIndex>* current_waypoints) {
  size_t prev_index = 0;
  for (GraphNodeIndex waypoint_to_merge : to_merge) {
    size_t new_index =
        ClosestTo(sp_trees, *current_waypoints, waypoint_to_merge, prev_index);

    // Will insert the waypoint right after 'new_index' and update prev_index,
    // as all subsequent waypoints should follow this one.
    current_waypoints->insert(
        std::next(current_waypoints->begin(), new_index + 1),
        waypoint_to_merge);
    prev_index = new_index + 1;
  }
}

// Retturns for each node the SP tree that is rooted at this node.
static GraphNodeMap<std::unique_ptr<ShortestPath>> GetSPTrees(
    const GraphNodeSet& nodes, const ExclusionSet& exclusion_set,
    const AdjacencyList& adj_list) {
  GraphNodeMap<std::unique_ptr<ShortestPath>> out;
  for (GraphNodeIndex node : nodes) {
    out[node] = make_unique<ShortestPath>(node, nodes, exclusion_set, adj_list);
  }

  return out;
}

std::vector<GraphNodeIndex> CombineWaypoints(
    GraphNodeIndex src, GraphNodeIndex dst, const ExclusionSet& exclusion_set,
    const AdjacencyList& adj_list,
    const std::vector<std::vector<GraphNodeIndex>>& waypoints) {
  CHECK(!waypoints.empty());

  GraphNodeSet all_waypoints = {src, dst};
  for (const std::vector<GraphNodeIndex>& waypoint_list : waypoints) {
    for (GraphNodeIndex waypoint : waypoint_list) {
      all_waypoints.Insert(waypoint);
    }
  }

  GraphNodeMap<std::unique_ptr<ShortestPath>> sp_trees =
      GetSPTrees(all_waypoints, exclusion_set, adj_list);

  std::vector<GraphNodeIndex> current_waypoints = {src};
  const std::vector<GraphNodeIndex>& first_waypoint_list = waypoints.front();
  current_waypoints.insert(current_waypoints.end(), first_waypoint_list.begin(),
                           first_waypoint_list.end());
  current_waypoints.emplace_back(dst);

  for (size_t i = 1; i < waypoints.size(); ++i) {
    Merge(waypoints[i], sp_trees, &current_waypoints);
  }
  return {std::next(current_waypoints.begin(), 1),
          std::next(current_waypoints.end(), -1)};
}

std::pair<GraphLinkSet, double> CommonSPLinks(const GraphStorage& graph) {
  GraphLinkSet out;
  GraphLinkSet all_links;

  const AdjacencyList& adj_list = graph.AdjacencyList();
  for (GraphNodeIndex root : adj_list.AllNodes()) {
    GraphNodeSet others = adj_list.AllNodes();
    others.Remove(root);

    ShortestPath sp(root, others, {}, adj_list);
    GraphLinkSet links_in_tree;
    std::tie(std::ignore, links_in_tree) = sp.ElementsInTree();

    GraphLinkSet links_in_tree_same_way;
    for (GraphLinkIndex link_index : links_in_tree) {
      const nc::net::GraphLink* link = graph.GetLink(link_index);
      GraphNodeIndex link_src = link->src();
      GraphNodeIndex link_dst = link->dst();
      if (link_src < link_dst) {
        links_in_tree_same_way.Insert(link_index);
        continue;
      }

      const nc::net::GraphLinkIndex* inverse_link_index =
          graph.FindUniqueInverseOrNull(link);
      if (inverse_link_index == nullptr) {
        links_in_tree_same_way.Insert(link_index);
        continue;
      }

      links_in_tree_same_way.Insert(*inverse_link_index);
    }

    all_links.InsertAll(links_in_tree_same_way);
    if (out.Empty()) {
      out = links_in_tree_same_way;
    } else {
      out = out.Intersection(links_in_tree_same_way);
    }
  }

  return {out, out.Count() / static_cast<double>(all_links.Count())};
}

}  // namespace nc
}  // namespace ncode
