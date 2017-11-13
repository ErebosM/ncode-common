#include <chrono>

#include "algorithm.h"
#include "net_common.h"
#include "net_gen.h"
#include "../substitute.h"

using namespace nc;
using namespace std::chrono;

std::vector<net::GraphNodeSet> RandomSample(const net::GraphStorage& graph,
                                            size_t count, std::mt19937* rnd) {
  CHECK(count > 0);
  std::vector<net::GraphNodeIndex> all_nodes;
  for (net::GraphNodeIndex node : graph.AllNodes()) {
    all_nodes.emplace_back(node);
  }

  std::shuffle(all_nodes.begin(), all_nodes.end(), *rnd);

  std::vector<net::GraphNodeSet> out;
  out.emplace_back();

  for (net::GraphNodeIndex node_index : all_nodes) {
    net::GraphNodeSet& last_set = out.back();
    last_set.Insert(node_index);

    if (last_set.Count() == count) {
      out.emplace_back();
    }
  }

  return out;
}

size_t CountSimplePaths(const std::vector<const net::Walk*>& paths,
                        const net::GraphStorage& storage) {
  size_t i = 0;
  for (const auto& path : paths) {
    if (path->IsPath(storage)) {
      ++i;
    }
  }

  return i;
}

// Compares two lists of paths and returns true if the first count simple paths
// from both are the same.
static double Compare(const std::vector<std::unique_ptr<net::Walk>>& paths_one,
                      const std::vector<const net::Walk*>& paths_two,
                      size_t count, const net::GraphStorage& storage) {
  CHECK(paths_one.size() >= count);
  CHECK(paths_two.size() >= count);
  double total_delta = 0;

  size_t i_one = 0;
  size_t i_two = 0;
  for (size_t i = 0; i < count; ++i) {
    const net::Walk* ls_one;
    while (true) {
      CHECK(paths_one.size() > i_one);
      ls_one = paths_one[i_one++].get();
      if (ls_one->IsPath(storage)) {
        break;
      }
    }

    const net::Walk* ls_two;
    while (true) {
      CHECK(paths_two.size() > i_two);
      ls_two = paths_two[i_two++];
      if (ls_two->IsPath(storage)) {
        break;
      }
    }

    double delta = std::abs(ls_one->delay().count() - ls_two->delay().count());
    total_delta += delta;
  }

  return total_delta / count;
}

std::string SetToString(const net::GraphLinkSet& set) {
  std::vector<std::string> out;
  for (net::GraphLinkIndex link : set) {
    out.emplace_back(std::to_string(link));
  }

  return nc::Join(out, ",");
}

net::GraphNodeSet NodesInPath(const net::Walk& path,
                              const net::GraphStorage* storage) {
  net::GraphNodeSet out;
  for (const auto& link : path.links()) {
    const net::GraphLink* link_ptr = storage->GetLink(link);
    out.Insert(link_ptr->src());
    out.Insert(link_ptr->dst());
  }

  return out;
}

milliseconds SinglePassSingleConstraint(const net::GraphStorage& storage,
                                        net::GraphNodeIndex src_node,
                                        net::GraphNodeIndex dst_node,
                                        size_t to_visit_size, size_t count,
                                        bool single) {
  net::ConstraintSet constraints;

  if (to_visit_size > 0) {
    std::mt19937 rnd(1);
    std::vector<net::GraphNodeSet> to_visit_sets =
        RandomSample(storage, to_visit_size, &rnd);

    if (single) {
      net::GraphNodeSet to_visit = to_visit_sets.front();
      to_visit.Remove(src_node);
      to_visit.Remove(dst_node);
      constraints.AddToVisitSet(to_visit);
    } else {
      for (const auto& set : to_visit_sets) {
        constraints.AddToVisitSet(set);
      }
    }
  }

  LOG(ERROR) << constraints.ToString(storage);

  std::vector<const net::Walk*> paths;
  std::vector<std::unique_ptr<net::Walk>> all_paths;

  //  Paths(src_node, dst_node, [&all_paths](std::unique_ptr<net::Walk> walk) {
  //    all_paths.emplace_back(std::move(walk));
  //  }, storage, constraints);
  //  std::sort(all_paths.begin(), all_paths.end(),
  //            [](const std::unique_ptr<net::Walk>& lhs,
  //               const std::unique_ptr<net::Walk>& rhs) {
  //              return lhs->delay() < rhs->delay();
  //            });

  auto start = high_resolution_clock::now();
  net::KShortestPathsGenerator ksp(src_node, dst_node, storage, constraints);
  for (size_t i = 0; i < count; ++i) {
    const net::Walk* p = ksp.KthShortestPathOrNull(i);
    if (p != nullptr) {
      paths.emplace_back(p);
    }
  }
  auto end = high_resolution_clock::now();
  LOG(ERROR) << duration_cast<milliseconds>(end - start).count();
  exit(1);

  LOG(ERROR) << ksp.GetStats().ToString();

  size_t simple_paths = CountSimplePaths(paths, storage);
  LOG(ERROR) << "KSP " << paths.size() << " simple " << simple_paths
             << " all paths " << all_paths.size();
  double delta = Compare(all_paths, paths, simple_paths, storage);
  LOG(ERROR) << "delta " << delta;

  return duration_cast<milliseconds>(end - start);
}

int main(int argc, char** argv) {
  Unused(argc);
  Unused(argv);

  net::GraphBuilder builder = net::GenerateNTT(net::Delay::zero(), 2.0);
  net::GraphStorage path_storage(builder);
  LOG(ERROR) << "Graph with " << path_storage.NodeCount() << " nodes and "
             << path_storage.LinkCount() << " links";

  net::GraphNodeIndex london_node = path_storage.NodeFromStringOrDie("london");
  net::GraphNodeIndex tokyo_node = path_storage.NodeFromStringOrDie("tokyo");
  size_t all_nodes_count = path_storage.AllNodes().Count();

  size_t limit = all_nodes_count;
  size_t max_count = 100000;

  std::vector<double> times;
  for (size_t i = 0; i < limit; i += 2) {
    milliseconds ms = SinglePassSingleConstraint(
        path_storage, london_node, tokyo_node, i, max_count, true);
    times.emplace_back(ms.count());
  }
  LOG(ERROR) << "[" << nc::Join(times, ",") << "]";

  times.clear();
  for (size_t i = 0; i < 10; ++i) {
    size_t count = i * max_count / 10;
    milliseconds ms = SinglePassSingleConstraint(
        path_storage, london_node, tokyo_node, limit / 2, count, true);
    times.emplace_back(ms.count());
  }
  LOG(ERROR) << "[" << nc::Join(times, ",") << "]";
}
