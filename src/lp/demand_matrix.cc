#include "demand_matrix.h"

#include <functional>
#include <iterator>
#include <tuple>

#include "../file.h"
#include "../map_util.h"
#include "../perfect_hash.h"
#include "../stats.h"
#include "../strutil.h"
#include "mc_flow.h"

namespace nc {
namespace lp {

using namespace std::chrono;

net::GraphLinkMap<double> DemandMatrix::SPUtilization() const {
  using namespace net;
  using NodeAndElement = std::pair<GraphNodeIndex, const DemandMatrixElement*>;

  GraphLinkSet all_links = graph_->AllLinks();
  GraphLinkMap<double> out;

  GraphNodeMap<std::vector<NodeAndElement>> src_to_destinations;
  for (const DemandMatrixElement& element : elements_) {
    src_to_destinations[element.src].emplace_back(element.dst, &element);
  }

  for (const auto& src_and_destinations : src_to_destinations) {
    GraphNodeIndex src = src_and_destinations.first;
    const std::vector<NodeAndElement>& destinations =
        *src_and_destinations.second;

    GraphNodeSet destinations_set;
    for (const NodeAndElement& node_and_element : destinations) {
      destinations_set.Insert(node_and_element.first);
    }

    ShortestPath sp(src, destinations_set, {}, graph_->AdjacencyList(), nullptr,
                    nullptr);
    for (const NodeAndElement& node_and_element : destinations) {
      std::unique_ptr<Walk> shortest_path = sp.GetPath(node_and_element.first);
      for (GraphLinkIndex link : shortest_path->links()) {
        out[link] += node_and_element.second->demand.Mbps();
      }
    }
  }

  for (GraphLinkIndex link : all_links) {
    double capacity = graph_->GetLink(link)->bandwidth().Mbps();
    double& link_utilization = out[link];
    link_utilization = link_utilization / capacity;
  }

  return out;
}

double DemandMatrix::SPMaxUtilization() const {
  net::GraphLinkMap<double> sp_utilization = SPUtilization();
  double max_utilization = 0;
  for (const auto& link_and_utilization : sp_utilization) {
    max_utilization = std::max(max_utilization, *link_and_utilization.second);
  }

  return max_utilization;
}

size_t DemandMatrix::OverloadedSPLinkCount() const {
  net::GraphLinkMap<double> link_utilization = SPUtilization();
  size_t i = 0;
  for (const auto& link_and_utilization : link_utilization) {
    if (*link_and_utilization.second > 1.0) {
      ++i;
    }
  }

  return i;
}

net::Bandwidth DemandMatrix::TotalLoad() const {
  double load_mbps = 0;
  for (const DemandMatrixElement& element : elements_) {
    load_mbps += element.demand.Mbps();
  }

  return net::Bandwidth::FromMBitsPerSecond(load_mbps);
}

net::Bandwidth DemandMatrix::Load(const NodePair& node_pair) const {
  for (const DemandMatrixElement& element : elements_) {
    if (element.src == node_pair.first && element.dst == node_pair.second) {
      return element.demand;
    }
  }

  return net::Bandwidth::FromMBitsPerSecond(0);
}

std::map<DemandMatrix::NodePair, std::pair<size_t, net::Delay>>
DemandMatrix::SPStats() const {
  using namespace net;

  std::map<DemandMatrix::NodePair, std::pair<size_t, net::Delay>> out;
  for (const DemandMatrixElement& element : elements_) {
    ShortestPath sp(element.src, {element.dst}, {}, graph_->AdjacencyList(),
                    nullptr, nullptr);
    std::unique_ptr<Walk> shortest_path = sp.GetPath(element.dst);
    out[{element.src, element.dst}] = {shortest_path->size(),
                                       shortest_path->delay()};
  }

  return out;
}

double DemandMatrix::SPGlobalUtilization() const {
  net::GraphLinkMap<double> sp_utilizaiton = SPUtilization();
  double total_load = 0;
  double total_capacity = 0;
  for (const auto& link_index_and_utilization : sp_utilizaiton) {
    net::GraphLinkIndex link_index = link_index_and_utilization.first;
    double utilization = *link_index_and_utilization.second;
    double capacity = graph_->GetLink(link_index)->bandwidth().Mbps();
    double load = utilization * capacity;

    total_load += load;
    total_capacity += capacity;
  }

  return total_load / total_capacity;
}

bool DemandMatrix::IsTriviallySatisfiable() const {
  net::GraphLinkMap<double> sp_utilizaiton = SPUtilization();
  for (const auto& link_and_utilization : sp_utilizaiton) {
    double utilization = *(link_and_utilization.second);
    if (utilization > 1.0) {
      return false;
    }
  }

  return true;
}

std::unique_ptr<DemandMatrix> DemandMatrix::Scale(double factor) const {
  auto tm = make_unique<DemandMatrix>(elements_, graph_);
  for (auto& element : tm->elements_) {
    double new_load = element.demand.Mbps() * factor;
    element.demand = net::Bandwidth::FromMBitsPerSecond(new_load);
  }

  return tm;
}

std::unique_ptr<DemandMatrix> DemandMatrix::Filter(
    std::function<bool(const DemandMatrixElement& element)> filter) const {
  std::vector<DemandMatrixElement> new_elements;
  for (const DemandMatrixElement& element : elements_) {
    if (filter(element)) {
      continue;
    }

    new_elements.emplace_back(element);
  }

  return make_unique<DemandMatrix>(new_elements, graph_);
}

std::unique_ptr<DemandMatrix> DemandMatrix::RemovePairs(
    const std::set<NodePair>& pairs) const {
  return Filter([&pairs](const DemandMatrixElement& element) {
    return ContainsKey(pairs, std::make_pair(element.src, element.dst));
  });
}

std::unique_ptr<DemandMatrix> DemandMatrix::IsolateFraction(
    double fraction) const {
  CHECK(fraction > 0);
  CHECK(fraction <= 1);
  nc::net::Bandwidth limit = TotalLoad() * fraction;

  std::vector<DemandMatrixElement> new_elements = elements_;
  std::sort(new_elements.begin(), new_elements.end(),
            [](const DemandMatrixElement& lhs, const DemandMatrixElement& rhs) {
              return lhs.demand > rhs.demand;
            });

  size_t i;
  nc::net::Bandwidth total = nc::net::Bandwidth::Zero();
  for (i = 0; i < new_elements.size(); ++i) {
    total += new_elements[i].demand;
    if (total > limit) {
      break;
    }
  }

  CHECK(i <= new_elements.size());
  size_t diff = new_elements.size() - i;
  new_elements.erase(std::prev(new_elements.end(), diff), new_elements.end());
  return make_unique<DemandMatrix>(new_elements, graph_);
}

std::unique_ptr<DemandMatrix> DemandMatrix::IsolateLargest() const {
  net::Bandwidth max_rate = net::Bandwidth::Zero();
  const DemandMatrixElement* element_ptr = nullptr;
  for (const DemandMatrixElement& element : elements_) {
    if (element.demand > max_rate) {
      element_ptr = &element;
      max_rate = element.demand;
    }
  }
  CHECK(element_ptr != nullptr);

  std::vector<DemandMatrixElement> new_elements = {*element_ptr};
  return make_unique<DemandMatrix>(new_elements, graph_);
}

static net::GraphLinkMap<double> GetCapacities(
    const net::GraphLinkSet& to_exclude, double capacity_multiplier,
    const net::GraphStorage& graph) {
  net::GraphLinkMap<double> out;
  for (net::GraphLinkIndex link : graph.AllLinks()) {
    if (to_exclude.Contains(link)) {
      continue;
    }

    double capacity =
        graph.GetLink(link)->bandwidth().Mbps() * capacity_multiplier;
    out[link] = capacity;
  }

  return out;
}

std::pair<net::Bandwidth, double> DemandMatrix::GetMaxFlow(
    const net::GraphLinkSet& to_exclude, double capacity_multiplier) const {
  MaxFlowSingleCommodityFlowProblem max_flow_problem(
      GetCapacities(to_exclude, capacity_multiplier, *graph_), graph_);
  for (const DemandMatrixElement& element : elements_) {
    max_flow_problem.AddDemand(element.src, element.dst, element.demand.Mbps());
  }

  double max_flow_mbps = 0;
  max_flow_problem.GetMaxFlow(&max_flow_mbps);
  return std::make_pair(net::Bandwidth::FromMBitsPerSecond(max_flow_mbps),
                        max_flow_problem.MaxDemandScaleFactor());
}

bool DemandMatrix::IsFeasible(const net::GraphLinkSet& to_exclude,
                              double capacity_multiplier) const {
  MaxFlowSingleCommodityFlowProblem max_flow_problem(
      GetCapacities(to_exclude, capacity_multiplier, *graph_), graph_);
  for (const DemandMatrixElement& element : elements_) {
    max_flow_problem.AddDemand(element.src, element.dst, element.demand.Mbps());
  }

  return max_flow_problem.IsFeasible();
}

double DemandMatrix::MaxCommodityScaleFactor(
    const net::GraphLinkSet& to_exclude, double capacity_multiplier) const {
  CHECK(TotalLoad() != nc::net::Bandwidth::Zero());
  MinMaxProblem max_flow_problem(
      graph_, GetCapacities(to_exclude, capacity_multiplier, *graph_), true);
  for (const DemandMatrixElement& element : elements_) {
    max_flow_problem.AddDemand(element.src, element.dst, element.demand.Mbps());
  }

  double link_utilization = max_flow_problem.Solve();
  return 1.0 / link_utilization;
}

bool DemandMatrix::ResilientToFailures() const {
  for (net::GraphLinkIndex link : graph_->AllLinks()) {
    if (!IsFeasible({link}, 1.0)) {
      return false;
    }
  }

  return true;
}

std::string DemandMatrix::ToString() const {
  std::vector<double> sp_utilizations;
  nc::net::GraphLinkMap<double> per_link_utilization = SPUtilization();
  for (const auto& link_and_utilization : per_link_utilization) {
    sp_utilizations.emplace_back(*link_and_utilization.second);
  }
  std::vector<double> sp_utilization_percentiles =
      Percentiles(&sp_utilizations, 10);

  std::vector<double> demands;
  for (const auto& element : elements_) {
    demands.emplace_back(element.demand.Mbps());
  }
  std::vector<double> demand_percentiles = Percentiles(&demands, 10);

  std::string out;
  nc::StrAppend(
      &out,
      nc::StrCat(
          "TM with ", static_cast<uint64_t>(elements_.size()),
          " demands, scale factor ", MaxCommodityScaleFactor({}, 1.0),
          "\nSP link utilizations: ", nc::Join(sp_utilization_percentiles, ","),
          "\nDemands (in Mbps): ", nc::Join(demand_percentiles, ","), "\n"));
  return out;
}

// Parses a line of the form <tag> <count> and returns count.
static uint32_t ParseCountOrDie(const std::string& tag,
                                const std::string& line) {
  std::vector<std::string> line_split = Split(line, " ");
  CHECK(line_split.size() == 2);
  CHECK(line_split[0] == tag);

  uint32_t count;
  CHECK(safe_strtou32(line_split[1], &count));
  return count;
}

std::unique_ptr<DemandMatrix> DemandMatrix::LoadRepetitaStringOrDie(
    const std::string& matrix_string,
    const std::vector<std::string>& node_names,
    const net::GraphStorage* graph) {
  std::vector<std::string> all_lines = Split(matrix_string, "\n");
  auto it = all_lines.begin();

  const std::string& demands_line = *it;
  uint32_t num_demands = ParseCountOrDie("DEMANDS", demands_line);

  // Skip free form line.
  ++it;

  std::map<std::pair<net::GraphNodeIndex, net::GraphNodeIndex>, double>
      total_demands;
  for (uint32_t i = 0; i < num_demands; ++i) {
    ++it;

    std::vector<std::string> line_split = Split(*it, " ");
    CHECK(line_split.size() == 4) << *it << " demand " << i;

    uint32_t src_index;
    uint32_t dst_index;
    double demand_kbps;

    CHECK(safe_strtou32(line_split[1], &src_index));
    CHECK(safe_strtou32(line_split[2], &dst_index));
    CHECK(safe_strtod(line_split[3], &demand_kbps));

    CHECK(src_index < node_names.size()) << src_index << " line " << *it;
    CHECK(dst_index < node_names.size()) << dst_index << " line " << *it;

    const net::GraphNodeIndex* src_ptr =
        graph->NodeFromStringOrNull(node_names[src_index]);
    const net::GraphNodeIndex* dst_ptr =
        graph->NodeFromStringOrNull(node_names[dst_index]);
    if (src_ptr == nullptr || dst_ptr == nullptr) {
      return {};
    }

    total_demands[{*src_ptr, *dst_ptr}] += demand_kbps;
  }

  std::vector<DemandMatrixElement> elements;
  for (const auto& src_and_dst_and_demand : total_demands) {
    double demand_kbps = src_and_dst_and_demand.second;
    if (demand_kbps < 1) {
      continue;
    }

    net::GraphNodeIndex src;
    net::GraphNodeIndex dst;
    std::tie(src, dst) = src_and_dst_and_demand.first;

    net::Bandwidth demand = net::Bandwidth::FromKBitsPerSecond(demand_kbps);
    elements.emplace_back(src, dst, demand);
  }

  return make_unique<DemandMatrix>(std::move(elements), graph);
}

static std::string GetPropertiesFileName(const std::string& matrix_file) {
  // Will strip the file's extension and replace it with .properties
  auto dot_location = matrix_file.find_last_of('.');
  std::string prop_file = matrix_file;
  if (dot_location != std::string::npos) {
    prop_file = matrix_file.substr(0, dot_location);
  }
  StrAppend(&prop_file, ".properties");

  return prop_file;
}

std::unique_ptr<DemandMatrix> DemandMatrix::LoadRepetitaFileOrDie(
    const std::string& matrix_file, const std::vector<std::string>& node_names,
    const net::GraphStorage* graph) {
  std::string prop_file = GetPropertiesFileName(matrix_file);

  std::unique_ptr<DemandMatrix> demand_matrix = LoadRepetitaStringOrDie(
      nc::File::ReadFileToStringOrDie(matrix_file), node_names, graph);

  if (File::Exists(prop_file)) {
    File::ReadLines(prop_file, [&demand_matrix](const std::string& line) {
      std::vector<std::string> key_and_value = nc::Split(line, " ");
      if (key_and_value.size() != 2) {
        return;
      }

      demand_matrix->UpdateProperty(key_and_value[0], key_and_value[1]);
    });
  }

  return demand_matrix;
}

std::string DemandMatrix::ToRepetitaString(
    const std::vector<std::string>& node_names) const {
  // node_names contains the nodes, as they were ordered in the topology file.
  // Need to be able to refer to them by the index in the topology file.
  net::GraphNodeMap<uint32_t> node_to_index_in_names;
  for (net::GraphNodeIndex node : graph_->AllNodes()) {
    std::string node_name = graph_->GetNode(node)->id();
    auto it = std::find(node_names.begin(), node_names.end(), node_name);
    CHECK(it != node_names.end());
    uint32_t index = std::distance(node_names.begin(), it);
    node_to_index_in_names[node] = index;
  }

  std::string out =
      StrCat("DEMANDS ", static_cast<uint64_t>(elements_.size()), "\n");
  StrAppend(&out, "label src dest bw\n");
  for (uint64_t i = 0; i < elements_.size(); ++i) {
    const DemandMatrixElement& element = elements_[i];

    StrAppend(&out,
              StrCat("demand_", i, " ",
                     node_to_index_in_names.GetValueOrDie(element.src), " ",
                     node_to_index_in_names.GetValueOrDie(element.dst), " ",
                     element.demand.Kbps(), "\n"));
  }

  return out;
}

void DemandMatrix::ToRepetitaFileOrDie(
    const std::vector<std::string>& node_names,
    const std::string& matrix_file) const {
  nc::File::WriteStringToFileOrDie(ToRepetitaString(node_names), matrix_file);
  if (properties_.empty()) {
    return;
  }

  std::string prop_file = GetPropertiesFileName(matrix_file);
  std::string prop_string;
  for (const auto& key_and_value : properties_) {
    prop_string += StrCat(key_and_value.first, " ", key_and_value.second, "\n");
  }
  nc::File::WriteStringToFileOrDie(prop_string, prop_file);
}

}  // namespace lp
}  // namespace nc
