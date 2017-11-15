#include "demand_matrix.h"

#include <chrono>
#include <cmath>
#include <iterator>
#include <tuple>

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

double DemandMatrix::MaxCommodityScaleFractor(
    double link_capacity_multiplier) const {
  return GetMaxFlow({}, link_capacity_multiplier).second;
}

double DemandMatrix::ApproximateMaxCommodityScaleFractor(
    double capacity_multiplier, double fraction_limit) const {
  return IsolateFraction(fraction_limit)
      ->MaxCommodityScaleFractor(capacity_multiplier);
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
      &out, nc::StrCat("TM with ", static_cast<uint64_t>(elements_.size()),
                       " demands, scale factor ", MaxCommodityScaleFractor(1.0),
                       "\nSP link utilizations: ",
                       nc::Join(sp_utilization_percentiles, ","),
                       "\nDemands (in Mbps): ",
                       nc::Join(demand_percentiles, ","), "\n"));
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

std::unique_ptr<DemandMatrix> DemandMatrix::LoadRepetitaOrDie(
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

std::string DemandMatrix::ToRepetita(
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

DemandGenerator::DemandGenerator(const net::GraphStorage* graph)
    : sp_({}, graph->AdjacencyList(), nullptr, nullptr),
      graph_(graph),
      sum_inverse_delays_squared_(0) {
  for (nc::net::GraphNodeIndex src : graph_->AllNodes()) {
    for (nc::net::GraphNodeIndex dst : graph_->AllNodes()) {
      if (src == dst) {
        continue;
      }

      double distance = sp_.GetDistance(src, dst).count();
      sum_inverse_delays_squared_ += std::pow(1.0 / distance, 2);
    }
  }
}

std::unique_ptr<DemandMatrix> DemandGenerator::SinglePass(
    double locality, nc::net::Bandwidth mean, std::mt19937* rnd) const {
  // Will start by getting the total incoming/outgoing traffic at each node.
  // These will come from an exponential distribution with the given mean.
  std::exponential_distribution<double> dist(1.0 / mean.Mbps());
  nc::net::GraphNodeMap<double> incoming_traffic_Mbps;
  nc::net::GraphNodeMap<double> outgoing_traffic_Mbps;
  double total_Mbps = 0;
  double sum_in = 0;
  double sum_out = 0;
  for (nc::net::GraphNodeIndex node : graph_->AllNodes()) {
    double in = dist(*rnd);
    double out = dist(*rnd);
    incoming_traffic_Mbps[node] = in;
    outgoing_traffic_Mbps[node] = out;
    total_Mbps += in + out;
    sum_in += in;
    sum_out += out;
  }
  double sum_product = sum_in * sum_out;

  // Time to compute the demand for each aggregate.
  double total_p = 0;
  std::vector<DemandMatrixElement> elements;
  for (nc::net::GraphNodeIndex src : graph_->AllNodes()) {
    for (nc::net::GraphNodeIndex dst : graph_->AllNodes()) {
      double gravity_p = (1 - locality) *
                         incoming_traffic_Mbps.GetValueOrDie(src) *
                         outgoing_traffic_Mbps.GetValueOrDie(dst) / sum_product;
      if (src == dst) {
        total_p += gravity_p;
        continue;
      }

      double distance = sp_.GetDistance(src, dst).count();
      double locality_p =
          locality / (distance * distance * sum_inverse_delays_squared_);

      double value_Mbps = (locality_p + gravity_p) * total_Mbps;
      total_p += locality_p + gravity_p;
      elements.push_back(
          {src, dst, nc::net::Bandwidth::FromMBitsPerSecond(value_Mbps)});
    }
  }
  CHECK(std::abs(total_p - 1.0) < 0.0001) << "Not a distribution " << total_p;

  return nc::make_unique<DemandMatrix>(elements, graph_);
}

std::unique_ptr<DemandMatrix> DemandGenerator::Generate(
    double commodity_scale_factor, double locality, std::mt19937* rnd) const {
  CHECK(commodity_scale_factor >= 1.0);
  CHECK(locality >= 0);
  CHECK(locality <= 1);

  auto tm =
      SinglePass(locality, nc::net::Bandwidth::FromMBitsPerSecond(1), rnd);
  double csf = tm->ApproximateMaxCommodityScaleFractor(1.0, 0.99);
  CHECK(csf != 0);
  tm = tm->Scale(csf);
  tm = tm->Scale(1.0 / commodity_scale_factor);
  return tm;
}

}  // namespace lp
}  // namespace nc
