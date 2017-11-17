#include "mc_flow.h"

#include <map>
#include <memory>
#include <string>

#include "../logging.h"
#include "../map_util.h"
#include "lp.h"

namespace nc {
namespace lp {

FlowProblem::FlowProblem(const net::GraphLinkMap<double>& link_capacities,
                         const net::GraphStorage* graph)
    : graph_(graph), link_capacities_(link_capacities) {
  for (const auto& link_index_and_capacity : link_capacities) {
    net::GraphLinkIndex link_index = link_index_and_capacity.first;
    const net::GraphLink* link = graph_->GetLink(link_index);

    net::GraphNodeIndex out = link->src();
    net::GraphNodeIndex in = link->dst();
    adjacency_map_[out].first.emplace_back(link_index);
    adjacency_map_[in].second.emplace_back(link_index);
  }
}

MaxFlowProblem::VarMap MaxFlowProblem::GetLinkToVariableMap(
    Problem* problem, std::vector<ProblemMatrixElement>* problem_matrix) {
  VarMap link_to_variables;

  // There will be a variable per link.
  for (const auto& link_and_capacity : link_capacities()) {
    net::GraphLinkIndex link_index = link_and_capacity.first;
    double capacity = *link_and_capacity.second;

    // One constraint per link to make sure the flow over it fits the capacity
    // of the link.
    ConstraintIndex link_constraint;
    link_constraint = problem->AddConstraint();
    problem->SetConstraintRange(link_constraint, 0, capacity);

    // One variable per link for the flow.
    VariableIndex var = problem->AddVariable();
    problem->SetVariableRange(var, 0, Problem::kInifinity);
    link_to_variables[link_index] = var;
    problem_matrix->emplace_back(link_constraint, var, 1.0);
  }

  return link_to_variables;
}

void MaxFlowProblem::AddFlowConservationConstraints(
    const VarMap& link_to_variables, Problem* problem,
    std::vector<ProblemMatrixElement>* problem_matrix) {
  for (const auto& node_and_adj_lists : adjacency_map()) {
    net::GraphNodeIndex node = node_and_adj_lists.first;

    const std::vector<net::GraphLinkIndex>& edges_out =
        node_and_adj_lists.second->first;
    const std::vector<net::GraphLinkIndex>& edges_in =
        node_and_adj_lists.second->second;

    // For all nodes except the source and the sink the sum of the flow into
    // the node should be equal to the sum of the flow out. All flow into the
    // source should be 0. All flow out of the sink should be 0.
    ConstraintIndex flow_conservation_constraint = problem->AddConstraint();
    problem->SetConstraintRange(flow_conservation_constraint, 0, 0);

    if (sources_.Contains(node)) {
      // Traffic that leaves the source.
      ConstraintIndex source_load_constraint = problem->AddConstraint();
      problem->SetConstraintRange(source_load_constraint, 0.0,
                                  Problem::kInifinity);

      for (net::GraphLinkIndex edge_out : edges_out) {
        VariableIndex var = link_to_variables.GetValueOrDie(edge_out);
        problem_matrix->emplace_back(source_load_constraint, var, 1.0);
      }

      for (net::GraphLinkIndex edge_in : edges_in) {
        VariableIndex var = link_to_variables.GetValueOrDie(edge_in);
        problem_matrix->emplace_back(source_load_constraint, var, -1.0);
      }
    } else if (sinks_.Contains(node)) {
      for (net::GraphLinkIndex edge_out : edges_out) {
        VariableIndex var = link_to_variables.GetValueOrDie(edge_out);
        problem_matrix->emplace_back(flow_conservation_constraint, var, 1.0);
      }
    } else {
      for (net::GraphLinkIndex edge_out : edges_out) {
        VariableIndex var = link_to_variables.GetValueOrDie(edge_out);
        problem_matrix->emplace_back(flow_conservation_constraint, var, -1.0);
      }

      for (net::GraphLinkIndex edge_in : edges_in) {
        VariableIndex var = link_to_variables.GetValueOrDie(edge_in);
        problem_matrix->emplace_back(flow_conservation_constraint, var, 1.0);
      }
    }
  }
}

bool MaxFlowProblem::GetMaxFlow(double* max_flow) {
  Problem problem(MAXIMIZE);
  std::vector<ProblemMatrixElement> problem_matrix;
  VarMap link_to_variables = GetLinkToVariableMap(&problem, &problem_matrix);
  AddFlowConservationConstraints(link_to_variables, &problem, &problem_matrix);

  // Records net total flow per variable.
  std::map<VariableIndex, double> totals;
  for (const auto& node_and_adj_lists : adjacency_map()) {
    net::GraphNodeIndex node = node_and_adj_lists.first;
    const std::vector<net::GraphLinkIndex>& edges_out =
        node_and_adj_lists.second->first;
    const std::vector<net::GraphLinkIndex>& edges_in =
        node_and_adj_lists.second->second;

    double flow_from_source;
    if (sources_.Contains(node)) {
      for (net::GraphLinkIndex edge_out : edges_out) {
        VariableIndex var = link_to_variables.GetValueOrDie(edge_out);
        totals[var] += 1.0;
      }

      for (net::GraphLinkIndex edge_in : edges_in) {
        VariableIndex var = link_to_variables.GetValueOrDie(edge_in);
        totals[var] -= 1.0;
      }
    }
  }

  for (const auto& var_index_and_total : totals) {
    VariableIndex var = var_index_and_total.first;
    double total = var_index_and_total.second;
    problem.SetObjectiveCoefficient(var, total);
  }

  std::unique_ptr<Solution> solution = problem.Solve();
  if (solution->type() != lp::OPTIMAL && solution->type() != lp::FEASIBLE) {
    return false;
  }

  *max_flow = solution->ObjectiveValue();
  return true;
}

SingleCommodityFlowProblem::SingleCommodityFlowProblem(
    const net::GraphLinkMap<double>& link_capacities,
    const net::GraphStorage* graph)
    : FlowProblem(link_capacities, graph) {}

SingleCommodityFlowProblem::SingleCommodityFlowProblem(
    const SingleCommodityFlowProblem& other, double scale_factor,
    double increment)
    : FlowProblem(other.link_capacities(), other.graph_),
      demands_(other.demands_) {
  for (auto node_and_demands : demands_) {
    std::vector<SrcAndLoad>& loads_for_destination = *node_and_demands.second;
    for (SrcAndLoad& load : loads_for_destination) {
      load.second = load.second * scale_factor + increment;
    }
  }
}

SingleCommodityFlowProblem::VarMap
SingleCommodityFlowProblem::GetLinkToVariableMap(
    Problem* problem, std::vector<ProblemMatrixElement>* problem_matrix,
    bool add_link_constraints) {
  VarMap link_to_variables;

  // There will be a variable per-link per-destination.
  for (const auto& link_and_capacity : link_capacities()) {
    net::GraphLinkIndex link_index = link_and_capacity.first;
    double capacity = *link_and_capacity.second;

    // One constraint per link to make sure the sum of all commodities over it
    // fit the capacity of the link.
    ConstraintIndex link_constraint;
    if (add_link_constraints) {
      link_constraint = problem->AddConstraint();
      problem->SetConstraintRange(link_constraint, 0, capacity);
    }

    for (const auto& dst_index_and_commodities : demands_) {
      net::GraphNodeIndex dst_index = dst_index_and_commodities.first;
      VariableIndex var = problem->AddVariable();
      problem->SetVariableRange(var, 0, Problem::kInifinity);
      link_to_variables[link_index][dst_index] = var;

      if (add_link_constraints) {
        problem_matrix->emplace_back(link_constraint, var, 1.0);
      }
    }
  }

  return link_to_variables;
}

SingleCommodityFlowProblem::VarMap
SingleCommoditySingleSinkFlowProblem::GetLinkToVariableMap(
    Problem* problem, std::vector<ProblemMatrixElement>* problem_matrix) {
  VarMap link_to_variables;

  // There will be a variable per-link per-destination.
  for (const auto& link_and_capacity : link_capacities()) {
    net::GraphLinkIndex link_index = link_and_capacity.first;
    double capacity = *link_and_capacity.second;
    ConstraintIndex link_constraint = problem->AddConstraint();

    problem->SetConstraintRange(link_constraint, 0, capacity);
    for (const auto& src_index_and_demand : demands_) {
      net::GraphNodeIndex src_index = src_index_and_demand.first;
      VariableIndex var = problem->AddVariable();
      problem->SetVariableRange(var, 0, Problem::kInifinity);
      link_to_variables[link_index][src_index] = var;

      problem_matrix->emplace_back(link_constraint, var, 1.0);
    }
  }

  return link_to_variables;
}

static VariableIndex GetVar(const SingleCommodityFlowProblem::VarMap& var_map,
                            net::GraphLinkIndex edge,
                            net::GraphNodeIndex dst_index) {
  const auto& vars = var_map[edge];
  return vars[dst_index];
}

static bool IsSource(const std::vector<SrcAndLoad>& commodities,
                     net::GraphNodeIndex index, double* load) {
  for (const auto& src_and_load : commodities) {
    if (src_and_load.first == index) {
      *load = src_and_load.second;
      return true;
    }
  }

  return false;
}

void SingleCommodityFlowProblem::AddFlowConservationConstraints(
    const VarMap& link_to_variables, Problem* problem,
    std::vector<ProblemMatrixElement>* problem_matrix) {
  // Per-commodity flow conservation.
  for (const auto& dst_index_and_commodities : demands_) {
    net::GraphNodeIndex dst_index = dst_index_and_commodities.first;
    const std::vector<SrcAndLoad>& commodities =
        *dst_index_and_commodities.second;

    for (const auto& node_and_adj_lists : adjacency_map()) {
      net::GraphNodeIndex node = node_and_adj_lists.first;

      const std::vector<net::GraphLinkIndex>& edges_out =
          node_and_adj_lists.second->first;
      const std::vector<net::GraphLinkIndex>& edges_in =
          node_and_adj_lists.second->second;

      // For all nodes except the source and the sink the sum of the flow into
      // the node should be equal to the sum of the flow out. All flow into the
      // source should be 0. All flow out of the sink should be 0.
      ConstraintIndex flow_conservation_constraint = problem->AddConstraint();
      problem->SetConstraintRange(flow_conservation_constraint, 0, 0);

      double flow_from_source;
      if (IsSource(commodities, node, &flow_from_source)) {
        // Traffic that leaves the source should sum up to at least the demand
        // of the commodity.
        ConstraintIndex source_load_constraint = problem->AddConstraint();
        problem->SetConstraintRange(source_load_constraint, flow_from_source,
                                    Problem::kInifinity);

        for (net::GraphLinkIndex edge_out : edges_out) {
          VariableIndex var = GetVar(link_to_variables, edge_out, dst_index);
          problem_matrix->emplace_back(source_load_constraint, var, 1.0);
        }

        for (net::GraphLinkIndex edge_in : edges_in) {
          VariableIndex var = GetVar(link_to_variables, edge_in, dst_index);
          problem_matrix->emplace_back(source_load_constraint, var, -1.0);
        }

      } else if (node == dst_index) {
        for (net::GraphLinkIndex edge_out : edges_out) {
          VariableIndex var = GetVar(link_to_variables, edge_out, dst_index);
          problem_matrix->emplace_back(flow_conservation_constraint, var, 1.0);
        }
      } else {
        for (net::GraphLinkIndex edge_out : edges_out) {
          VariableIndex var = GetVar(link_to_variables, edge_out, dst_index);
          problem_matrix->emplace_back(flow_conservation_constraint, var, -1.0);
        }

        for (net::GraphLinkIndex edge_in : edges_in) {
          VariableIndex var = GetVar(link_to_variables, edge_in, dst_index);
          problem_matrix->emplace_back(flow_conservation_constraint, var, 1.0);
        }
      }
    }
  }
}

std::map<SrcAndDst, std::vector<FlowAndPath>>
SingleCommodityFlowProblem::RecoverPathsFromSolution(
    const VarMap& link_to_variables, const lp::Solution& solution) {
  std::map<SrcAndDst, std::vector<FlowAndPath>> out;

  // Have to create a separate problem for each destination, as
  // SingleCommodityFlowProblem groups demands by destination.
  for (const auto& dst_and_srcs : demands_) {
    net::GraphNodeIndex dst = dst_and_srcs.first;
    const std::vector<SrcAndLoad>& sources_and_loads = *dst_and_srcs.second;

    // The flow that comes out of sources for the destination. This is more than
    // or equal to the demand in demands_.
    net::GraphNodeMap<double> flow_exiting_sources;

    net::GraphLinkMap<double> capacities_for_destination;
    for (const auto& link_and_variables : link_to_variables) {
      net::GraphLinkIndex link_index = link_and_variables.first;
      const net::GraphLink* link = graph_->GetLink(link_index);

      const net::GraphNodeMap<VariableIndex>& dst_to_variable =
          *link_and_variables.second;

      // This variable in the solution will have the load on the link that is
      // destined for 'dst'.
      VariableIndex var = dst_to_variable.GetValueOrDie(dst);
      double flow_over_link = solution.VariableValue(var);
      if (flow_over_link == 0) {
        continue;
      }

      capacities_for_destination[link_index] = flow_over_link;
      flow_exiting_sources[link->src()] += flow_over_link;
      flow_exiting_sources[link->dst()] -= flow_over_link;
      //      LOG(INFO) << link->ToStringNoPorts() << " -> " << flow_over_link;
    }

    //    LOG(INFO) << "demands to " << graph_->GetNode(dst)->id();
    SingleCommoditySingleSinkFlowProblem sub_problem(capacities_for_destination,
                                                     dst, graph_);
    for (const auto& src_and_load : sources_and_loads) {
      net::GraphNodeIndex src = src_and_load.first;
      double flow_out = flow_exiting_sources.GetValueOrDie(src);
      double demand = src_and_load.second == 0 ? flow_out : src_and_load.second;
      sub_problem.AddDemand(src, demand);
      //      LOG(INFO) << "from " << graph_->GetNode(src)->id() << " demand "
      //                << demand;
    }

    net::GraphNodeMap<std::vector<FlowAndPath>> paths_in_sub_problem =
        sub_problem.GetShortestPaths();
    CHECK(!paths_in_sub_problem.Empty());

    for (auto src_and_paths : paths_in_sub_problem) {
      net::GraphNodeIndex src = src_and_paths.first;
      std::vector<FlowAndPath>& paths_for_src = *src_and_paths.second;
      out[{src, dst}] = std::move(paths_for_src);
    }
  }

  return out;
}

void SingleCommoditySingleSinkFlowProblem::AddFlowConservationConstraints(
    const VarMap& link_to_variables, Problem* problem,
    std::vector<ProblemMatrixElement>* problem_matrix) {
  // Per-commodity flow conservation.
  for (const auto& src_index_and_demand : demands_) {
    net::GraphNodeIndex src_index = src_index_and_demand.first;
    double demand = *src_index_and_demand.second;

    for (const auto& node_and_adj_lists : adjacency_map()) {
      net::GraphNodeIndex node = node_and_adj_lists.first;

      const std::vector<net::GraphLinkIndex>& edges_out =
          node_and_adj_lists.second->first;
      const std::vector<net::GraphLinkIndex>& edges_in =
          node_and_adj_lists.second->second;

      ConstraintIndex flow_conservation_constraint = problem->AddConstraint();
      problem->SetConstraintRange(flow_conservation_constraint, 0, 0);

      if (node == src_index) {
        ConstraintIndex source_load_constraint = problem->AddConstraint();
        problem->SetConstraintRange(source_load_constraint, demand,
                                    Problem::kInifinity);

        for (net::GraphLinkIndex edge_out : edges_out) {
          VariableIndex var = GetVar(link_to_variables, edge_out, src_index);
          problem_matrix->emplace_back(source_load_constraint, var, 1.0);
        }

        for (net::GraphLinkIndex edge_in : edges_in) {
          VariableIndex var = GetVar(link_to_variables, edge_in, src_index);
          problem_matrix->emplace_back(source_load_constraint, var, -1.0);
        }

      } else if (node == sink_) {
        for (net::GraphLinkIndex edge_out : edges_out) {
          VariableIndex var = GetVar(link_to_variables, edge_out, src_index);
          problem_matrix->emplace_back(flow_conservation_constraint, var, 1.0);
        }
      } else {
        for (net::GraphLinkIndex edge_out : edges_out) {
          VariableIndex var = GetVar(link_to_variables, edge_out, src_index);
          problem_matrix->emplace_back(flow_conservation_constraint, var, -1.0);
        }

        for (net::GraphLinkIndex edge_in : edges_in) {
          VariableIndex var = GetVar(link_to_variables, edge_in, src_index);
          problem_matrix->emplace_back(flow_conservation_constraint, var, 1.0);
        }
      }
    }
  }
}

void SingleCommodityFlowProblem::AddDemand(const std::string& source,
                                           const std::string& sink,
                                           double demand) {
  AddDemand(graph_->NodeFromStringOrDie(source),
            graph_->NodeFromStringOrDie(sink), demand);
}

void SingleCommodityFlowProblem::AddDemand(net::GraphNodeIndex source,
                                           net::GraphNodeIndex sink,
                                           double demand) {
  std::vector<SrcAndLoad>& src_and_loads = demands_[sink];
  for (const SrcAndLoad& src_and_load : src_and_loads) {
    CHECK(src_and_load.first != source);
  }

  src_and_loads.emplace_back(source, demand);
}

void SingleCommoditySingleSinkFlowProblem::AddDemand(const std::string& source,
                                                     double demand) {
  AddDemand(graph_->NodeFromStringOrDie(source), demand);
}

void SingleCommoditySingleSinkFlowProblem::AddDemand(net::GraphNodeIndex source,
                                                     double demand) {
  CHECK(!demands_.HasValue(source));
  demands_[source] = demand;
}

bool SingleCommodityFlowProblem::IsFeasible() {
  Problem problem(MAXIMIZE);
  std::vector<ProblemMatrixElement> problem_matrix;
  VarMap link_to_variables =
      GetLinkToVariableMap(&problem, &problem_matrix, true);
  AddFlowConservationConstraints(link_to_variables, &problem, &problem_matrix);

  // Solve the problem.
  problem.SetMatrix(problem_matrix);
  std::unique_ptr<Solution> solution = problem.Solve();
  return solution->type() == lp::OPTIMAL || solution->type() == lp::FEASIBLE;
}

static constexpr double kMaxScaleFactor = 10000000.0;
static constexpr double kStopThreshold = 0.0001;

double SingleCommodityFlowProblem::MaxDemandScaleFactor() {
  if (!IsFeasible()) {
    return 0;
  }

  bool all_zero = true;
  for (const auto& dst_index_and_commodities : demands_) {
    const std::vector<SrcAndLoad>& commodities =
        *dst_index_and_commodities.second;
    for (const SrcAndLoad& commodity : commodities) {
      if (commodity.second != 0) {
        all_zero = false;
        break;
      }
    }

    if (!all_zero) {
      break;
    }
  }

  if (all_zero) {
    return 0;
  }

  // Will do a binary search to look for the problem that is the closes to being
  // infeasible in a given number of steps.
  double min_bound = 1.0;
  double max_bound = kMaxScaleFactor;

  double curr_estimate = kMaxScaleFactor;
  while (true) {
    CHECK(max_bound >= min_bound);
    double delta = max_bound - min_bound;
    if (delta <= kStopThreshold) {
      break;
    }

    double guess = min_bound + (max_bound - min_bound) / 2;
    SingleCommodityFlowProblem test_problem(*this, guess, 0);

    bool is_feasible = test_problem.IsFeasible();
    if (is_feasible) {
      curr_estimate = guess;
      min_bound = guess;
    } else {
      max_bound = guess;
    }
  }

  if (curr_estimate == kMaxScaleFactor) {
    return 1.0;
  }

  return curr_estimate;
}

double SingleCommodityFlowProblem::MaxDemandIncrement() {
  if (!IsFeasible() || demands_.Count() == 0) {
    return 0;
  }

  // The initial increment will be the max of the cpacity of all links.
  double max_capacity = 0;
  for (const auto& link_and_capacity : link_capacities()) {
    double capacity = *link_and_capacity.second;
    max_capacity = std::max(max_capacity, capacity);
  }

  double min_bound = 1.0;
  double max_bound = max_capacity;
  double curr_estimate = max_capacity;
  while (true) {
    CHECK(max_bound >= min_bound);
    double delta = max_bound - min_bound;
    if (delta <= kStopThreshold) {
      break;
    }

    double guess = min_bound + (max_bound - min_bound) / 2;
    SingleCommodityFlowProblem test_problem(*this, 1.0, guess);

    bool is_feasible = test_problem.IsFeasible();
    if (is_feasible) {
      curr_estimate = guess;
      min_bound = guess;
    } else {
      max_bound = guess;
    }
  }

  return curr_estimate;
}

net::GraphNodeMap<std::vector<FlowAndPath>>
SingleCommoditySingleSinkFlowProblem::RecoverPaths(
    const VarMap& link_to_variables, const lp::Solution& solution) const {
  net::GraphNodeMap<std::vector<FlowAndPath>> out;

  for (const auto& src_index_and_demand : demands_) {
    net::GraphNodeIndex src_index = src_index_and_demand.first;
    double demand = *src_index_and_demand.second;

    net::GraphLinkMap<double> link_to_flow;
    for (const auto& link_and_variables : link_to_variables) {
      net::GraphLinkIndex link = link_and_variables.first;
      const net::GraphNodeMap<VariableIndex>& variables =
          *link_and_variables.second;
      CHECK(variables.HasValue(src_index));

      double flow = solution.VariableValue(variables[src_index]);
      if (flow > 0) {
        link_to_flow[link] = flow;
      }
    }

    net::Links links;
    net::GraphNodeSet nodes_set;
    std::vector<FlowAndPath>& paths = out[src_index];
    bool commodity_has_volume = demand > 0;

    double starting_flow =
        commodity_has_volume ? demand : std::numeric_limits<double>::max();
    RecoverPathsRecursive(demand, src_index, starting_flow, &link_to_flow,
                          &links, &nodes_set, &paths);
    if (!commodity_has_volume) {
      continue;
    }

    double total_flow = 0;
    for (const FlowAndPath& path : paths) {
      total_flow += path.flow();
    }
    CHECK(std::abs(total_flow - starting_flow) / total_flow < 0.01)
        << total_flow << " vs " << starting_flow;
  }

  return out;
}

double SingleCommoditySingleSinkFlowProblem::RecoverPathsRecursive(
    double demand, net::GraphNodeIndex at_node, double overall_flow,
    net::GraphLinkMap<double>* flow_over_links, net::Links* links_so_far,
    net::GraphNodeSet* nodes_so_far_set, std::vector<FlowAndPath>* out) const {
  if (at_node == sink_) {
    CHECK(overall_flow != std::numeric_limits<double>::max());
    if (demand > 0) {
      CHECK(overall_flow <= demand);
    }

    for (net::GraphLinkIndex link : *links_so_far) {
      double& remaining = flow_over_links->GetValueOrDie(link);
      remaining -= overall_flow;
    }

    auto new_path = make_unique<net::Walk>(*links_so_far, *graph_);
    out->emplace_back(overall_flow, std::move(new_path));
    //    LOG(ERROR) << "FF " << overall_flow << " "
    //               << new_path->ToStringNoPorts(*graph_);
    return 0;
  }

  const auto& adj_lists = adjacency_map().GetValueOrDie(at_node);
  const std::vector<net::GraphLinkIndex>& edges_out = adj_lists.first;
  for (net::GraphLinkIndex edge_out : edges_out) {
    if (!flow_over_links->HasValue(edge_out)) {
      continue;
    }

    const net::GraphLink* edge = graph_->GetLink(edge_out);
    double& remaining = flow_over_links->GetValueOrDie(edge_out);
    double to_take = std::min(remaining, overall_flow);
    //    LOG(ERROR) << "OF " << overall_flow << " R " << remaining << " TT "
    //               << to_take << " at " << graph_->GetNode(at_node)->id();
    if (to_take <= 0) {
      continue;
    }

    links_so_far->emplace_back(edge_out);
    net::GraphNodeIndex neighbor = edge->dst();
    if (nodes_so_far_set->Contains(neighbor)) {
      continue;
    }

    nodes_so_far_set->Insert(neighbor);
    double remainder =
        RecoverPathsRecursive(demand, neighbor, to_take, flow_over_links,
                              links_so_far, nodes_so_far_set, out);
    overall_flow -= (to_take - remainder);

    nodes_so_far_set->Remove(neighbor);
    links_so_far->pop_back();
    //    LOG(ERROR) << "Remainder " << remainder << " OF " << overall_flow << "
    //    at "
    //               << graph_->GetNode(at_node)->id();
  }

  return overall_flow;
}

net::GraphNodeMap<std::vector<FlowAndPath>>
SingleCommoditySingleSinkFlowProblem::GetShortestPaths() {
  Problem problem(MINIMIZE);
  std::vector<ProblemMatrixElement> problem_matrix;
  VarMap link_to_variables = GetLinkToVariableMap(&problem, &problem_matrix);
  AddFlowConservationConstraints(link_to_variables, &problem, &problem_matrix);

  for (const auto& link_and_sources : link_to_variables) {
    net::GraphLinkIndex link_index = link_and_sources.first;
    net::Delay link_delay = graph_->GetLink(link_index)->delay();
    double link_cost =
        std::chrono::duration<double, std::milli>(link_delay).count();

    for (const auto& source_and_var : *link_and_sources.second) {
      VariableIndex var = *source_and_var.second;
      problem.SetObjectiveCoefficient(var, link_cost);
    }
  }

  problem.SetMatrix(problem_matrix);
  std::unique_ptr<Solution> solution = problem.Solve();
  //  LOG(INFO) << "SUB " << solution->type();
  if (solution->type() != lp::OPTIMAL && solution->type() != lp::FEASIBLE) {
    return {};
  }

  return RecoverPaths(link_to_variables, *solution);
}

bool MaxFlowSingleCommodityFlowProblem::GetMaxFlow(
    double* max_flow, std::map<SrcAndDst, std::vector<FlowAndPath>>* paths) {
  Problem problem(MAXIMIZE);
  std::vector<ProblemMatrixElement> problem_matrix;
  VarMap link_to_variables =
      GetLinkToVariableMap(&problem, &problem_matrix, true);
  AddFlowConservationConstraints(link_to_variables, &problem, &problem_matrix);
  for (const auto& dst_index_and_demands : demands_) {
    net::GraphNodeIndex dst_index = dst_index_and_demands.first;
    const std::vector<SrcAndLoad>& demands = *dst_index_and_demands.second;

    // Records net total flow per variable.
    std::map<VariableIndex, double> totals;
    for (const auto& node_and_adj_lists : adjacency_map()) {
      net::GraphNodeIndex node = node_and_adj_lists.first;

      const std::vector<net::GraphLinkIndex>& edges_out =
          node_and_adj_lists.second->first;
      const std::vector<net::GraphLinkIndex>& edges_in =
          node_and_adj_lists.second->second;

      double flow_from_source;
      if (IsSource(demands, node, &flow_from_source)) {
        for (net::GraphLinkIndex edge_out : edges_out) {
          VariableIndex var = GetVar(link_to_variables, edge_out, dst_index);
          totals[var] += 1.0;
        }

        for (net::GraphLinkIndex edge_in : edges_in) {
          VariableIndex var = GetVar(link_to_variables, edge_in, dst_index);
          totals[var] -= 1.0;
        }
      }
    }

    for (const auto& var_index_and_total : totals) {
      VariableIndex var = var_index_and_total.first;
      double total = var_index_and_total.second;
      problem.SetObjectiveCoefficient(var, total);
    }
  }

  // Solve the problem.
  problem.SetMatrix(problem_matrix);
  std::unique_ptr<Solution> solution = problem.Solve();
  if (solution->type() != lp::OPTIMAL && solution->type() != lp::FEASIBLE) {
    return false;
  }

  //  LOG(INFO) << "flow " << solution->ObjectiveValue();
  *max_flow = solution->ObjectiveValue();
  if (paths == nullptr) {
    return true;
  }

  *paths = RecoverPathsFromSolution(link_to_variables, *solution);
  return true;
}

MaxFlowSingleCommodityFlowProblem::MaxFlowSingleCommodityFlowProblem(
    const net::GraphLinkMap<double>& link_capacities,
    const net::GraphStorage* graph)
    : SingleCommodityFlowProblem(link_capacities, graph) {}

}  // namespace lp
}  // namespace ncode
