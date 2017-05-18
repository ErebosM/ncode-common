#include "graph_query.h"

#include <stddef.h>
#include <algorithm>

#include "../common.h"
#include "../logging.h"

namespace nc {
namespace net {

std::unique_ptr<DisjunctKShortestPathsGenerator> PathExpression::Evaluate()
    const {
  return make_unique<DisjunctKShortestPathsGenerator>(src_, dst_, *graph_,
                                                      constraints_);
}

static void ExtendToVisit(const GraphNodeSet* new_set,
                          std::vector<GraphNodeSet>* to_visit) {
  if (new_set == nullptr) {
    return;
  }

  if (std::find(to_visit->begin(), to_visit->end(), *new_set) !=
      to_visit->end()) {
    return;
  }

  to_visit->emplace_back(*new_set);
}

std::unique_ptr<PathExpression> PathExpression::And(
    const PathExpression& other) const {
  CheckSameContext(other);

  // Only know how to handle two rules with one set of constraints each. When
  // multiple sets of constraints are disjuncted it is hard to figure out
  // which combination we should go for.
  CHECK(constraints_.size() == 1 && other.constraints_.size() == 1);

  // Will get for this and the other expression the best path.
  auto gen_this = Evaluate();
  auto gen_other = other.Evaluate();

  size_t best_generator_index_this;
  const Walk* best_walk_this =
      gen_this->KthShortestPathOrNull(0, &best_generator_index_this);

  size_t best_generator_index_other;
  const Walk* best_walk_other =
      gen_other->KthShortestPathOrNull(0, &best_generator_index_other);
  if (best_walk_other == nullptr || best_walk_this == nullptr) {
    return {};
  }

  // Now we have the two best paths from each expression. We also have for
  // each expression the index of the OR clause that generated the path. The
  // result of this AND will have an exclusion set that is the combination of
  // the exclusion sets of each expression's clause.
  const KShortestPathsGenerator* best_generator_this =
      gen_this->ksp_generator(best_generator_index_this);
  const KShortestPathsGenerator* best_generator_other =
      gen_other->ksp_generator(best_generator_index_other);

  const ConstraintSet& best_constraint_set_this =
      best_generator_this->constraints();
  const ConstraintSet& best_constraint_set_other =
      best_generator_other->constraints();

  // There are no guarantees that the new exclusion set avoids both paths, but
  // whatever rule we produce needs to be compliant with both exclusion sets.
  ExclusionSet new_exclusion_set;
  new_exclusion_set.AddAll(best_constraint_set_this.exclusion_set());
  new_exclusion_set.AddAll(best_constraint_set_other.exclusion_set());
  if (best_walk_this->ContainsAny(new_exclusion_set.links_to_exclude()) ||
      best_walk_this->ContainsAny(new_exclusion_set.nodes_to_exclude(),
                                  *graph_) ||
      best_walk_other->ContainsAny(new_exclusion_set.links_to_exclude()) ||
      best_walk_other->ContainsAny(new_exclusion_set.nodes_to_exclude(),
                                   *graph_)) {
    return {};
  }

  // For each of the two best paths need to figure out what the waypoints are.
  // Waypoints are the nodes that make the path compliant---they are one or
  // more nodes from each of the sets that the path needs to visit.
  std::vector<GraphNodeIndex> waypoints_this =
      best_constraint_set_this.Waypoints(best_walk_this->links(), *graph_);
  std::vector<GraphNodeIndex> waypoints_other =
      best_constraint_set_other.Waypoints(best_walk_other->links(), *graph_);

  std::vector<GraphNodeIndex> waypoints_combined =
      CombineWaypoints(src_, dst_, new_exclusion_set, graph_->AdjacencyList(),
                       {waypoints_this, waypoints_other});

  std::vector<GraphNodeSet> to_visit;
  // Now need to reverse the process---given the waypoints need to convert
  // back to sets.
  for (GraphNodeIndex waypoint : waypoints_combined) {
    const GraphNodeSet* set =
        best_constraint_set_this.FindSetToVisitOrNull(waypoint);
    ExtendToVisit(set, &to_visit);

    set = best_constraint_set_other.FindSetToVisitOrNull(waypoint);
    ExtendToVisit(set, &to_visit);
  }

  auto to_return = make_unique<PathExpression>(src_, dst_, graph_);
  to_return->constraints_.emplace(new_exclusion_set, to_visit);
  return to_return;
}

std::unique_ptr<PathExpression> PathExpression::Or(
    const PathExpression& other) const {
  CheckSameContext(other);

  auto to_return = make_unique<PathExpression>(src_, dst_, graph_);
  std::set<ConstraintSet>& constraints_to_return = to_return->constraints_;
  constraints_to_return = constraints_;

  // If both rules are simple, then it is enough to add the nodes from the other
  // rule to the visit set of this one.
  if (IsSimple() && other.IsSimple()) {
    const std::vector<GraphNodeSet>& to_visit =
        other.constraints().begin()->to_visit();

    // Need a copy, as the set will not let us modify the item in place.
    auto it = constraints_to_return.begin();
    ConstraintSet first_constraint_set = *it;
    first_constraint_set.ExtendFirstSetToVisitOrDie(to_visit.front());

    constraints_to_return.erase(it);
    constraints_to_return.insert(first_constraint_set);
    return to_return;
  }

  constraints_to_return.insert(other.constraints_.begin(),
                               other.constraints_.end());
  return to_return;
}

bool PathExpression::IsSimple() const {
  if (constraints_.size() > 1) {
    return false;
  }

  return constraints_.begin()->to_visit().size() == 1;
}

// Returns a new constraint set that is the result of 'second' added to
// 'first'. The exclusion sets will be combined. The list of sets of nodes to
// visit from the second one will be appended to the list of sets of nodes
// from the first one.
static ConstraintSet ExtendConstraintSet(const ConstraintSet& first,
                                         const ConstraintSet& second) {
  ConstraintSet return_set = first;
  return_set.Exclude().AddAll(second.exclusion_set());

  for (const auto& set_to_visit : second.to_visit()) {
    return_set.AddToVisitSet(set_to_visit);
  }

  return return_set;
}

std::unique_ptr<PathExpression> PathExpression::Then(
    const PathExpression& other) const {
  CheckSameContext(other);

  std::set<ConstraintSet> new_constraints;
  for (const ConstraintSet& other_constraint_set : other.constraints_) {
    for (const ConstraintSet& constraint_set : constraints_) {
      new_constraints.emplace(
          ExtendConstraintSet(constraint_set, other_constraint_set));
    }
  }

  auto to_return = make_unique<PathExpression>(src_, dst_, graph_);
  to_return->constraints_ = std::move(new_constraints);
  return to_return;
}

std::unique_ptr<PathExpression> PathExpression::Fallback(
    const PathExpression& other) const {
  CheckSameContext(other);

  auto gen = Evaluate();
  const Walk* sp = gen->KthShortestPathOrNull(0);
  if (sp != nullptr) {
    auto to_return = make_unique<PathExpression>(src_, dst_, graph_);
    to_return->constraints_ = constraints_;
    return to_return;
  }

  auto to_return = make_unique<PathExpression>(src_, dst_, graph_);
  to_return->constraints_ = other.constraints_;
  return to_return;
}

void PathExpression::CheckSameContext(const PathExpression& other) const {
  CHECK(other.src_ == src_ && other.dst_ == dst_);
}

}  // namespace net
}  // namespace nc
