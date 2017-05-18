#ifndef NCODE_NET_GRAPH_QUERY_H
#define NCODE_NET_GRAPH_QUERY_H

#include <memory>
#include <vector>

#include "algorithm.h"
#include "net_common.h"

namespace nc {
namespace net {

class PathExpression {
 public:
  PathExpression(GraphNodeIndex src, GraphNodeIndex dst,
                 const GraphStorage* graph)
      : src_(src), dst_(dst), graph_(graph) {}

  // A convenience constructor that constructs this expression with a single set
  // of nodes in the visit order.
  PathExpression(GraphNodeIndex src, GraphNodeIndex dst,
                 const GraphNodeSet& initial_set, const GraphStorage* graph)
      : src_(src), dst_(dst), graph_(graph) {
    constraints_.insert({{}, {initial_set}});
  }

  PathExpression(GraphNodeIndex src, GraphNodeIndex dst,
                 const ExclusionSet& exclusion_set, const GraphStorage* graph)
      : src_(src), dst_(dst), graph_(graph) {
    constraints_.insert({exclusion_set, {}});
  }

  // Returns a generator capable of generating the K shortest paths that comply
  // with this expression.
  std::unique_ptr<DisjunctKShortestPathsGenerator> Evaluate() const;

  // Produces an expression which is the AND of this expression and another one.
  std::unique_ptr<PathExpression> And(const PathExpression& other) const;

  // Produces an expression which is the OR of this expression and another one.
  std::unique_ptr<PathExpression> Or(const PathExpression& other) const;

  // Produces an expression in which the nodes to visit in 'other' follow the
  // nodes to visit in this one.
  std::unique_ptr<PathExpression> Then(const PathExpression& other) const;

  // If this expression is satisfiable, returns an expression equvalent to this
  // one. If not returns one equivalent to 'other'.
  std::unique_ptr<PathExpression> Fallback(const PathExpression& other) const;

  const std::set<ConstraintSet>& constraints() const { return constraints_; }

  // A simple rule is one that has only one constraint and that constraint has
  // only one set to visit.
  bool IsSimple() const;

  friend bool operator==(const PathExpression& lhs, const PathExpression& rhs) {
    return lhs.src_ == rhs.src_ && lhs.dst_ == rhs.dst_ &&
           lhs.constraints_ == rhs.constraints_ && lhs.graph_ == lhs.graph_;
  }

 private:
  void CheckSameContext(const PathExpression& other) const;

  // The source and the destination are needed as the results of operations on
  // this rule may depend on them.
  GraphNodeIndex src_;
  GraphNodeIndex dst_;

  // The disjunction of multiple constraint sets.
  std::set<ConstraintSet> constraints_;

  // The graph.
  const GraphStorage* graph_;

  DISALLOW_COPY_AND_ASSIGN(PathExpression);
};

}  // namespace net
}  // namespace nc
#endif
