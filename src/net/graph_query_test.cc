#include "graph_query.h"

#include <chrono>
#include <initializer_list>
#include <vector>

#include "test_common.h"
#include "gtest/gtest.h"
#include "net_gen.h"

namespace nc {
namespace net {
namespace {

static constexpr Bandwidth kBw = Bandwidth::FromBitsPerSecond(100);

class Braess : public ::testing::Test, public test::Base {
 protected:
  Braess() : Base(GenerateBraess(kBw)) {}
};

TEST_F(Braess, SimpleRule) {
  PathExpression rule(N("A"), N("D"), {N("B")}, &graph_);
  ASSERT_EQ(1ul, rule.constraints().size());

  auto gen = rule.Evaluate();
  ASSERT_EQ(P("[A->B, B->D]"), gen->KthShortestPathOrNull(0)->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), gen->KthShortestPathOrNull(1)->links());
  ASSERT_EQ(nullptr, gen->KthShortestPathOrNull(2));
}

TEST_F(Braess, BadSrcDst) {
  PathExpression rule_one(N("A"), N("D"), {N("B")}, &graph_);
  PathExpression rule_two(N("A"), N("C"), {N("C")}, &graph_);
  ASSERT_DEATH(rule_one.Then(rule_two), ".*");
}

TEST_F(Braess, SimpleThenRule) {
  PathExpression rule_one(N("A"), N("D"), {N("B")}, &graph_);
  PathExpression rule_two(N("A"), N("D"), {N("C")}, &graph_);
  auto new_rule = rule_one.Then(rule_two);

  const std::set<ConstraintSet>& constraints = new_rule->constraints();
  ASSERT_EQ(1ul, constraints.size());
  const std::vector<GraphNodeSet> to_visit = constraints.begin()->to_visit();
  ASSERT_EQ(2ul, to_visit.size());

  auto gen = new_rule->Evaluate();
  ASSERT_EQ(P("[A->B, B->C, C->D]"), gen->KthShortestPathOrNull(0)->links());
  ASSERT_EQ(nullptr, gen->KthShortestPathOrNull(1));
}

TEST_F(Braess, SimpleOr) {
  PathExpression rule_one(N("A"), N("D"), {N("B")}, &graph_);
  PathExpression rule_two(N("A"), N("D"), {N("C")}, &graph_);
  auto new_rule = rule_one.Or(rule_two);
  ASSERT_EQ(*new_rule, *rule_two.Or(rule_one));

  // The two ORs should have been combined into one set with two nodes.
  ASSERT_EQ(1ul, new_rule->constraints().size());
}

TEST_F(Braess, ThenOr) {
  PathExpression rule_one(N("A"), N("D"), {N("B")}, &graph_);
  PathExpression rule_two(N("A"), N("D"), {N("C")}, &graph_);
  PathExpression rule_three(N("A"), N("D"), {N("D")}, &graph_);

  auto new_rule = rule_one.Then(rule_two)->Or(rule_three);
  ASSERT_EQ(*new_rule, *(rule_three.Or(*rule_one.Then(rule_two))));

  const std::set<ConstraintSet>& constraints = new_rule->constraints();
  ASSERT_EQ(2ul, constraints.size());

  // The destination is D, and we are fine as long as we visit D, this means
  // that all paths should be ok.
  auto gen = new_rule->Evaluate();
  ASSERT_EQ(P("[A->C, C->D]"), gen->KthShortestPathOrNull(0)->links());
  ASSERT_EQ(P("[A->B, B->D]"), gen->KthShortestPathOrNull(1)->links());
  ASSERT_EQ(P("[A->B, B->C, C->D]"), gen->KthShortestPathOrNull(2)->links());
  ASSERT_EQ(nullptr, gen->KthShortestPathOrNull(3));
}

TEST_F(Braess, And) {
  PathExpression rule_one(N("A"), N("D"), {N("C")}, &graph_);
  PathExpression rule_two(N("A"), N("D"), {N("B")}, &graph_);
  auto new_rule = rule_one.And(rule_two);
  ASSERT_EQ(*new_rule, *(rule_two.And(rule_one)));

  const std::set<ConstraintSet>& constraints = new_rule->constraints();
  ASSERT_EQ(1ul, constraints.size());

  const std::vector<GraphNodeSet>& to_visit = constraints.begin()->to_visit();
  ASSERT_EQ(2ul, to_visit.size());
  ASSERT_EQ(GraphNodeSet({N("B")}), to_visit[0]);
  ASSERT_EQ(GraphNodeSet({N("C")}), to_visit[1]);
}

TEST_F(Braess, AndAvoid) {
  ExclusionSet exclusion_set;
  exclusion_set.Nodes({N("C")});

  PathExpression rule_one(N("A"), N("D"), {N("A")}, &graph_);
  PathExpression rule_two(N("A"), N("D"), {N("B")}, &graph_);
  PathExpression exclusion_rule(N("A"), N("D"), exclusion_set, &graph_);
  auto new_rule = rule_one.Then(rule_two)->And(exclusion_rule);
  ASSERT_EQ(*new_rule, *(exclusion_rule.And(*rule_one.Then(rule_two))));

  const std::set<ConstraintSet>& constraints = new_rule->constraints();
  ASSERT_EQ(1ul, constraints.size());

  // Only one set in to_visit because A is the source, upon computing the AND
  // the first set to visit will always be satisfied leaving only the second
  // one.
  const std::vector<GraphNodeSet>& to_visit = constraints.begin()->to_visit();
  ASSERT_EQ(1ul, to_visit.size());
  ASSERT_EQ(GraphNodeSet({N("B")}), to_visit[0]);
  ASSERT_EQ(constraints.begin()->exclusion_set(), exclusion_set);
}

TEST_F(Braess, AndAvoidUnmet) {
  ExclusionSet exclusion_set;
  exclusion_set.Nodes({N("D")});

  PathExpression rule_one(N("A"), N("D"), {N("B")}, &graph_);
  PathExpression exclusion_rule(N("A"), N("D"), exclusion_set, &graph_);
  auto new_rule = rule_one.And(exclusion_rule);

  // There is no path that satisfies the exclusion set.
  ASSERT_FALSE(new_rule);
  ASSERT_EQ(new_rule, exclusion_rule.And(rule_one));
}

}  // namespace
}  // namespace net
}  // namespace nc
