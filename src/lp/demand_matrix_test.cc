#include "demand_matrix.h"

#include <gtest/gtest.h>
#include <set>

#include "../net/net_gen.h"

namespace nc {
namespace lp {

using namespace nc::net;

static constexpr Bandwidth kBw = Bandwidth::FromMBitsPerSecond(100);
static constexpr Delay kDelay = Delay(10);

class TmGenTest : public ::testing::Test {
 protected:
  TmGenTest()
      : graph_storage_(GenerateFullGraph(2, kBw, kDelay)),
        generator_(&graph_storage_, 1) {}

  GraphStorage graph_storage_;
  DemandGenerator generator_;
};

TEST_F(TmGenTest, BadFraction) {
  ASSERT_DEATH(generator_.Generate(0.9, 0.5), ".*");
  ASSERT_DEATH(generator_.Generate(1.2, 1.2), ".*");
  ASSERT_DEATH(generator_.Generate(-1.2, 0.5), ".*");
  ASSERT_DEATH(generator_.Generate(1.2, -0.5), ".*");
}

TEST_F(TmGenTest, Simple) {
  auto tm = generator_.Generate(1.2, 0.5);
  CHECK(tm);
  LOG(INFO) << tm->ToString();
}

}  // namespace lp
}  // naemspace nc
