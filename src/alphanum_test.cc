#include "alphanum.h"

#include <cstring>
#include "gtest/gtest.h"

namespace nc {
namespace {

TEST(AlphanumSort, Compare) {
  ASSERT_TRUE(alphanum_comp("", "") == 0);
  ASSERT_TRUE(alphanum_comp("", "a") < 0);
  ASSERT_TRUE(alphanum_comp("a", "") > 0);
  ASSERT_TRUE(alphanum_comp("a", "a") == 0);
  ASSERT_TRUE(alphanum_comp("", "9") < 0);
  ASSERT_TRUE(alphanum_comp("9", "") > 0);
  ASSERT_TRUE(alphanum_comp("1", "1") == 0);
  ASSERT_TRUE(alphanum_comp("1", "2") < 0);
  ASSERT_TRUE(alphanum_comp("3", "2") > 0);
  ASSERT_TRUE(alphanum_comp("a1", "a1") == 0);
  ASSERT_TRUE(alphanum_comp("a1", "a2") < 0);
  ASSERT_TRUE(alphanum_comp("a2", "a1") > 0);
  ASSERT_TRUE(alphanum_comp("a1a2", "a1a3") < 0);
  ASSERT_TRUE(alphanum_comp("a1a2", "a1a0") > 0);
  ASSERT_TRUE(alphanum_comp("134", "122") > 0);
  ASSERT_TRUE(alphanum_comp("12a3", "12a3") == 0);
  ASSERT_TRUE(alphanum_comp("12a1", "12a0") > 0);
  ASSERT_TRUE(alphanum_comp("12a1", "12a2") < 0);
  ASSERT_TRUE(alphanum_comp("a", "aa") < 0);
  ASSERT_TRUE(alphanum_comp("aaa", "aa") > 0);
  ASSERT_TRUE(alphanum_comp("Alpha 2", "Alpha 2") == 0);
  ASSERT_TRUE(alphanum_comp("Alpha 2", "Alpha 2A") < 0);
  ASSERT_TRUE(alphanum_comp("Alpha 2 B", "Alpha 2") > 0);

  ASSERT_TRUE(alphanum_comp(1, 1) == 0);
  ASSERT_TRUE(alphanum_comp(1, 2) < 0);
  ASSERT_TRUE(alphanum_comp(2, 1) > 0);
  ASSERT_TRUE(alphanum_comp(1.2, 3.14) < 0);
  ASSERT_TRUE(alphanum_comp(3.14, 2.71) > 0);
  ASSERT_TRUE(alphanum_comp(true, true) == 0);
  ASSERT_TRUE(alphanum_comp(true, false) > 0);
  ASSERT_TRUE(alphanum_comp(false, true) < 0);

  std::string str("Alpha 2");
  ASSERT_TRUE(alphanum_comp(str, "Alpha 2") == 0);
  ASSERT_TRUE(alphanum_comp(str, "Alpha 2A") < 0);
  ASSERT_TRUE(alphanum_comp("Alpha 2 B", str) > 0);

  ASSERT_TRUE(alphanum_comp(str, strdup("Alpha 2")) == 0);
  ASSERT_TRUE(alphanum_comp(str, strdup("Alpha 2A")) < 0);
  ASSERT_TRUE(alphanum_comp(strdup("Alpha 2 B"), str) > 0);
}

TEST(AlphanumSort, Sort) {
  std::vector<std::string> unsorted = {"0", "1", "10", "2", "120", "101", "A"};
  std::vector<std::string> sorted = {"0", "1", "2", "10", "101", "120", "A"};
  std::sort(unsorted.begin(), unsorted.end(), alphanum_less<std::string>());
  ASSERT_EQ(unsorted, sorted);
}

}  // namespace
}  // namespace nc
