#include "trie.h"

#include <string>

#include "gtest/gtest.h"

namespace nc {
namespace net {
namespace {

static std::vector<char> ToV(const std::string& str) {
  return {str.begin(), str.end()};
}

static std::vector<int> V(const std::vector<int>& in) { return in; }

class TrieTest : public ::testing::Test {
 protected:
  Trie<char, int> trie_;
};

TEST_F(TrieTest, BadAdd) { ASSERT_DEATH(trie_.Add({}, 0), ".*"); }

TEST_F(TrieTest, BadPrefix) {
  ASSERT_DEATH(trie_.SequencesWithPrefix({}), ".*");
}

TEST_F(TrieTest, SingleWord) {
  trie_.Add(ToV("BLAH"), 0);

  ASSERT_EQ(V({0}), trie_.SequencesWithPrefix(ToV("BLAH")));
  ASSERT_EQ(V({0}), trie_.SequencesWithPrefix(ToV("BLA")));
  ASSERT_EQ(V({0}), trie_.SequencesWithPrefix(ToV("B")));
  ASSERT_EQ(V({}), trie_.SequencesWithPrefix(ToV("ZLAH")));
  ASSERT_EQ(V({}), trie_.SequencesWithPrefix(ToV("BLAHZ")));
}

TEST_F(TrieTest, SingleWordTwice) {
  trie_.Add(ToV("BLAH"), 0);
  trie_.Add(ToV("BLAH"), 1);

  ASSERT_EQ(V({0, 1}), trie_.SequencesWithPrefix(ToV("BLAH")));
  ASSERT_EQ(V({}), trie_.SequencesWithPrefix(ToV("BLAHZ")));
}

TEST_F(TrieTest, MultiWord) {
  trie_.Add(ToV("BLAH"), 0);
  trie_.Add(ToV("BLAHZ"), 1);
  trie_.Add(ToV("BLAME"), 2);
  trie_.Add(ToV("ZLA"), 3);

  ASSERT_EQ(V({0, 1}), trie_.SequencesWithPrefix(ToV("BLAH")));
  ASSERT_EQ(V({0, 1, 2}), trie_.SequencesWithPrefix(ToV("BLA")));
  ASSERT_EQ(V({2}), trie_.SequencesWithPrefix(ToV("BLAM")));
  ASSERT_EQ(V({0, 1, 2}), trie_.SequencesWithPrefix(ToV("B")));
  ASSERT_EQ(V({3}), trie_.SequencesWithPrefix(ToV("ZLA")));
  ASSERT_EQ(V({1}), trie_.SequencesWithPrefix(ToV("BLAHZ")));
  ASSERT_EQ(V({}), trie_.SequencesWithPrefix(ToV("ZLAB")));
  ASSERT_EQ(V({}), trie_.SequencesWithPrefix(ToV("BLAHK")));
}

}  // namespace
}  // namespace net
}  // namespace nc
