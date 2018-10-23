#include "fwrapper.h"

#include <gtest/gtest.h>

#include "file.h"
#include "port.h"
#include "status.h"

namespace nc {
namespace test {

static constexpr char kTestFile[] = "test_file";

class FWrapperFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    File::DeleteRecursively(kTestFile, nullptr, nullptr);
  }
};

TEST_F(FWrapperFixture, Create) {
  {
    auto result = FWrapper::Open(kTestFile, "w");
    ASSERT_TRUE(result.ok());
  }

  ASSERT_TRUE(File::Exists(kTestFile));
}

TEST_F(FWrapperFixture, Write) {
  {
    auto result = FWrapper::Open(kTestFile, "w");
    ASSERT_TRUE(result.ok());

    FWrapper fw = result.ConsumeValueOrDie();
    ASSERT_TRUE(fw.WriteUint64(2).ok());
  }

  std::string contents = File::ReadFileToStringOrDie(kTestFile);

  uint32_t value = BigEndian::FromHost64(2);
  uint32_t* contents_ptr = reinterpret_cast<uint32_t*>(&(contents[0]));
  ASSERT_EQ(*contents_ptr, value);
}

TEST_F(FWrapperFixture, FileSize) {
  {
    auto result = FWrapper::Open(kTestFile, "w");
    ASSERT_TRUE(result.ok());

    FWrapper fw = result.ConsumeValueOrDie();
    ASSERT_TRUE(fw.WriteUint64(2).ok());
  }

  {
    auto result = FWrapper::Open(kTestFile, "r");
    ASSERT_TRUE(result.ok());
    FWrapper fw = result.ConsumeValueOrDie();

    ASSERT_TRUE(fw.Seek(1).ok());
    ASSERT_EQ(8ul, fw.FileSize().ValueOrDie());
    ASSERT_EQ(1ul, fw.Tell().ValueOrDie());
  }
}

TEST_F(FWrapperFixture, WriteRead) {
  {
    auto result = FWrapper::Open(kTestFile, "w");
    ASSERT_TRUE(result.ok());

    FWrapper fw = result.ConsumeValueOrDie();
    ASSERT_TRUE(fw.WriteUint32(2).ok());
    ASSERT_TRUE(fw.WriteUint32(3).ok());
  }

  {
    auto result = FWrapper::Open(kTestFile, "r");
    ASSERT_TRUE(result.ok());

    FWrapper fw = result.ConsumeValueOrDie();
    ASSERT_EQ(2ul, fw.ReadUint32().ValueOrDie());
    ASSERT_EQ(3ul, fw.ReadUint32().ValueOrDie());
  }
}

class BulkReadFixture : public ::testing::Test {
 protected:
  BulkReadFixture()
      : p_([this](std::vector<uint8_t>::const_iterator from,
                  std::vector<uint8_t>::const_iterator to, size_t i) {
          for (auto it = from; it != to; ++it) {
            ASSERT_EQ(5, *it);
          }
          processed_.emplace_back(std::make_pair(std::distance(from, to), i));
        }) {}

  void SetUp() override {
    std::vector<uint8_t> data;
    for (size_t i = 0; i < 1000000; ++i) {
      data.emplace_back(5);
    }

    auto result = FWrapper::Open(kTestFile, "w");
    FWrapper fw = result.ConsumeValueOrDie();
    CHECK_OK(fw.Write(data.data(), data.size()));
  }

  FWrapper::Processor p_;
  std::vector<std::pair<size_t, size_t>> processed_;
};

TEST_F(BulkReadFixture, BulkReadBadSize) {
  auto result = FWrapper::Open(kTestFile, "r");
  FWrapper fw = result.ConsumeValueOrDie();

  FWrapper::Processor p;
  ASSERT_DEATH(fw.ReadBulk({{100, 0}}, p), ".*");
}

TEST_F(BulkReadFixture, BulkReadNoReads) {
  auto result = FWrapper::Open(kTestFile, "r");
  FWrapper fw = result.ConsumeValueOrDie();

  FWrapper::Processor p;
  ASSERT_TRUE(fw.ReadBulk({}, p).ok());
}

TEST_F(BulkReadFixture, BulkReadSingleRead) {
  auto result = FWrapper::Open(kTestFile, "r");
  FWrapper fw = result.ConsumeValueOrDie();

  fw.ReadBulk({{0, 100}}, p_);
  ASSERT_EQ(1ul, processed_.size());
  ASSERT_EQ(std::make_pair(static_cast<size_t>(100), static_cast<size_t>(0u)),
            processed_[0]);
}

TEST_F(BulkReadFixture, BulkReadCoalescedReads) {
  auto result = FWrapper::Open(kTestFile, "r");
  FWrapper fw = result.ConsumeValueOrDie();

  fw.ReadBulk({{4000, 1500}, {300, 3700}, {100, 200}, {0, 100}}, p_);
  ASSERT_EQ(4ul, processed_.size());
  ASSERT_EQ(std::make_pair(static_cast<size_t>(100), static_cast<size_t>(3)),
            processed_[0]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(200), static_cast<size_t>(2)),
            processed_[1]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(3700), static_cast<size_t>(1)),
            processed_[2]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(1500), static_cast<size_t>(0)),
            processed_[3]);

  std::map<uint64_t, uint64_t> model = {{5500, 1}};
  FWRapperStats stats = fw.stats();
  ASSERT_EQ(model, stats.reads.counts());
}

TEST_F(BulkReadFixture, BulkReadMixedReads) {
  auto result = FWrapper::Open(kTestFile, "r");
  FWrapper fw = result.ConsumeValueOrDie();

  // Coalesced reads from 0 to 350 and one from 500 to 1010, single read from
  // 5000 to 5050.
  fw.ReadBulk(
      {{300, 50}, {500, 500}, {5000, 500}, {1000, 10}, {100, 200}, {0, 100}},
      p_);
  ASSERT_EQ(6ul, processed_.size());
  ASSERT_EQ(std::make_pair(static_cast<size_t>(100), static_cast<size_t>(5)),
            processed_[0]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(200), static_cast<size_t>(4)),
            processed_[1]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(50), static_cast<size_t>(0)),
            processed_[2]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(500), static_cast<size_t>(1)),
            processed_[3]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(10), static_cast<size_t>(3)),
            processed_[4]);
  ASSERT_EQ(std::make_pair(static_cast<size_t>(500), static_cast<size_t>(2)),
            processed_[5]);

  std::map<uint64_t, uint64_t> model = {{350, 1}, {510, 1}, {500, 1}};
  FWRapperStats stats = fw.stats();
  ASSERT_EQ(model, stats.reads.counts());
}

TEST_F(BulkReadFixture, BadReadOverlapping) {
  auto result = FWrapper::Open(kTestFile, "r");
  FWrapper fw = result.ConsumeValueOrDie();

  // Same as above but overlap 300 -> 4750
  ASSERT_DEATH(fw.ReadBulk({{300, 4750},
                            {500, 500},
                            {5000, 500},
                            {1000, 10},
                            {100, 200},
                            {0, 100}},
                           p_),
               ".*");
}

}  // namespace test
}  // namespace nc
