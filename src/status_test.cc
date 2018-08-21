#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "logging.h"
#include "status.h"
#include "gtest/gtest.h"

namespace nc {
namespace {

TEST(Status, Empty) {
  Status status;
  EXPECT_EQ(error::OK, Status::OK.error_code());
  EXPECT_EQ("OK", Status::OK.ToString());
}

TEST(Status, GenericCodes) {
  EXPECT_EQ(error::OK, Status::OK.error_code());
  EXPECT_EQ(error::CANCELLED, Status::CANCELLED.error_code());
  EXPECT_EQ(error::UNKNOWN, Status::UNKNOWN.error_code());
}

TEST(Status, ConstructorZero) {
  Status status(error::OK, "msg");
  EXPECT_TRUE(status.ok());
  EXPECT_EQ("OK", status.ToString());
}

TEST(Status, CheckOK) {
  Status status;
  CHECK_OK(status);
  CHECK_OK(status) << "Failed";
  DCHECK_OK(status) << "Failed";
}

TEST(Status, ErrorMessage) {
  Status status(error::INVALID_ARGUMENT, "");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ("", status.error_message().ToString());
  EXPECT_EQ("INVALID_ARGUMENT", status.ToString());
  status = Status(error::INVALID_ARGUMENT, "msg");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ("msg", status.error_message().ToString());
  EXPECT_EQ("INVALID_ARGUMENT: msg", status.ToString());
  status = Status(error::OK, "msg");
  EXPECT_TRUE(status.ok());
  EXPECT_EQ("", status.error_message().ToString());
  EXPECT_EQ("OK", status.ToString());
}

TEST(Status, Copy) {
  Status a(error::UNKNOWN, "message");
  Status b(a);
  ASSERT_EQ(a.ToString(), b.ToString());
}

TEST(Status, Assign) {
  Status a(error::UNKNOWN, "message");
  Status b;
  b = a;
  ASSERT_EQ(a.ToString(), b.ToString());
}

TEST(Status, AssignEmpty) {
  Status a(error::UNKNOWN, "message");
  Status b;
  a = b;
  ASSERT_EQ(std::string("OK"), a.ToString());
  ASSERT_TRUE(b.ok());
  ASSERT_TRUE(a.ok());
}

TEST(Status, EqualsOK) { ASSERT_EQ(Status::OK, Status()); }

TEST(Status, EqualsSame) {
  const Status a = Status(error::CANCELLED, "message");
  const Status b = Status(error::CANCELLED, "message");
  ASSERT_EQ(a, b);
}

TEST(Status, EqualsCopy) {
  const Status a = Status(error::CANCELLED, "message");
  const Status b = a;
  ASSERT_EQ(a, b);
}

TEST(Status, EqualsDifferentCode) {
  const Status a = Status(error::CANCELLED, "message");
  const Status b = Status(error::UNKNOWN, "message");
  ASSERT_NE(a, b);
}

TEST(Status, EqualsDifferentMessage) {
  const Status a = Status(error::CANCELLED, "message");
  const Status b = Status(error::CANCELLED, "another");
  ASSERT_NE(a, b);
}

}  // namepsace
}  // namespace nc
