#include "logging.h"

#include <sys/errno.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "file.h"
#include "gtest/gtest.h"
#include "strutil.h"
#include "substitute.h"

namespace nc {
namespace {
std::vector<std::string> captured_messages_;
static int original_stderr_ = -1;
static std::string stderr_capture_filename_;

void CaptureTestStderr() {
  CHECK_EQ(original_stderr_, -1) << "Already capturing.";

  stderr_capture_filename_ = File::WorkingDirectoryOrDie() + "/captured_stderr";

  int fd =
      open(stderr_capture_filename_.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0777);
  CHECK(fd >= 0) << "open: " << strerror(errno);

  original_stderr_ = dup(2);
  close(2);
  dup2(fd, 2);
  close(fd);
}

std::string GetCapturedTestStderr() {
  CHECK_NE(original_stderr_, -1) << "Not capturing.";

  close(2);
  dup2(original_stderr_, 2);
  original_stderr_ = -1;

  std::string result = File::ReadFileToStringOrDie(stderr_capture_filename_);
  remove(stderr_capture_filename_.c_str());

  return result;
}

void CaptureLog(LogLevel level, const char* filename, int line,
                const std::string& message, LogColor color) {
  Unused(color);
  captured_messages_.emplace_back(Substitute(
      "$0 $1:$2: $3", static_cast<int>(level), filename, line, message));
}

TEST(LoggingTest, DefaultLogging) {
  CaptureTestStderr();
  int line = __LINE__;
  LOG(INFO) << "A message.";
  LOG(WARNING) << "A warning.";
  LOG(ERROR) << "An error.";

  std::string text = GetCapturedTestStderr();
  EXPECT_EQ("\r[INFO " __FILE__ ":" + SimpleItoa(line + 1) +
                "] A message.\n\r"
                "[WARNING " __FILE__ ":" +
                SimpleItoa(line + 2) +
                "] A warning.\n\r"
                "[ERROR " __FILE__ ":" +
                SimpleItoa(line + 3) + "] An error.\n",
            text);
}

TEST(LoggingTest, NullLogging) {
  LogHandler* old_handler = SetLogHandler(NULL);

  CaptureTestStderr();
  LOG(INFO) << "A message.";
  LOG(WARNING) << "A warning.";
  LOG(ERROR) << "An error.";

  EXPECT_TRUE(SetLogHandler(old_handler) == NULL);

  std::string text = GetCapturedTestStderr();
  EXPECT_EQ("", text);
}

TEST(LoggingTest, CaptureLogging) {
  captured_messages_.clear();

  LogHandler* old_handler = SetLogHandler(&CaptureLog);

  int start_line = __LINE__;
  LOG(ERROR) << "An error.";
  LOG(WARNING) << "A warning.";

  EXPECT_TRUE(SetLogHandler(old_handler) == &CaptureLog);

  ASSERT_EQ(2ul, captured_messages_.size());
  EXPECT_EQ("2 " __FILE__ ":" + SimpleItoa(start_line + 1) + ": An error.",
            captured_messages_[0]);
  EXPECT_EQ("1 " __FILE__ ":" + SimpleItoa(start_line + 2) + ": A warning.",
            captured_messages_[1]);
}

}  // namespace
}  // namespace nc
