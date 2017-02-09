#include <stdio.h>
#include "logging.h"

namespace nc {
void DefaultLogHandler(LogLevel level, const char* filename, int line,
                       const std::string& message, LogColor color) {
  static const char* level_names[] = {"INFO", "WARNING", "ERROR", "FATAL"};
  static const char* color_prefixes[] = {"\033[31m", "\033[32m", "\033[34m",
                                         "\033[97m", "\033[33m", "\033[39m"};

  // We use fprintf() instead of cerr because we want this to work at static
  // initialization time.
  if (color != LOGCOLOR_DEFAULT) {
    fprintf(stderr, "%s[%s %s:%d] %s%s\n", color_prefixes[color],
            level_names[level], filename, line, message.c_str(),
            color_prefixes[LOGCOLOR_DEFAULT]);
  } else {
    fprintf(stderr, "[%s %s:%d] %s\n", level_names[level], filename, line,
            message.c_str());
  }
  fflush(stderr);  // Needed on MSVC.
}
}  // namespace nc

namespace nc {
namespace internal {
void NullLogHandler(LogLevel /* level */, const char* /* filename */,
                    int /* line */, const std::string& /* message */,
                    LogColor /* color */) {
  // Nothing.
}

static LogHandler* log_handler_ = &DefaultLogHandler;

LogMessage::LogMessage(LogLevel level, const char* filename, int line,
                       LogColor color)
    : level_(level), color_(color), filename_(filename), line_(line) {}

LogMessage::~LogMessage() {}

void LogMessage::Finish() {
  log_handler_(level_, filename_, line_, message_, color_);
  if (level_ == LOGLEVEL_FATAL) {
    abort();
  }
}

void LogFinisher::operator=(LogMessage& other) { other.Finish(); }

}  // namespace internal

LogHandler* SetLogHandler(LogHandler* new_func) {
  LogHandler* old = internal::log_handler_;
  if (old == &internal::NullLogHandler) {
    old = NULL;
  }
  if (new_func == NULL) {
    internal::log_handler_ = &internal::NullLogHandler;
  } else {
    internal::log_handler_ = new_func;
  }
  return old;
}
}  // namespace nc
