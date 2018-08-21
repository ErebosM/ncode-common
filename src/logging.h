// Taken and modified from the protobuf code. The original notice follows:
//
// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef NCODE_LOGGING_H_
#define NCODE_LOGGING_H_

#include <sys/types.h>
#include <iostream>
#include <sstream>

namespace nc {
enum LogLevel {
  LOGLEVEL_INFO,
  LOGLEVEL_WARNING,
  LOGLEVEL_ERROR,
  LOGLEVEL_FATAL,
#ifdef NDEBUG
  LOGLEVEL_DFATAL = LOGLEVEL_ERROR
#else
  LOGLEVEL_DFATAL = LOGLEVEL_FATAL
#endif
};

enum LogColor {
  LOGCOLOR_RED,
  LOGCOLOR_GREEN,
  LOGCOLOR_BLUE,
  LOGCOLOR_WHITE,
  LOGCOLOR_YELLOW,
  LOGCOLOR_DEFAULT
};

namespace internal {

class LogFinisher;

class LogMessage {
 public:
  LogMessage(LogLevel level, const char* filename, int line, LogColor color);
  ~LogMessage();

  // Definitely slower than the original and probably less portable, but will
  // implicitly handle all classes that define <<.
  template <typename T>
  LogMessage& operator<<(T t) {
    std::stringstream ss;
    ss << t;
    message_ += ss.str();
    return *this;
  }

 private:
  friend class LogFinisher;
  void Finish();

  LogLevel level_;
  LogColor color_;
  const char* filename_;
  int line_;
  std::string message_;
};

// Used to make the entire "LOG(BLAH) << etc." expression have a void return
// type and print a newline after each message.
class LogFinisher {
 public:
  void operator=(LogMessage& other);
};

}  // namespace internal

#define LOG(LEVEL)                                            \
  ::nc::internal::LogFinisher() = ::nc::internal::LogMessage( \
      ::nc::LOGLEVEL_##LEVEL, __FILE__, __LINE__, ::nc::LOGCOLOR_DEFAULT)
#define CLOG(LEVEL, COLOR)                                    \
  ::nc::internal::LogFinisher() = ::nc::internal::LogMessage( \
      ::nc::LOGLEVEL_##LEVEL, __FILE__, __LINE__, ::nc::LOGCOLOR_##COLOR)
#define LOG_IF(LEVEL, CONDITION) !(CONDITION) ? (void)0 : LOG(LEVEL)

#define CHECK(EXPRESSION) \
  LOG_IF(FATAL, !(EXPRESSION)) << "CHECK failed: " #EXPRESSION ": "
#define CHECK_OK(A) CHECK(::nc::internal::IsOk(A))
#define CHECK_EQ(A, B) CHECK((A) == (B))
#define CHECK_NE(A, B) CHECK((A) != (B))
#define CHECK_LT(A, B) CHECK((A) < (B))
#define CHECK_LE(A, B) CHECK((A) <= (B))
#define CHECK_GT(A, B) CHECK((A) > (B))
#define CHECK_GE(A, B) CHECK((A) >= (B))

namespace internal {
template <typename T>
T* CheckNotNull(const char* /* file */, int /* line */, const char* name,
                T* val) {
  if (val == nullptr) {
    LOG(FATAL) << name;
  }
  return val;
}

template <typename T>
bool IsOk(T status) {
  return status.ok();
}
template <>
inline bool IsOk(bool status) {
  return status;
}
}
// namespace internal

#define CHECK_NOTNULL(A)                           \
  ::nc::internal::CheckNotNull(__FILE__, __LINE__, \
                               "'" #A "' must not be NULL", (A))

#ifdef NDEBUG

#define DLOG LOG_IF(INFO, false)

#define DCHECK(EXPRESSION) \
  while (false) CHECK(EXPRESSION)
#define DCHECK_OK(E) DCHECK(::nc::internal::IsOk(E))
#define DCHECK_EQ(A, B) DCHECK((A) == (B))
#define DCHECK_NE(A, B) DCHECK((A) != (B))
#define DCHECK_LT(A, B) DCHECK((A) < (B))
#define DCHECK_LE(A, B) DCHECK((A) <= (B))
#define DCHECK_GT(A, B) DCHECK((A) > (B))
#define DCHECK_GE(A, B) DCHECK((A) >= (B))

#else  // NDEBUG

#define DLOG LOG

#define DCHECK CHECK
#define DCHECK_OK CHECK_OK
#define DCHECK_EQ CHECK_EQ
#define DCHECK_NE CHECK_NE
#define DCHECK_LT CHECK_LT
#define DCHECK_LE CHECK_LE
#define DCHECK_GT CHECK_GT
#define DCHECK_GE CHECK_GE

#endif  // !NDEBUG

typedef void LogHandler(LogLevel level, const char* filename, int line,
                        const std::string& message, LogColor color);

// The default log handler.
void DefaultLogHandler(LogLevel level, const char* filename, int line,
                       const std::string& message, LogColor color);

// The protobuf library sometimes writes warning and error messages to
// stderr.  These messages are primarily useful for developers, but may
// also help end users figure out a problem.  If you would prefer that
// these messages be sent somewhere other than stderr, call SetLogHandler()
// to set your own handler.  This returns the old handler.  Set the handler
// to NULL to ignore log messages (but see also LogSilencer, below).
//
// Obviously, SetLogHandler is not thread-safe.  You should only call it
// at initialization time, and probably not from library code.  If you
// simply want to suppress log messages temporarily (e.g. because you
// have some code that tends to trigger them frequently and you know
// the warnings are not important to you), use the LogSilencer class
// below.
LogHandler* SetLogHandler(LogHandler* new_func);

}  // namespace nc

#endif  // COMMON_LOGGING_H_
