// Code taken from Google's protobuf.

#ifndef NCODE_STATUS_H__
#define NCODE_STATUS_H__

#include <iostream>
#include <string>

#include "stringpiece.h"

namespace nc {

namespace error {
enum Code {
  OK = 0,
  CANCELLED = 1,
  UNKNOWN = 2,
  INVALID_ARGUMENT = 3,
  DEADLINE_EXCEEDED = 4,
  NOT_FOUND = 5,
  ALREADY_EXISTS = 6,
  PERMISSION_DENIED = 7,
  UNAUTHENTICATED = 16,
  RESOURCE_EXHAUSTED = 8,
  FAILED_PRECONDITION = 9,
  ABORTED = 10,
  OUT_OF_RANGE = 11,
  UNIMPLEMENTED = 12,
  INTERNAL = 13,
  UNAVAILABLE = 14,
  DATA_LOSS = 15,
};
}  // namespace error

class Status {
 public:
  static const Status OK;  // Identical to 0-arg constructor
  static const Status CANCELLED;
  static const Status UNKNOWN;

  // Creates a "successful" status.
  Status();

  // Create a status in the canonical error space with the specified
  // code, and error message.  If "code == 0", error_message is
  // ignored and a Status object identical to Status::OK is
  // constructed.
  Status(error::Code error_code, StringPiece error_message);
  Status(const Status&);
  Status& operator=(const Status& x);
  ~Status() {}

  // Accessor
  bool ok() const { return error_code_ == error::OK; }

  int error_code() const { return error_code_; }

  StringPiece error_message() const { return error_message_; }

  bool operator==(const Status& x) const;
  bool operator!=(const Status& x) const { return !operator==(x); }

  // Return a combination of the error code name and message.
  std::string ToString() const;

 private:
  error::Code error_code_;
  std::string error_message_;
};

// Prints a human-readable representation of 'x' to 'os'.
std::ostream& operator<<(std::ostream& os, const Status& x);

// Run a command that returns a Status.  If the called code returns an
// error status, return that status up out of this method too.
//
// Example:
//   RETURN_IF_ERROR(DoThings(4));
#define RETURN_IF_ERROR(expr)                                                \
  do {                                                                       \
    /* Using _status below to avoid capture problems if expr is "status". */ \
    const ::nc::Status _status = (expr);                                     \
    if (!_status.ok()) return _status;                                       \
  } while (0)

}  // namespace nc
#endif
