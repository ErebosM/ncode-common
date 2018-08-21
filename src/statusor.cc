#include "statusor.h"

#include "logging.h"

namespace nc {

namespace internal_statusor {

void Helper::HandleInvalidStatusCtorArg(Status* status) {
  const char* kMessage =
      "An OK status is not a valid constructor argument to StatusOr<T>";
  LOG(ERROR) << kMessage;
  // Fall back to error::INTERNAL.
  *status = Status(error::INTERNAL, kMessage);
}

void Helper::Crash(const Status& status) {
  LOG(FATAL) << "Attempting to fetch value instead of handling error "
             << status.ToString();
}

}  // namespace internal
}  // namespace nc
