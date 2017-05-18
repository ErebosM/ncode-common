#include "server.h"

namespace nc {
namespace viz {

bool BlockingRawReadFromSocket(int sock, char* buf, uint32_t len) {
  uint32_t total = 0;

  while (total < len) {
    int bytes_read = read(sock, buf + total, len - total);
    if (bytes_read <= 0) {
      LOG(ERROR) << "Unable to read: " << strerror(errno);
      return false;
    }

    total += bytes_read;
  }

  return true;
}

bool BlockingRawWriteToSocket(int sock, const char* buf, uint32_t len) {
  uint32_t total = 0;

  while (total < len) {
    int bytes_written = write(sock, buf + total, len - total);
    if (bytes_written < 0) {
      LOG(ERROR) << "Unable to write: " << strerror(errno);
      return false;
    }

    total += bytes_written;
  }

  return true;
}

}  // namespace web
}  // namespace nc
