#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iostream>
#include <vector>

#include "common.h"
#include "logging.h"
#include "stats.h"
#include "statusor.h"

namespace nc {

// Statistics about a FWrapper instance.
struct FWRapperStats {
  FWRapperStats()
      : total_read_time(0), total_write_time(0), total_seek_time(0) {}

  // Total time spent reading, writing and seeking
  std::chrono::nanoseconds total_read_time;
  std::chrono::nanoseconds total_write_time;
  std::chrono::nanoseconds total_seek_time;

  // Statistics about the reads and the writes performed.
  DiscreteDistribution<uint64_t> reads;
  DiscreteDistribution<uint64_t> writes;
};

class FWrapper {
 public:
  struct FileRegion {
    uint64_t offset_from_start;
    uint64_t numer_of_bytes;
  };

  using Processor =
      std::function<void(std::vector<uint8_t>::const_iterator from,
                         std::vector<uint8_t>::const_iterator to, size_t)>;

  static StatusOr<FWrapper> Open(const std::string& file,
                                 const std::string& fopen_mode);

  FWrapper() : file_(nullptr) {}

  FWrapper(FWrapper&& other) : file_(other.file_) { other.file_ = nullptr; }

  ~FWrapper();

  // Writes a uint32_t to a file.
  Status WriteUint32(uint32_t value);

  // Writes a uint64_t to a file.
  Status WriteUint64(uint64_t value);

  // Reads a uint32_t from a file.
  StatusOr<uint32_t> ReadUint32();

  // Reads a uint64_t from a file.
  StatusOr<uint64_t> ReadUint64();

  // Sets the current offset from the beginning of the file.
  Status Seek(uint64_t offset);

  // Returns the current offset from the beginning of the file.
  StatusOr<uint64_t> Tell();

  // Skips a number of bytes.
  Status Skip(uint64_t num_bytes);

  // Writes bytes at the current offset.
  Status Write(const void* bytes, uint64_t count);

  // Reads bytes from the current offset.
  Status Read(uint64_t count_bytes, void* destination);

  // Performs all outstanding writes.
  Status Flush();

  // Returns the size of the file.
  StatusOr<uint64_t> FileSize();

  // Reads in a number of regions from the file and calls a callback on each.
  // The regions are not guaranteed to be read in the order given, and the
  // callack's from/to iterator range is temporary storage. This function will
  // not return until all reads are complete. The regions must not overlap.
  Status ReadBulk(const std::vector<FileRegion>& regions, Processor processor);

  const FWRapperStats& stats() const { return stats_; }

 private:
  FWrapper(FILE* file) : file_(file) { CHECK(file != nullptr); }

  FILE* file_;
  FWRapperStats stats_;

  DISALLOW_COPY_AND_ASSIGN(FWrapper);
};

}  // namespace nc
