#include "fwrapper.h"

#include <sys/errno.h>
#include <algorithm>
#include <cstring>
#include <iterator>
#include <string>
#include <utility>

#include "port.h"
#include "status.h"
#include "strutil.h"

namespace nc {

StatusOr<FWrapper> FWrapper::Open(const std::string& file,
                                  const std::string& fopen_mode) {
  FILE* new_stream = fopen(file.c_str(), fopen_mode.c_str());
  if (new_stream == nullptr) {
    return Status(error::INVALID_ARGUMENT, StrCat("Unable to fopen ", file));
  }

  return FWrapper(new_stream);
}

FWrapper::~FWrapper() {
  if (file_ != nullptr && fclose(file_) != 0) {
    LOG(ERROR) << "Unable to fclose: " << strerror(errno);
  }
}

Status FWrapper::WriteUint32(uint32_t value) {
  Timer t(&stats_.total_write_time);

  uint32_t to_write = BigEndian::FromHost32(value);
  if (fwrite(&to_write, 4, 1, file_) != 1) {
    return Status(error::INTERNAL,
                  StrCat("Unable to fwrite, ", strerror(errno)));
  }

  stats_.writes.Add(4);
  return Status::OK;
}

Status FWrapper::WriteUint64(uint64_t value) {
  Timer t(&stats_.total_write_time);

  uint64_t to_write = BigEndian::FromHost64(value);
  if (fwrite(&to_write, 8, 1, file_) != 1) {
    return Status(error::INTERNAL,
                  StrCat("Unable to fwrite, ", strerror(errno)));
  }

  stats_.writes.Add(8);
  return Status::OK;
}

StatusOr<uint32_t> FWrapper::ReadUint32() {
  Timer t(&stats_.total_read_time);

  uint32_t to_read;
  if (fread(&to_read, 4, 1, file_) != 1) {
    return Status(error::INTERNAL,
                  StrCat("Unable to fread, ", strerror(errno)));
  }

  stats_.reads.Add(4);
  return BigEndian::ToHost32(to_read);
}

StatusOr<uint64_t> FWrapper::ReadUint64() {
  Timer t(&stats_.total_read_time);

  uint64_t to_read;
  if (fread(&to_read, 8, 1, file_) != 1) {
    return Status(error::INTERNAL,
                  StrCat("Unable to fread, ", strerror(errno)));
  }

  stats_.reads.Add(8);
  return BigEndian::ToHost64(to_read);
}

static Status FseekHelper(FILE* stream, long int offset, int whence) {
  if (fseek(stream, offset, whence) != 0) {
    return Status(error::INTERNAL, "Unable to fseek");
  }

  return Status::OK;
}

Status FWrapper::Seek(uint64_t offset) {
  Timer t(&stats_.total_seek_time);
  return FseekHelper(file_, offset, SEEK_SET);
}

Status FWrapper::Skip(uint64_t num_bytes) {
  Timer t(&stats_.total_seek_time);
  return FseekHelper(file_, num_bytes, SEEK_CUR);
}

StatusOr<uint64_t> FWrapper::Tell() {
  Timer t(&stats_.total_seek_time);

  long int tell = ftell(file_);
  if (tell == -1) {
    return Status(error::INTERNAL, "Unable to ftell");
  }

  return tell;
}

Status FWrapper::Write(const void* bytes, uint64_t count_bytes) {
  Timer t(&stats_.total_write_time);

  if (fwrite(bytes, count_bytes, 1, file_) != 1) {
    return Status(error::INTERNAL,
                  StrCat("Unable to fwrite, ", strerror(errno)));
  }

  stats_.writes.Add(count_bytes);
  return Status::OK;
}

Status FWrapper::Read(uint64_t count_bytes, void* destination) {
  Timer t(&stats_.total_read_time);

  if (fread(destination, count_bytes, 1, file_) != 1) {
    return Status(error::INTERNAL,
                  StrCat("Unable to fread, ", strerror(errno)));
  }

  stats_.reads.Add(count_bytes);
  return Status::OK;
}

Status FWrapper::Flush() {
  if (file_ != nullptr) {
    if (fflush(file_) != 0) {
      return Status(error::INTERNAL, "Unable to fflush");
    }
  }

  return Status::OK;
}

StatusOr<uint64_t> FWrapper::FileSize() {
  uint64_t current;
  ASSIGN_OR_RETURN(current, Tell());

  RETURN_IF_ERROR(FseekHelper(file_, 0, SEEK_END));
  uint64_t file_size;
  ASSIGN_OR_RETURN(file_size, Tell());

  RETURN_IF_ERROR(FseekHelper(file_, current, SEEK_SET));
  return file_size;
}

using RegionAndIndex = std::pair<const FWrapper::FileRegion*, size_t>;

Status FWrapper::ReadBulk(
    const std::vector<FileRegion>& regions,
    std::function<void(std::vector<uint8_t>::const_iterator from,
                       std::vector<uint8_t>::const_iterator to, size_t)>
        processor) {
  std::vector<RegionAndIndex> regions_and_indices(regions.size());
  for (size_t i = 0; i < regions.size(); ++i) {
    CHECK(regions[i].numer_of_bytes > 0);
    regions_and_indices[i] = {&regions[i], i};
  }

  std::sort(regions_and_indices.begin(), regions_and_indices.end(),
            [](const RegionAndIndex& lhs, const RegionAndIndex& rhs) {
              return lhs.first->offset_from_start <
                     rhs.first->offset_from_start;
            });

  std::vector<uint8_t> tmp_storage;
  for (size_t i = 0; i < regions_and_indices.size(); ++i) {
    const FileRegion& ri = *(regions_and_indices[i].first);
    uint64_t stretch_start = ri.offset_from_start;
    uint64_t stretch_end = stretch_start;

    size_t stretch_end_i;
    for (stretch_end_i = i; stretch_end_i < regions_and_indices.size();
         ++stretch_end_i) {
      const FileRegion& r_next = *(regions_and_indices[stretch_end_i].first);

      uint64_t next_offset = r_next.offset_from_start;
      if (next_offset < stretch_end) {
        LOG(FATAL) << "Regions overlap, next offset " << next_offset
                   << " stretch end " << stretch_end;
      } else if (next_offset == stretch_end) {
        stretch_end += r_next.numer_of_bytes;
      } else {
        break;
      }
    }

    uint64_t to_read = stretch_end - stretch_start;
    CHECK(to_read > 0);

    tmp_storage.resize(to_read);
    RETURN_IF_ERROR(Seek(stretch_start));
    Read(to_read, &(tmp_storage[0]));

    uint64_t total_offset = 0;
    for (size_t to_process_i = i; to_process_i < stretch_end_i;
         ++to_process_i) {
      const RegionAndIndex& ri = regions_and_indices[to_process_i];
      uint64_t to_consume = ri.first->numer_of_bytes;

      auto from = tmp_storage.begin() + total_offset;
      auto to = from + to_consume;
      processor(from, to, ri.second);
      total_offset += to_consume;
    }

    i = stretch_end_i - 1;
  }

  return Status::OK;
}

}  // namespace nc
