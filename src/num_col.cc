#include "num_col.h"

namespace nc {
namespace num_col {

void ImmutablePackedIntVector::ZigZagEncode(std::vector<int64_t>* values) {
  for (int64_t& v : *values) {
    v = (v << 1) ^ (v >> 63);
  }
}

int64_t ImmutablePackedIntVector::ZigZagDecode(uint64_t v) {
  return (v >> 1) ^ (-(v & 1));
}

void ImmutablePackedIntVector::Init(std::vector<int64_t>* values) {
  if (values->empty()) {
    bytes_per_num_ = 1;
    base_ = 0;
    return;
  }

  max_ = *(std::max_element(values->begin(), values->end()));
  min_ = *(std::min_element(values->begin(), values->end()));

  if (HasNegativeValues(*values)) {
    zig_zagged_ = true;
    ZigZagEncode(values);
  }

  Rebase(values);

  // At this point the values will be all non-negative with one or more 0s.
  uint64_t max = *(std::max_element(values->begin(), values->end()));
  for (bytes_per_num_ = 1; bytes_per_num_ < 8; ++bytes_per_num_) {
    if (max < (1UL << (bytes_per_num_ * 8))) {
      break;
    }
  }

  uint64_t bytes_count = values->size() * bytes_per_num_;
  data_.resize(bytes_count);
  for (size_t i = 0; i < values->size(); ++i) {
    memcpy(&data_[bytes_per_num_ * i], &((*values)[i]), bytes_per_num_);
  }
}

uint64_t ImmutablePackedIntVector::RawValueAtIndex(size_t index) const {
  uint64_t byte_index = bytes_per_num_ * index;
  CHECK(byte_index < data_.size()) << byte_index << " vs " << data_.size();
  if (data_.size() - byte_index > 8) {
    const uint64_t* v_ptr =
        reinterpret_cast<const uint64_t*>(&(data_[byte_index]));
    switch (bytes_per_num_) {
      case 1:
        return (*v_ptr) & kOneByteMask;
      case 2:
        return (*v_ptr) & kTwoByteMask;
      case 3:
        return (*v_ptr) & kThreeByteMask;
      case 4:
        return (*v_ptr) & kFourByteMask;
      case 5:
        return (*v_ptr) & kFiveByteMask;
      case 6:
        return (*v_ptr) & kSixByteMask;
      case 7:
        return (*v_ptr) & kSevenByteMask;
      case 8:
        return *v_ptr;
    }
  }
  uint64_t out = 0;
  memcpy(&out, &data_[byte_index], bytes_per_num_);
  return out;
}

void ImmutablePackedIntVector::Rebase(std::vector<int64_t>* values) {
  base_ = *(std::min_element(values->begin(), values->end()));
  for (int64_t& v : *values) {
    v -= base_;
  }
}

int64_t ImmutablePackedIntVector::at(size_t index) const {
  uint64_t v = base_ + RawValueAtIndex(index);
  if (zig_zagged_) {
    return ZigZagDecode(v);
  }

  return v;
}

uint64_t ImmutablePackedIntVector::ByteEstimate() const {
  return sizeof(this) + data_.size();
}

ImmutablePackedIntVectorStats ImmutablePackedIntVector::Stats() const {
  ImmutablePackedIntVectorStats out;
  out.zig_zagged = zig_zagged_;
  out.bytes_per_num = bytes_per_num_;
  out.num_values = size();
  out.byte_size_estimate = sizeof(this) + data_.size();
  return out;
}

std::string StorageTypeToString(StorageType storage_type) {
  switch (storage_type) {
    case INT_PACKED:
      return "INT_PACKED";
    case RLE:
      return "RLE";
    case BIT_VECTOR:
      return "BIT_VECTOR";
    case DOUBLE_VECTOR:
      return "DOUBLE_VECTOR";
  }

  return "";
}

std::string IndexTypeToString(IndexType index_type) {
  switch (index_type) {
    case UNINDEXED:
      return "UNINDEXED";
    case BASIC:
      return "BASIC";
    case SORTED_INTERVAL:
      return "SORTED_INTERVAL";
    case BIT_RANGES:
      return "BIT_RANGES";
  }

  return "";
}

static std::string QuantityToString(uint64_t value, const std::string& single,
                                    const std::string& thousand,
                                    const std::string& million,
                                    const std::string& billion) {
  if (value < 1000) {
    return std::to_string(value) + single;
  }

  if (value < 1000 * 1000) {
    return StrCat(ToStringMaxDecimals(value / 1000.0, 2), thousand);
  }

  if (value < 1000 * 1000 * 1000) {
    return StrCat(ToStringMaxDecimals(value / 1000.0 / 1000.0, 2), million);
  }

  return StrCat(ToStringMaxDecimals(value / 1000.0 / 1000.0 / 1000.0, 2),
                billion);
}

std::string BytesToString(uint64_t bytes) {
  return QuantityToString(bytes, "B", "kB", "MB", "GB");
}

std::string NumericalQuantityToString(uint64_t value) {
  return QuantityToString(value, "", "k", "M", "B");
}

}  // namespace num_col
}  // namespace nc
