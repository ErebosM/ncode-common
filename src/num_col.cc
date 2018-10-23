#include "num_col.h"

namespace nc {

RangeSet::RangeSet(const std::vector<Range>& ranges, bool already_sorted) {
  if (ranges.empty()) {
    return;
  }

  ranges_ = ranges;
  if (!already_sorted) {
    std::sort(ranges_.begin(), ranges_.end());
  }
  RemoveOverlap(&ranges_);
}

RangeSet::RangeSet(std::vector<Range>* ranges, bool already_sorted) {
  CHECK(ranges != nullptr);
  if (ranges->empty()) {
    return;
  }

  std::swap(ranges_, *ranges);
  if (!already_sorted) {
    std::sort(ranges_.begin(), ranges_.end());
  }
  RemoveOverlap(&ranges_);
}

void RangeSet::RemoveOverlap(std::vector<Range>* ranges) {
  // Will first remove overlap.
  for (size_t i = 0; i < ranges->size() - 1; ++i) {
    const Range& next_range = (*ranges)[i + 1];
    Range& range = (*ranges)[i];

    uint32_t next_start_index = next_range.first;
    uint32_t start_index = range.first;
    uint32_t& count = range.second;
    uint32_t end_index = start_index + count;

    if (next_start_index < end_index) {
      uint32_t delta = end_index - next_start_index;
      CHECK(delta <= count);
      count -= delta;
    }
  }

  // And then merge consecutive ranges.
  std::vector<Range> out;
  for (size_t i = 0; i < ranges->size(); ++i) {
    const Range& ri = (*ranges)[i];
    uint32_t stretch_start = ri.first;
    uint32_t stretch_end = stretch_start;

    uint32_t stretch_end_i;
    for (stretch_end_i = i; stretch_end_i < ranges->size(); ++stretch_end_i) {
      const Range& r_next = (*ranges)[stretch_end_i];

      uint32_t next_index = r_next.first;
      if (next_index < stretch_end) {
        LOG(FATAL) << "Ranges overlap, next index " << next_index
                   << " stretch end " << stretch_end;
      } else if (next_index == stretch_end) {
        stretch_end += r_next.second;
      } else {
        break;
      }
    }

    uint32_t stretch_len = stretch_end - stretch_start;
    if (stretch_len) {
      out.push_back({stretch_start, stretch_len});
    }

    i = stretch_end_i - 1;
  }

  std::swap(*ranges, out);
}

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

uint64_t ImmutablePackedIntVector::RawValueAtIndex(uint32_t index) const {
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

int64_t ImmutablePackedIntVector::ValueAt(uint32_t index) const {
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

}  // namespace nc
