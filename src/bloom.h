#include <stddef.h>
#include <stdint.h>
#include <tuple>
#include <utility>
#include <vector>

#include "common.h"

namespace nc {

void MurmurHash3_x64_128(const void* key, const int len, uint32_t seed,
                         uint32_t* h1_out, uint32_t* h2_out);

template <typename T>
struct ValueExtractor {
  void operator()(const T& value, const void** v, size_t* s) const {
    const void* value_ptr = static_cast<const void*>(&value);
    *v = value_ptr;
    *s = sizeof(T);
  }
};

template <>
struct ValueExtractor<std::string> {
  void operator()(const std::string& value, const void** v, size_t* s) const {
    *v = value.data();
    *s = value.size();
  }
};

template <typename T>
struct ValueExtractor<std::vector<T>> {
  void operator()(const std::vector<T>& value, const void** v,
                  size_t* s) const {
    *v = value.data();
    *s = value.size() * sizeof(T);
  }
};

template <typename T, typename V = ValueExtractor<T>>
class BloomFilter {
 public:
  BloomFilter(size_t expected_num_elements,
              double false_positive_probability = 0.0001) {
    double l2 = std::log2(false_positive_probability);
    num_hashes_ = ceil(-l2);
    CHECK(num_hashes_ > 0);

    size_t num_bits = ceil(-1.44 * l2 * expected_num_elements);
    CHECK(num_bits > 0);
    bits_.resize(num_bits);
  }

  void Add(const T& value) {
    uint32_t hash_one;
    uint32_t hash_two;
    HashValue(value, &hash_one, &hash_two);

    for (size_t i = 0; i < num_hashes_; ++i) {
      uint64_t bit_to_set = ComputeNthHash(hash_one, hash_two, i);
      bits_[bit_to_set % bits_.size()] = true;
    }
  }

  bool PossiblyContains(const T& value) const {
    uint32_t hash_one;
    uint32_t hash_two;
    HashValue(value, &hash_one, &hash_two);

    for (size_t i = 0; i < num_hashes_; ++i) {
      uint64_t bit_to_set = ComputeNthHash(hash_one, hash_two, i);
      if (!bits_[bit_to_set % bits_.size()]) {
        return false;
      }
    }

    return true;
  }

  double SaturationFactor() const {
    double total = 0;
    for (bool i : bits_) {
      if (i) {
        ++total;
      }
    }

    return total / bits_.size();
  }

 private:
  static constexpr double kLn2 = 0.6931471805599453;
  static constexpr uint32_t kHashSeed = 42;

  inline uint64_t ComputeNthHash(uint32_t hash_one, uint32_t hash_two,
                                 size_t n) const {
    return hash_one + n * hash_two;
  }

  inline void HashValue(const T& value, uint32_t* hash_one,
                        uint32_t* hash_two) const {
    const void* value_ptr;
    size_t len;
    extractor_(value, &value_ptr, &len);

    MurmurHash3_x64_128(value_ptr, len, kHashSeed, hash_one, hash_two);
  }

  V extractor_;
  size_t num_hashes_;
  std::vector<bool> bits_;
};

}  // namespace nc
