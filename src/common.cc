#include "common.h"

#include <glob.h>
#include <string>
#include <iomanip>
#include <iostream>
#include <random>

namespace nc {

std::vector<std::string> Glob(const std::string& pat) {
  glob_t glob_result;
  glob(pat.c_str(), GLOB_TILDE, NULL, &glob_result);
  std::vector<std::string> ret;
  for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
    ret.push_back(std::string(glob_result.gl_pathv[i]));
  }
  globfree(&glob_result);
  return ret;
}

uint32_t ghtonl(uint32_t x) {
  union {
    uint32_t result;
    uint8_t result_array[4];
  };
  result_array[0] = static_cast<uint8_t>(x >> 24);
  result_array[1] = static_cast<uint8_t>((x >> 16) & 0xFF);
  result_array[2] = static_cast<uint8_t>((x >> 8) & 0xFF);
  result_array[3] = static_cast<uint8_t>(x & 0xFF);
  return result;
}

void ThresholdEnforcerPolicy::set_empty_threshold_absolute(
    double empty_threshold_absolute) {
  CHECK(empty_threshold_absolute >= 0)
      << "Absolute threshold should be a positive number";
  empty_threshold_absolute_ = empty_threshold_absolute;
}

void ThresholdEnforcerPolicy::set_threshold_absolute(
    double threshold_absolute) {
  CHECK(threshold_absolute >= 0)
      << "Absolute threshold should be a positive number";
  threshold_absolute_ = threshold_absolute;
}

void ThresholdEnforcerPolicy::set_threshold_relative_to_current(
    double threshold_relative_to_current) {
  CHECK(threshold_relative_to_current >= 0 &&
        threshold_relative_to_current <= 1)
      << "Relative threshold should be in [0-1]";
  threshold_relative_to_current_ = threshold_relative_to_current;
}

void ThresholdEnforcerPolicy::set_threshold_relative_to_total(
    double threshold_relative_to_total) {
  CHECK(threshold_relative_to_total >= 0 && threshold_relative_to_total <= 1)
      << "Relative threshold should be in [0-1]";
  threshold_relative_to_total_ = threshold_relative_to_total;
}

double ThresholdEnforcerPolicy::empty_threshold_absolute() const {
  return empty_threshold_absolute_;
}

double ThresholdEnforcerPolicy::threshold_absolute() const {
  return threshold_absolute_;
}

double ThresholdEnforcerPolicy::threshold_relative_to_current() const {
  return threshold_relative_to_current_;
}

double ThresholdEnforcerPolicy::threshold_relative_to_total() const {
  return threshold_relative_to_total_;
}

void TimeoutPolicy::set_timeout_penalty(uint64_t timeout_penalty) {
  timeout_penalty_ = timeout_penalty;
}

uint64_t TimeoutPolicy::timeout_penalty_lookback() const {
  return timeout_penalty_lookback_;
}

void TimeoutPolicy::set_timeout_penalty_lookback(
    uint64_t timeout_penalty_lookback) {
  timeout_penalty_lookback_ = timeout_penalty_lookback;
}

bool TimeoutPolicy::timeout_penalty_cumulative() const {
  return timeout_penalty_cumulative_;
}

void TimeoutPolicy::set_timeout_penalty_cumulative(
    bool timeout_penalty_cumulative) {
  timeout_penalty_cumulative_ = timeout_penalty_cumulative;
}

CountdownTimer::CountdownTimer(std::chrono::nanoseconds budget)
    : construction_time_(std::chrono::steady_clock::now()), budget_(budget) {}

bool CountdownTimer::Expired() const {
  using namespace std::chrono;
  auto now = steady_clock::now();
  nanoseconds delta = duration_cast<nanoseconds>(now - construction_time_);
  return delta >= budget_;
}

std::chrono::nanoseconds CountdownTimer::RemainingTime() const {
  using namespace std::chrono;
  auto now = steady_clock::now();
  nanoseconds delta = duration_cast<nanoseconds>(now - construction_time_);
  if (delta >= budget_) {
    return nanoseconds::zero();
  }
  return budget_ - delta;
}

std::string RandomString(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

std::string ToStringMaxDecimals(double value, int decimals) {
  std::ostringstream ss;
  ss << std::fixed << std::setprecision(decimals) << value;
  std::string s = ss.str();
  if (decimals > 0 && s[s.find_last_not_of('0')] == '.') {
    s.erase(s.size() - decimals + 1);
  }
  return s;
}

static uint64_t GetRandomIdHelper(std::function<bool(uint64_t)> id_taken,
                                  std::mt19937* rnd) {
  for (size_t i = 5; i <= 32; ++i) {
    uint64_t max = (1 << i) - 1;
    std::uniform_int_distribution<uint64_t> distribution(0, max);

    uint64_t next_id = distribution(*rnd);
    if (!id_taken(next_id)) {
      return next_id;
    }
  }

  std::uniform_int_distribution<uint64_t> distribution;
  while (true) {
    uint64_t next_id = distribution(*rnd);
    if (id_taken(next_id)) {
      continue;
    }

    return next_id;
  }
}

uint64_t GetRandomId(std::function<bool(uint64_t)> id_taken,
                     std::mt19937* rnd) {
  if (rnd != nullptr) {
    return GetRandomIdHelper(id_taken, rnd);
  }
  std::mt19937 new_rnd(1);
  return GetRandomIdHelper(id_taken, &new_rnd);
}

}  // namespace nc
