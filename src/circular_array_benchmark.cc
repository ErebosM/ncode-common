#include <chrono>

#include "circular_array.h"
#include "common.h"
#include "logging.h"

static constexpr size_t kCount = 1 << 30;

using namespace std::chrono;

#define BENCH_ARRAY(N)                                                         \
  {                                                                            \
    auto start = high_resolution_clock::now();                                 \
    static constexpr size_t kWindowSize = 1 << N;                              \
    auto array = ncode::make_unique<ncode::CircularArray<int, kWindowSize>>(); \
    for (size_t i = 0; i < kCount; ++i) {                                      \
      array->AddValue(i);                                                       \
    }                                                                          \
                                                                               \
    auto end = high_resolution_clock::now();                                   \
    auto duration = end - start;                                               \
    size_t duration_ms = duration_cast<milliseconds>(duration).count();        \
    LOG(INFO) << "Window " << kWindowSize << " inserted " << kCount            \
              << " entries in " << duration_ms << "ms";                        \
  }

int main(int argc, char** argv) {
  ncode::Unused(argc);
  ncode::Unused(argv);

  BENCH_ARRAY(5);
  BENCH_ARRAY(10);
  BENCH_ARRAY(15);
  BENCH_ARRAY(20);
  BENCH_ARRAY(25);
  BENCH_ARRAY(30);
}
