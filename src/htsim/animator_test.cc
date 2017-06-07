#include "gtest/gtest.h"
#include "animator.h"
#include "../common.h"

namespace nc {
namespace htsim {
namespace {

using namespace std::chrono;

class DummyAnimationComponent : public AnimationComponent {
 public:
  void ApplyValue(double value) override { values_.emplace_back(value); }

  const std::vector<double>& values() const { return values_; }

 private:
  std::vector<double> values_;
};

class AnimatorTest : public ::testing::Test {
 protected:
  template <typename T>
  void ApplyAndCheck(T time, const std::vector<double>& values,
                     LinearAnimator* animator) {
    animator->ApplyAt(event_queue_.ToTime(time), &event_queue_);
    ASSERT_EQ(component_.values(), values);
  }

  SimTimeEventQueue event_queue_;
  DummyAnimationComponent component_;
};

TEST_F(AnimatorTest, NoFrames) {
  ASSERT_DEATH(LinearAnimator linear_animator({}, false, &component_), ".*");
}

TEST_F(AnimatorTest, DuplicateFrames) {
  ASSERT_DEATH(
      LinearAnimator linear_animator({{hours(10), 100.0}, {hours(10), 150.0}},
                                     false, &component_),
      ".*");
}

TEST_F(AnimatorTest, SingleFrame) {
  LinearAnimator linear_animator({{hours(10), 100.0}}, false, &component_);
  ApplyAndCheck(hours(5), {100.0}, &linear_animator);
}

TEST_F(AnimatorTest, SingleFrameAfter) {
  LinearAnimator linear_animator({{hours(10), 100.0}}, false, &component_);
  ApplyAndCheck(hours(50), {100.0}, &linear_animator);
}

TEST_F(AnimatorTest, SingleFrameInterpolateBefore) {
  LinearAnimator linear_animator({{hours(10), 100.0}}, true, &component_);
  ApplyAndCheck(hours(5), {50.0}, &linear_animator);
}

TEST_F(AnimatorTest, TwoFrames) {
  LinearAnimator linear_animator({{hours(10), 100.0}, {hours(20), 150.0}}, true,
                                 &component_);
  ApplyAndCheck(hours(10), {100.0}, &linear_animator);
  ApplyAndCheck(hours(15), {100.0, 125.0}, &linear_animator);
  ApplyAndCheck(hours(20), {100.0, 125.0, 150.0}, &linear_animator);
}

TEST_F(AnimatorTest, AnimationContainer) {
  DummyAnimationComponent component_one;
  DummyAnimationComponent component_two;

  std::vector<KeyFrame> key_frames_one = {{seconds(10), 100.0},
                                          {seconds(50), 200.0}};
  auto linear_animator_one =
      make_unique<LinearAnimator>(key_frames_one, false, &component_one);

  std::vector<KeyFrame> key_frames_two = {{seconds(10), 200.0},
                                          {seconds(50), -100.0}};
  auto linear_animator_two =
      make_unique<LinearAnimator>(key_frames_two, false, &component_two);

  AnimationContainer container("SomeId", milliseconds(1000), &event_queue_);
  container.AddAnimator(std::move(linear_animator_one));
  container.AddAnimator(std::move(linear_animator_two));

  event_queue_.RunAndStopIn(seconds(100));

  std::vector<double> values_model_one;
  std::vector<double> values_model_two;
  for (size_t i = 0; i < 9; ++i) {
    values_model_one.emplace_back(100.0);
    values_model_two.emplace_back(200.0);
  }

  for (size_t i = 0; i < 40; ++i) {
    double v = 100.0 + 100.0 * (i / 40.0);
    values_model_one.emplace_back(v);

    v = 200.0 - 300.0 * (i / 40.0);
    values_model_two.emplace_back(v);
  }

  for (size_t i = 0; i < 50; ++i) {
    values_model_one.emplace_back(200.0);
    values_model_two.emplace_back(-100.0);
  }

  ASSERT_EQ(values_model_one, component_one.values());
  ASSERT_EQ(values_model_two, component_two.values());
}

}  // namespace
}  // namespace htsim
}  // namespace ncode
