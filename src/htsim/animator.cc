#include "animator.h"

#include <type_traits>

#include "../map_util.h"
#include "../substitute.h"

namespace nc {
namespace htsim {

std::ostream& operator<<(std::ostream& output, const KeyFrame& op) {
  output << Substitute("KeyFrame at $0ns value $1", op.at.count(), op.value);
  return output;
}

Animator::Animator(const std::vector<KeyFrame>& key_frames,
                   AnimationComponent* animation_component)
    : animation_component_(animation_component) {
  CHECK(!key_frames.empty());
  for (const KeyFrame& key_frame : key_frames) {
    InsertOrDie(&key_frames_, key_frame.at.count(), key_frame);
  }
}

LinearAnimator::LinearAnimator(const std::vector<KeyFrame>& key_frames,
                               bool start_at_zero,
                               AnimationComponent* animation_component)
    : Animator(key_frames, animation_component),
      start_at_zero_(start_at_zero) {}

void LinearAnimator::ApplyAt(EventQueueTime at, EventQueue* event_queue) {
  double value;

  size_t at_nanos = event_queue->TimeToNanos(at).count();
  auto upper_bound = key_frames_.upper_bound(at_nanos);
  if (upper_bound == key_frames_.end()) {
    value = key_frames_.rbegin()->second.value;
  } else if (upper_bound == key_frames_.begin() && !start_at_zero_) {
    value = upper_bound->second.value;
  } else {
    double value_end_at = upper_bound->second.value;
    double frame_end_at = upper_bound->second.at.count();

    double value_start_at;
    double frame_start_at;
    if (upper_bound == key_frames_.begin()) {
      value_start_at = 0;
      frame_start_at = 0;
    } else {
      auto prev = std::prev(upper_bound);
      value_start_at = prev->second.value;
      frame_start_at = prev->second.at.count();
    }

    double fraction =
        (at_nanos - frame_start_at) / (frame_end_at - frame_start_at);
    value = value_start_at + (value_end_at - value_start_at) * fraction;
  }

  animation_component_->ApplyValue(value);
}

AnimationContainer::AnimationContainer(const std::string& id,
                                       std::chrono::milliseconds timestep,
                                       EventQueue* event_queue)
    : EventConsumer(id, event_queue) {
  timestep_ = event_queue->ToTime(timestep);
  EnqueueIn(timestep_);
}

void AnimationContainer::AddAnimator(std::unique_ptr<Animator> animator) {
  animators_.emplace_back(std::move(animator));
}

void AnimationContainer::HandleEvent() {
  for (const auto& animator : animators_) {
    animator->ApplyAt(event_queue()->CurrentTime(), event_queue());
  }
  EnqueueIn(timestep_);
}

}  // namespace nc
}  // namespace htims
