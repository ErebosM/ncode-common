// Series of classes that can animate double values based on keyframes.

#ifndef NCODE_HTSIM_ANIMATOR_H
#define NCODE_HTSIM_ANIMATOR_H

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

#include "../common.h"
#include "../event_queue.h"

namespace nc {
namespace htsim {

// Knows how to modify itself based on a given value.
class AnimationComponent {
 public:
  virtual ~AnimationComponent() {}
  virtual void ApplyValue(double value) = 0;
};

// A single keyframe.
struct KeyFrame {
  std::chrono::nanoseconds at;
  double value;

  friend std::ostream& operator<<(std::ostream& output, const KeyFrame& op);
};

class Animator {
 public:
  Animator(const std::vector<KeyFrame>& key_frames,
           AnimationComponent* animation_component);

  virtual ~Animator() {}

  virtual void ApplyAt(EventQueueTime at, EventQueue* event_queue) = 0;

 protected:
  AnimationComponent* animation_component_;

  // The key frames, sorted by time in ms.
  std::map<size_t, KeyFrame> key_frames_;
};

// Animates a single value linearly.
class LinearAnimator : public Animator {
 public:
  LinearAnimator(const std::vector<KeyFrame>& key_frames, bool start_at_zero,
                 AnimationComponent* animation_component);

  // Applies the value at a given point in time. The value will be linearly
  // interpolated between adjacent keyframes.
  void ApplyAt(EventQueueTime at, EventQueue* event_queue) override;

 private:
  bool start_at_zero_;

  DISALLOW_COPY_AND_ASSIGN(LinearAnimator);
};

class AnimationContainer : public EventConsumer {
 public:
  AnimationContainer(const std::string& id, std::chrono::milliseconds timestep,
                     EventQueue* event_queue);

  void AddAnimator(std::unique_ptr<Animator> animator);

  void HandleEvent() override;

 private:
  EventQueueTime timestep_;

  std::vector<std::unique_ptr<Animator>> animators_;
};

}  // namespace nc
}  // namespace htsim

#endif
