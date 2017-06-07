#ifndef NCODE_HTSIM_HTSIM_H
#define NCODE_HTSIM_HTSIM_H

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <string>
#include <inttypes.h>

#include "../event_queue.h"

namespace nc {
namespace htsim {

// A generic component in the simulation. All objects that need access to the
// event queue will inherit from this class. Those that need to process events
// will also inherit from common::EventConsumer.
class SimComponent {
 public:
  // The id of this component.
  const std::string& id() { return id_; }

 protected:
  SimComponent(const std::string& id, EventQueue* event_queue)
      : id_(id), event_queue_(event_queue) {}

  // Uniquely identifies each component in the simulation.
  const std::string id_;

  // Each component has a non-owning pointer to the event queue.
  EventQueue* const event_queue_;
};

}  // namespace htsim
}  // namespace ncode

#endif
