#include <memory>
#include <string>

#include "net_common.h"

namespace nc {
namespace net {
namespace test {

class Base {
 protected:
  Base(const GraphBuilder& builder) : graph_(builder) {}

  // Returns the path described by a string.
  Links P(const std::string& path_string) {
    std::unique_ptr<Walk> walk = graph_.WalkFromStringOrDie(path_string);
    if (!walk) {
      return {};
    }

    return walk->links();
  }

  // Returns a node.
  GraphNodeIndex N(const std::string& node) {
    return graph_.NodeFromStringOrDie(node);
  }

  // Returns a link.
  GraphLinkIndex L(const std::string& src, const std::string& dst) {
    return graph_.LinkOrDie(src, dst);
  }

  GraphStorage graph_;
};

}  // namespace test
}  // namespace net
}  // namespace nc
