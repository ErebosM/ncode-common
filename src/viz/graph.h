#ifndef NCODE_WEB_GRAPH_H
#define NCODE_WEB_GRAPH_H

#include <stddef.h>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "../net/net_common.h"

namespace nc {
namespace viz {
class HtmlPage;
} /* namespace web */
} /* namespace nc */

namespace nc {
namespace viz {

// Data associated with an edge.
struct EdgeData {
  EdgeData(net::GraphLinkIndex link, const std::vector<double>& load,
           const std::string& tooltip = "", size_t distance_hint = 0)
      : link(link),
        tooltip(tooltip),
        distance_hint(distance_hint),
        load(load) {}

  // The link associated with this edge.
  net::GraphLinkIndex link;

  // Tooltip to be displayed when hovering.
  std::string tooltip;

  // A hint to the layout about the length of the edge. If 0 will use 2 * node
  // radius.
  size_t distance_hint;

  // A list of values in the range [0-1]. The color of the edge will be based on
  // the load. There should be as many values as there are display modes.
  std::vector<double> load;
};

// A path through the graph.
struct PathData {
  PathData(const net::Walk* path, const std::string& label = "",
           const std::string& legend_label = "")
      : path(path), legend_label(legend_label), label(label) {}

  // The path.
  const net::Walk* path;

  // A label that will be displayed next to the path's legend entry.
  std::string legend_label;

  // A label that will be displayed along the path's legend.
  std::string label;
};

// If the graph contains more than one display mode there will be a drop-down
// box that will allow switching between them. The currently active display mode
// dictates the links' colors.
struct DisplayMode {
  explicit DisplayMode(const std::string& name) : name(name) {}

  std::string name;
};

// Renders the graph to an HTML page. If a localizer callback is provided it
// will be used to get x,y coordinates for each node.
using LocalizerCallback =
    std::function<std::pair<double, double>(const std::string&)>;
void GraphToHTML(const std::vector<EdgeData>& edges,
                 const std::vector<PathData>& paths,
                 const std::vector<DisplayMode>& display_modes,
                 const net::GraphStorage& graph, HtmlPage* out,
                 LocalizerCallback localizer = LocalizerCallback());

}  // namespace viz
}  // namespace ncode

#endif
