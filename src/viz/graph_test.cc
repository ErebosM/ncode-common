#include "graph.h"

#include <chrono>

#include "gtest/gtest.h"
#include "web_page.h"

#include "../file.h"
#include "../net/net_gen.h"

namespace nc {
namespace viz {
namespace {

TEST(Graph, SimpleGraph) {
  net::GraphBuilder builder =
      net::GenerateFullGraph(2, net::Bandwidth::FromBitsPerSecond(10000),
                             std::chrono::microseconds(100));
  net::GraphStorage graph_storage(builder);

  std::vector<EdgeData> edge_data;
  std::vector<PathData> path_data;
  std::vector<DisplayMode> display_modes;

  for (const auto& link_base : builder.links()) {
    net::GraphLinkIndex link =
        graph_storage.LinkOrDie(link_base.src_id(), link_base.dst_id());
    std::vector<double> values = {0.1, 0.9};
    edge_data.emplace_back(link, values, "Some tooltip", 0);
  }

  std::unique_ptr<net::Walk> path =
      graph_storage.WalkFromStringOrDie("[N0->N1]");
  path_data.emplace_back(path.get(), "Label 1", "Label 2");

  display_modes.emplace_back("Mode 1");
  display_modes.emplace_back("Mode 2");

  HtmlPage page;
  GraphToHTML(edge_data, path_data, display_modes, &graph_storage, &page);
  File::WriteStringToFile(page.Construct(), "graph.html");
}

}  // namespace
}  // namespace web
}  // namespace ncode
