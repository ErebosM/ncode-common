#include "graph.h"

#include "ctemplate/template.h"
#include "ctemplate/template_dictionary.h"
#include "ctemplate/template_enums.h"
#include <algorithm>
#include <map>
#include <set>
#include <tuple>

#include "json.hpp"
#include "web_page.h"
#include "../logging.h"
#include "../map_util.h"

namespace nc {
namespace viz {

extern "C" const unsigned char www_graph_html[];
extern "C" const unsigned www_graph_html_size;
extern "C" const unsigned char www_graph_style_html[];
extern "C" const unsigned www_graph_style_html_size;

static constexpr char kGraphKey[] = "graph";
static constexpr char kGraphJSONKey[] = "graph_json";
static constexpr char kPathJSONKey[] = "paths_json";
static constexpr char kDisplayModeSectionMarker[] = "display_mode_section";
static constexpr char kDisplayModeKey[] = "display_mode";

// Link data in the format that the HTML template expects. The data will be
// converted to json later.
struct LinkDataHelper {
  size_t src_index;
  size_t dst_index;
  std::vector<double> forward_load;
  std::vector<double> reverse_load;
  std::string forward_tooltip;
  std::string reverse_tooltip;
  double strength;
};

// Like LinkData, but for paths.
struct PathDataHelper {
  std::vector<net::GraphNodeIndex> node_indices;
  std::string label;
  std::string legend_label;
};

using LinkDataMap = std::map<size_t, std::map<size_t, LinkDataHelper>>;

void GraphToHTML(const std::vector<EdgeData>& edges,
                 const std::vector<PathData>& paths,
                 const std::vector<DisplayMode>& display_modes,
                 const net::GraphStorage& graph_storage, HtmlPage* out,
                 LocalizerCallback localizer) {
  CHECK(!display_modes.empty()) << "At least one display mode required";

  // Mapping from the node id to the sequential index of the node.
  std::set<const LinkDataHelper*> link_data_set;

  // All links are bidirectional.
  LinkDataMap src_to_dst_to_link_data;

  // The paths are not required to be in any particular order.
  std::vector<PathDataHelper> all_paths;

  // Need to invert the node_id_to_node_index map. This will also sort the
  // NodeData instances by index.
  std::map<net::GraphNodeIndex, std::string> node_index_to_node_id;
  for (const auto& node_id_and_node_index : graph_storage.NodeIdToIndex()) {
    const std::string& node_id = node_id_and_node_index.first;
    CHECK(!node_id.empty());
    net::GraphNodeIndex node_index = node_id_and_node_index.second;
    node_index_to_node_id.emplace(node_index, node_id);
  }

  double max_hint = 1.0;
  for (const EdgeData& edge_data : edges) {
    max_hint = std::max(static_cast<double>(edge_data.distance_hint), max_hint);
  }

  for (const EdgeData& edge_data : edges) {
    net::GraphNodeIndex src_index =
        graph_storage.GetLink(edge_data.link)->src();
    net::GraphNodeIndex dst_index =
        graph_storage.GetLink(edge_data.link)->dst();
    CHECK(ContainsKey(node_index_to_node_id, src_index)) << src_index;
    CHECK(ContainsKey(node_index_to_node_id, dst_index)) << dst_index;
    CHECK(src_index != dst_index);
    bool forward = true;
    if (src_index > dst_index) {
      forward = false;
      std::swap(src_index, dst_index);
    }

    LinkDataHelper& link_data_helper =
        src_to_dst_to_link_data[src_index][dst_index];
    link_data_helper.src_index = src_index;
    link_data_helper.dst_index = dst_index;
    link_data_helper.strength = 1 - (edge_data.distance_hint / max_hint);

    CHECK(edge_data.load.size() == display_modes.size());
    if (forward) {
      link_data_helper.forward_load = edge_data.load;
      link_data_helper.forward_tooltip = edge_data.tooltip;
    } else {
      link_data_helper.reverse_load = edge_data.load;
      link_data_helper.reverse_tooltip = edge_data.tooltip;
    }

    link_data_set.insert(&link_data_helper);
  }

  for (const PathData& path_data : paths) {
    PathDataHelper path_data_helper;
    path_data_helper.label = path_data.label;
    path_data_helper.legend_label = path_data.legend_label;

    const net::Walk* path = path_data.path;
    path_data_helper.node_indices.reserve(path->links().size() + 1);

    const net::Links& links_on_path = path->links();
    for (net::GraphLinkIndex link : links_on_path) {
      net::GraphNodeIndex src_index = graph_storage.GetLink(link)->src();
      net::GraphNodeIndex dst_index = graph_storage.GetLink(link)->dst();

      // Each source will be added to the path's nodes, as well as the
      // destination of the last link.
      path_data_helper.node_indices.emplace_back(src_index);
      if (link == links_on_path.back()) {
        path_data_helper.node_indices.emplace_back(dst_index);
      }
    }

    std::set<size_t> node_index_set(path_data_helper.node_indices.begin(),
                                    path_data_helper.node_indices.end());
    if (node_index_set.size() != path_data_helper.node_indices.size()) {
      LOG(FATAL) << "Path with duplicate nodes";
    }

    all_paths.emplace_back(path_data_helper);
  }

  using json = nlohmann::json;
  json graph_json;
  for (const auto& node_index_and_node_id : node_index_to_node_id) {
    const std::string& node_id = node_index_and_node_id.second;
    json node_object;
    node_object["name"] = node_id;

    if (localizer) {
      double x, y;
      std::tie(x, y) = localizer(node_id);
      node_object["x"] = x;
      node_object["y"] = y;
      node_object["fixed"] = true;
    }

    graph_json["nodes"].push_back(node_object);
  }

  for (const LinkDataHelper* link_data : link_data_set) {
    json link_object;
    link_object["source"] = link_data->src_index;
    link_object["target"] = link_data->dst_index;
    link_object["forward_tooltip"] = link_data->forward_tooltip;
    link_object["reverse_tooltip"] = link_data->reverse_tooltip;
    link_object["strength"] = link_data->strength;

    CHECK(!link_data->forward_load.empty()) << link_data->src_index << " "
                                            << link_data->dst_index;
    CHECK(!link_data->reverse_load.empty()) << link_data->src_index << " "
                                            << link_data->dst_index;

    for (double load : link_data->forward_load) {
      link_object["forward_load"].push_back(load);
    }
    for (double load : link_data->reverse_load) {
      link_object["reverse_load"].push_back(load);
    }
    graph_json["links"].push_back(link_object);
  }

  json paths_json = json::array();
  for (const PathDataHelper& path : all_paths) {
    json path_object;
    path_object["label"] = path.label;
    path_object["legend_label"] = path.legend_label;
    for (net::GraphNodeIndex node_index : path.node_indices) {
      path_object["nodes"].push_back(static_cast<size_t>(node_index));
    }
    paths_json.push_back(path_object);
  }

  static bool initialized = false;
  if (!initialized) {
    initialized = true;
    std::string graph_string(reinterpret_cast<const char*>(www_graph_html),
                             www_graph_html_size);
    ctemplate::StringToTemplateCache(kGraphKey, graph_string,
                                     ctemplate::DO_NOT_STRIP);
  }

  std::string graph_style_string(
      reinterpret_cast<const char*>(www_graph_style_html),
      www_graph_style_html_size);
  out->AddOrUpdateHeadElement("graph_style", graph_style_string);
  out->AddD3();

  ctemplate::TemplateDictionary dictionary("Graph");
  dictionary.SetValue(kGraphJSONKey, graph_json.dump(1));
  dictionary.SetValue(kPathJSONKey, paths_json.dump(1));
  for (const auto& display_mode : display_modes) {
    ctemplate::TemplateDictionary* sub_dict =
        dictionary.AddSectionDictionary(kDisplayModeSectionMarker);
    sub_dict->SetValue(kDisplayModeKey, display_mode.name);
  }

  CHECK(ctemplate::ExpandTemplate(kGraphKey, ctemplate::DO_NOT_STRIP,
                                  &dictionary, out->body()));
}

}  // namespace web
}  // namespace nc
