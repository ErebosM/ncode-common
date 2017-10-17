#include "square_grid.h"

#include "../logging.h"
#include "../strutil.h"
#include "ctemplate/template.h"
#include "ctemplate/template_dictionary.h"
#include "ctemplate/template_enums.h"
#include "json.hpp"

namespace nc {
namespace viz {

// Resources for the templates.
extern "C" const unsigned char www_grid_blurb_html[];
extern "C" const unsigned www_grid_blurb_html_size;

static constexpr char kGridBlurbKey[] = "grid_blurb";
static constexpr char kGridIdMarker[] = "grid_id";
static constexpr char kGridRowLabesIdMarker[] = "grid_row_labels";
static constexpr char kGridColLabesIdMarker[] = "grid_col_labels";
static constexpr char kGridDataIdMarker[] = "grid_data";

using json = nlohmann::json;

void SquareGrid::ToHTML(HtmlPage* page) const {
  page->AddD3();

  static bool initialized = false;
  if (!initialized) {
    initialized = true;
    std::string grid_blurb_string(
        reinterpret_cast<const char*>(www_grid_blurb_html),
        www_grid_blurb_html_size);
    ctemplate::StringToTemplateCache(kGridBlurbKey, grid_blurb_string,
                                     ctemplate::STRIP_WHITESPACE);
  }

  json row_labels_json = json::array();
  for (const std::string& row_label : row_labels_) {
    row_labels_json.push_back(row_label);
  }

  json col_labels_json = json::array();
  for (const std::string& col_label : col_labels_) {
    col_labels_json.push_back(col_label);
  }

  json grid_json;
  for (const auto& row : cells_) {
    json row_json;
    for (const auto& cell : row) {
      json cell_json = json::object();
      cell_json["value"] = cell.value;
      cell_json["color_saturation"] = cell.color_saturation;
      if (cell.hover_function_generator) {
        cell_json["hover_callback"] = cell.hover_function_generator().raw();
      }
      if (cell.click_function_generator) {
        cell_json["click_callback"] = cell.click_function_generator().raw();
      }

      row_json.push_back(cell_json);
    }
    grid_json.push_back(row_json);
  }

  ctemplate::TemplateDictionary dictionary("GridBlurb");
  dictionary.SetValue(kGridIdMarker, id_);
  dictionary.SetValue(kGridRowLabesIdMarker, row_labels_json.dump(1));
  dictionary.SetValue(kGridColLabesIdMarker, col_labels_json.dump(1));
  dictionary.SetValue(kGridDataIdMarker, grid_json.dump(1));
  CHECK(ctemplate::ExpandTemplate(kGridBlurbKey, ctemplate::STRIP_WHITESPACE,
                                  &dictionary, page->body()));
}

JavascriptFuncion SquareGrid::InitFunction() const {
  std::string f = StrCat("grid_", id_, "_init_function();");
  return JavascriptFuncion(f);
}

}  // namespace nc
}  // namespace viz
