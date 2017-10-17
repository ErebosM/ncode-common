#ifndef NCODE_WEB_GRAPH_H
#define NCODE_WEB_GRAPH_H

#include <functional>
#include <string>
#include <vector>

#include "web_page.h"

namespace nc {
namespace viz {
class HtmlPage;
} /* namespace viz */
} /* namespace nc */

namespace nc {
namespace viz {

// A cell in the grid.
struct GridCell {
  GridCell(std::string value) : value(value), color_saturation(0) {}

  // A string for the cell.
  std::string value;

  // A number between 0-1; the cell will be colored based on this value.
  double color_saturation;

  // Generates a function that will be executed when the cell is hovered.
  std::function<JavascriptFuncion()> hover_function_generator;

  // Generates a function that will be executed when the cell is clicked.
  std::function<JavascriptFuncion()> click_function_generator;
};

// A grid of squares.
class SquareGrid {
 public:
  SquareGrid(const std::string& id) : id_(id) {}

  // Adds a new row to the grid.
  void AddRow(const std::vector<GridCell>& row) { cells_.emplace_back(row); }

  // Render to a page.
  void ToHTML(HtmlPage* page) const;

  JavascriptFuncion InitFunction() const;

 private:
  // Uniquely identifies this grid.
  std::string id_;

  std::vector<std::vector<GridCell>> cells_;

  // Row/column labels.
  std::vector<std::string> row_labels_;
  std::vector<std::string> col_labels_;
};

}  // namespace viz
}  // namespce nc
#endif
