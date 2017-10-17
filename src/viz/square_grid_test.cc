#include "square_grid.h"

#include <gtest/gtest.h>

#include "../logging.h"
#include "web_page.h"

namespace nc {
namespace viz {
namespace {

TEST(Grid, SimpleGrid) {
  SquareGrid grid("1");
  grid.AddRow({{"one"}});

  HtmlPage page;
  grid.ToHTML(&page);
  page.AddOnLoadFunction(grid.InitFunction());

  LOG(INFO) << page.Construct();
}

}  // namespace
}  // namespace viz
}  // namespace nc
