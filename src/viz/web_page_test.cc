#include "web_page.h"

#include "ctemplate/template.h"
#include "ctemplate/template_enums.h"

#include "../strutil.h"
#include "../substitute.h"
#include "gtest/gtest.h"

namespace nc {
namespace viz {
namespace {

static constexpr char kTitle[] = "Some Title";

class PageFixture : public ::testing::Test {
 protected:
  void SetUp() override { page_.set_title(kTitle); }

  HtmlPage page_;
};

TEST_F(PageFixture, Init) {
  ASSERT_EQ(Substitute("<html lang=\"en\"><head><title>$0</title>"
                       "</head><body></body></html>\n",
                       kTitle),
            page_.Construct());
}

TEST_F(PageFixture, Head) {
  StrAppend(page_.head(), "stuff");
  ASSERT_EQ(Substitute("<html lang=\"en\"><head><title>$0</title>stuff"
                       "</head><body></body></html>\n",
                       kTitle),
            page_.Construct());
}

TEST_F(PageFixture, Body) {
  StrAppend(page_.body(), "stuff");
  ASSERT_EQ(Substitute("<html lang=\"en\"><head><title>$0</title>"
                       "</head><body>stuff</body></html>\n",
                       kTitle),
            page_.Construct());
}

TEST_F(PageFixture, HeadElement) {
  std::string script =
      "<script type=\"text/javascript\" "
      "src=\"https://awesomescript\"></script>";
  std::string expected = Substitute(
      "<html lang=\"en\"><head><title>$0</title>$1"
      "</head><body></body></html>\n",
      kTitle, script);
  std::string expected_twice = Substitute(
      "<html lang=\"en\"><head><title>$0</title>$1$2"
      "</head><body></body></html>\n",
      kTitle, script, script);

  page_.AddOrUpdateHeadElement("something", script);
  ASSERT_EQ(expected, page_.Construct());
  page_.AddOrUpdateHeadElement("something", script);
  ASSERT_EQ(expected, page_.Construct());
  page_.AddOrUpdateHeadElement("something_else", script);
  ASSERT_EQ(expected_twice, page_.Construct());
}

TEST_F(PageFixture, Table) {
  HtmlTable table("some_id", {"colA", "colB", "colC"});
  table.AddRow<int>({1, 2, 3});
  table.ToHtml(&page_);

  ASSERT_NE(std::string::npos,
            page_.Construct().find("<table id=\"some_id\" "
                                   "class=\"display\"><thead><tr><th>colA</"
                                   "th><th>colB</th><th>colC</th></tr></"
                                   "thead><tbody><tr><td>1</td><td>2</"
                                   "td><td>3</td></tr></tbody></"
                                   "table>"));
}

TEST(TemplatePage, BodyTemplate) {
  std::string page_template =
      StrCat("<html><head></head><body>{{", TemplatePage::kBodyMarker,
             "}}</body></html>");
  ctemplate::StringToTemplateCache("random_key1", page_template,
                                   ctemplate::STRIP_WHITESPACE);

  TemplatePage page("random_key1");
  ASSERT_EQ("<html><head></head><body></body></html>", page.Construct());
  StrAppend(page.body(), "test");
  ASSERT_EQ("<html><head></head><body>test</body></html>", page.Construct());
}

TEST(TemplatePage, NavigationTemplate) {
  std::string page_template =
      StrCat("<html><head></head><body>{{#", TemplatePage::kNavigationMarker,
             "}}", StrCat("<ul>{{", TemplatePage::kNavigationUrlMarker, "}},{{",
                          TemplatePage::kNavigationNameMarker, "}}</ul>"),
             "{{/", TemplatePage::kNavigationMarker, "}}</body></html>");
  ctemplate::StringToTemplateCache("random_key2", page_template,
                                   ctemplate::STRIP_WHITESPACE);

  TemplatePage page("random_key2");
  ASSERT_EQ("<html><head></head><body></body></html>", page.Construct());

  NavigationEntry entry;
  entry.name = "Test";
  entry.url = "TestLink";
  NavigationEntry another_entry;
  another_entry.name = "Test2";
  another_entry.url = "TestLink2";
  TemplatePage page_with_navigation("random_key2");
  page_with_navigation.AddNavigationEntry(entry);
  page_with_navigation.AddNavigationEntry(another_entry);

  ASSERT_EQ(
      "<html><head></head><body><ul>TestLink,Test</ul>"
      "<ul>TestLink2,Test2</ul></body></html>",
      page_with_navigation.Construct());
}

}  // namespace test
}  // namespace web
}  // namespace ncode
