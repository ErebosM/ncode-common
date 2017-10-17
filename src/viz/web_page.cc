#include "web_page.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>

#include "ctemplate/template.h"
#include "ctemplate/template_dictionary.h"
#include "ctemplate/template_enums.h"
#include "../logging.h"
#include "../strutil.h"
#include "../substitute.h"

namespace nc {
namespace viz {

// Resources for the templates.
extern "C" const unsigned char www_index_html[];
extern "C" const unsigned www_index_html_size;
extern "C" const unsigned char www_table_blurb_html[];
extern "C" const unsigned www_table_blurb_html_size;

// A bunch of tags.
static constexpr char kHTMLOpenTag[] = "<html lang=\"en\">";
static constexpr char kHeadOpenTag[] = "<head>";
static constexpr char kHeadCloseTag[] = "</head>";
static constexpr char kBodyOpenTag[] = "<body>";
static constexpr char kBodyCloseTag[] = "</body>";
static constexpr char kHTMLCloseTag[] = "</html>";
static constexpr char kTitleOpenTag[] = "<title>";
static constexpr char kTitleCloseTag[] = "</title>";
static constexpr char kDefaultTemplateKey[] = "default_template";
static constexpr char kTableBlurbKey[] = "table_blurb";
static constexpr char kTableIdMarker[] = "table_id";
static constexpr char kElementsSectionMarker[] = "elements";
static constexpr char kElementIdMarker[] = "element_id";
static constexpr char kColumnIndexMarker[] = "column_index";

// DataTables stuff
static constexpr char kDataTablesCSS[] =
    "https://cdn.datatables.net/1.10.12/css/jquery.dataTables.css";
static constexpr char kDataTablesButtonsCSS[] =
    "https://cdn.datatables.net/buttons/1.2.2/css/buttons.dataTables.min.css";
static constexpr char kDataTablesJS[] =
    "https://cdn.datatables.net/1.10.12/js/jquery.dataTables.js";
static constexpr char kDataTablesButtonsJS[] =
    "https://cdn.datatables.net/buttons/1.2.2/js/dataTables.buttons.min.js";
static constexpr char kJQueryJS[] =
    "https://cdnjs.cloudflare.com/ajax/libs/jquery/1.12.3/jquery.min.js";
static constexpr char kJQueryUIJS[] =
    "https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.12.0/jquery-ui.min.js";
static constexpr char kJQueryUICSS[] =
    "https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.12.0/jquery-ui.min.css";
static constexpr char kJQueryValidators[] =
    "https://cdnjs.cloudflare.com/ajax/libs/jquery-form-validator/2.3.26/"
    "jquery.form-validator.min.js";

// D3
static constexpr char kD3JS[] =
    "https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min.js";

JavascriptFuncion JavascriptFuncion::Chain(const JavascriptFuncion& other) {
  return JavascriptFuncion(StrCat(function_, other.function_));
}

void HtmlPage::AddOrUpdateHeadElement(const std::string& element_id,
                                      const std::string& element) {
  elements_in_head_[element_id] = element;
}

void HtmlPage::AddScript(const std::string& location) {
  if (std::find(scripts_.begin(), scripts_.end(), location) == scripts_.end()) {
    scripts_.push_back(location);
  }
}

void HtmlPage::AddStyle(const std::string& location) {
  if (std::find(stylesheets_.begin(), stylesheets_.end(), location) ==
      stylesheets_.end()) {
    stylesheets_.push_back(location);
  }
}

void HtmlPage::AddD3() { AddScript(kD3JS); }

std::string HtmlPage::Construct() const {
  std::string return_string = Substitute("$0$1", kHTMLOpenTag, kHeadOpenTag);
  StrAppend(&return_string, kTitleOpenTag, title_, kTitleCloseTag);
  StrAppend(&return_string, ConstructHead());
  StrAppend(&return_string, kHeadCloseTag, kBodyOpenTag, body_);
  StrAppend(&return_string, kBodyCloseTag, kHTMLCloseTag, "\n");
  return return_string;
}

std::string HtmlPage::ConstructHead() const {
  std::string return_string;
  for (const auto& id_and_element : elements_in_head_) {
    StrAppend(&return_string, id_and_element.second);
  }
  StrAppend(&return_string, head_);

  for (const std::string& css_location : stylesheets_) {
    StrAppend(
        &return_string,
        Substitute("<link rel=\"stylesheet\" type=\"text/css\" href=\"$0\">",
                   css_location));
  }

  for (const std::string& script_location : scripts_) {
    StrAppend(&return_string,
              Substitute("<script type=\"text/javascript\" "
                         "charset=\"utf8\" src=\"$0\"></script>",
                         script_location));
  }

  StrAppend(
      &return_string,
      Substitute("<script type='text/javascript'>window.onload=function() "
                 "{$0};</script>",
                 onload_.raw()));

  if (std::find(scripts_.begin(), scripts_.end(), kJQueryUIJS) !=
      scripts_.end()) {
    StrAppend(&return_string,
              "<script>$(document).ready(function() "
              "{$(\"[name='collapse_div']\").accordion({collapsible: true, "
              "active: false});} "
              ");</script>");
  }

  if (std::find(scripts_.begin(), scripts_.end(), kJQueryValidators) !=
      scripts_.end()) {
    StrAppend(
        &return_string,
        "<script>$(document).ready(function() {$.validate({});});</script>");
  }

  return return_string;
}

constexpr char TemplatePage::kHeadMarker[];
constexpr char TemplatePage::kBodyMarker[];
constexpr char TemplatePage::kNavigationMarker[];
constexpr char TemplatePage::kNavigationUrlMarker[];
constexpr char TemplatePage::kNavigationNameMarker[];

std::string TemplatePage::Construct() const {
  ctemplate::TemplateDictionary dictionary("TemplatePage");
  dictionary.SetValue(kHeadMarker, ConstructHead());
  dictionary.SetValue(kBodyMarker, body_);

  for (const auto& entry : navigation_entries_) {
    ctemplate::TemplateDictionary* navigation_dict =
        dictionary.AddSectionDictionary(kNavigationMarker);
    navigation_dict->SetValue(kNavigationUrlMarker, entry.url);
    navigation_dict->SetValue(kNavigationNameMarker, entry.name);
  }

  std::string output;
  CHECK(ctemplate::ExpandTemplate(ctemplate_key_, ctemplate::STRIP_WHITESPACE,
                                  &dictionary, &output));
  return output;
}

void HtmlTable::ToHtml(HtmlPage* page) const {
  page->AddStyle(kDataTablesCSS);
  page->AddScript(kJQueryJS);
  page->AddScript(kDataTablesJS);

  std::string* b = page->body();

  StrAppend(b, Substitute("<table id=\"$0\" class=\"display\">", id_));
  StrAppend(b, "<thead><tr>");
  for (const std::string& col_header : header_) {
    StrAppend(b, Substitute("<th>$0</th>", col_header));
  }
  StrAppend(b, "</tr></thead>");

  StrAppend(b, "<tbody>");
  for (const std::vector<std::string>& row : rows_) {
    StrAppend(b, "<tr>");
    for (const std::string& col : row) {
      StrAppend(b, Substitute("<td>$0</td>", col));
    }
    StrAppend(b, "</tr>");
  }

  StrAppend(b, "</tbody>");
  StrAppend(b, "</table>");
  if (select_elements_.empty()) {
    StrAppend(b, StrCat("<script>$(document).ready( function () { $('#"));
    StrAppend(b, id_, StrCat("').DataTable();} );</script>"));
    return;
  }

  page->AddStyle(kDataTablesButtonsCSS);
  page->AddScript(kDataTablesButtonsJS);

  static bool initialized = false;
  if (!initialized) {
    initialized = true;
    std::string table_blurb_string(
        reinterpret_cast<const char*>(www_table_blurb_html),
        www_table_blurb_html_size);
    ctemplate::StringToTemplateCache(kTableBlurbKey, table_blurb_string,
                                     ctemplate::STRIP_WHITESPACE);
  }

  ctemplate::TemplateDictionary dictionary("TableBlurb");
  dictionary.SetValue(kTableIdMarker, id_);

  for (const SelectElement& select_element : select_elements_) {
    ctemplate::TemplateDictionary* sub_dict =
        dictionary.AddSectionDictionary(kElementsSectionMarker);
    sub_dict->SetValue(kElementIdMarker, select_element.element_id);
    sub_dict->SetValue(kColumnIndexMarker,
                       std::to_string(select_element.col_index));
  }

  CHECK(ctemplate::ExpandTemplate(kTableBlurbKey, ctemplate::STRIP_WHITESPACE,
                                  &dictionary, b));
}

std::unique_ptr<TemplatePage> GetDefaultTemplate() {
  static bool initialized = false;
  if (!initialized) {
    initialized = true;
    std::string page_template_string(
        reinterpret_cast<const char*>(www_index_html), www_index_html_size);
    ctemplate::StringToTemplateCache(kDefaultTemplateKey, page_template_string,
                                     ctemplate::STRIP_WHITESPACE);
  }

  auto page = make_unique<TemplatePage>(kDefaultTemplateKey);
  return page;
}

std::string GetLink(const std::string& location, const std::string& tag) {
  return Substitute("<a href=\"$0\">$1</a>", location, tag);
}

std::string GetDiv(const std::string& contents) {
  return Substitute("<div>$0</div>", contents);
}

std::string GetP(const std::string& contents) {
  return Substitute("<p>$0</p>", contents);
}

void HtmlForm::ToHtml(HtmlPage* page) const {
  page->AddScript(kJQueryJS);
  page->AddScript(kJQueryValidators);

  std::string* b = page->body();
  StrAppend(b, Substitute("<form role=\"form\" action=\"$0\" method=\"$1\">",
                          action_, get_ ? "get" : "post"));
  for (size_t i = 0; i < fields_.size(); ++i) {
    const auto& field_ptr = fields_[i];
    StrAppend(b, "<div class=\"form-group\">");
    std::string id = StrCat(id_prefix_, "_", std::to_string(i));
    field_ptr->ToHtml(id, page);
    StrAppend(b, "</div>");
  }

  StrAppend(
      b, "<button type=\"submit\" class=\"btn btn-default\">Submit</button>");
  StrAppend(b, "</form>");
}

void HtmlTable::AddSelect(const std::string& element_id, size_t col_index) {
  select_elements_.push_back({element_id, col_index});
}

static std::string GetDataValidation(const HtmlFormField& html_form_field) {
  if (html_form_field.required) {
    return "data-validation=\"required\"";
  }

  return "";
}

void HtmlFormTextInput::ToHtml(const std::string& id, HtmlPage* page) {
  std::string* b = page->body();
  StrAppend(b, Substitute("<label>$0</label>", label));
  StrAppend(
      b, Substitute("<input class=\"form-control\" name=\"$0\" id=\"$1\" $2 ",
                    var_name, id, GetDataValidation(*this)));
  if (!placeholder.empty()) {
    StrAppend(b, Substitute("placeholder=\"$0\"", placeholder));
  }
  StrAppend(b, ">");
}

void HtmlFormSelectInput::ToHtml(const std::string& id, HtmlPage* page) {
  std::string* b = page->body();
  StrAppend(b, Substitute("<label>$0</label>", label));
  StrAppend(
      b, Substitute("<select class=\"form-control\" name=\"$0\" id=\"$1\" $2>",
                    var_name, id, GetDataValidation(*this)));
  for (const auto& id_and_option_name : options) {
    const std::string& id = id_and_option_name.first;
    const std::string& name = id_and_option_name.second;
    StrAppend(b, Substitute("<option value=\"$0\">$1</option>", id, name));
  }
  StrAppend(b, "</select>");
}

void HtmlFormCheckboxInput::ToHtml(const std::string& id, HtmlPage* page) {
  std::string* b = page->body();
  StrAppend(b, "<div class=\"checkbox\"><label>");
  StrAppend(b, Substitute("<input type=\"checkbox\" "
                          "value=\"1\" name=\"$0\" id=\"$1\" $2></input>",
                          var_name, id, GetDataValidation(*this)));
  StrAppend(b, label);
  StrAppend(b, "</label></div>");
}

void HtmlFormHiddenInput::ToHtml(const std::string& id, HtmlPage* page) {
  std::string* b = page->body();
  StrAppend(b, Substitute("<input type=\"hidden\" "
                          "value=\"$0\" name=\"$1\" id=\"$2\"></input>",
                          initial_value, var_name, id));
}

void AccordionStart(const std::string& title, HtmlPage* out) {
  out->AddScript(kJQueryJS);
  out->AddScript(kJQueryUIJS);
  out->AddStyle(kJQueryUICSS);
  StrAppend(out->body(),
            Substitute("<div name=\"collapse_div\"><h3>$0</h3>", title));
}

void AccordionEnd(HtmlPage* out) { StrAppend(out->body(), "</div>"); }

void HtmlDiv::ToHTML(HtmlPage* page) const {
  StrAppend(page->body(), Substitute("<div id='$0'>$1</div>", id_, contents_));
}

}  // namespace web
}  // namespace nc
