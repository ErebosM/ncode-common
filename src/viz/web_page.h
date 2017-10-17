#ifndef NCODE_WEB_H_
#define NCODE_WEB_H_

#include <stddef.h>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "../common.h"

namespace nc {
namespace viz {

// A javascript function.
class JavascriptFuncion {
 public:
  // By default the function does nothing.
  JavascriptFuncion() {}

  explicit JavascriptFuncion(const std::string& function)
      : function_(function) {}

  const std::string raw() const { return function_; }

  // Returns a new function that executes this function and then another one.
  JavascriptFuncion Chain(const JavascriptFuncion& other);

 private:
  // The function, as it will be rendered on the web page.
  std::string function_;
};

// A generic web page.
class HtmlPage {
 public:
  HtmlPage() {}

  virtual ~HtmlPage() {}

  // Adds or updates a head section element. Elements are indexed by a string id
  // and a new element will only be added if another one with the same id does
  // not exist. The elements are added upon calling Construct.
  void AddOrUpdateHeadElement(const std::string& element_id,
                              const std::string& element);

  // Adds a script element to the head of the page.
  void AddScript(const std::string& location);

  // Adds D3 to the page.
  void AddD3();

  // Adds a CSS to the head of the page.
  void AddStyle(const std::string& location);

  // Adds a function to be executed when the page loads.
  void AddOnLoadFunction(const JavascriptFuncion& function) {
    onload_ = onload_.Chain(function);
  }

  // Constructs a string with the HTML contents of the web page.
  virtual std::string Construct() const;

  // Returns a non-owning pointer to the head section of the web page.
  std::string* head() { return &head_; }

  // Returns a non-owning pointer to the body section of the web page.
  std::string* body() { return &body_; }

  void set_title(const std::string& title) { title_ = title; }

  // The title of the page.
  const std::string& title() const { return title_; }

 protected:
  // Constructs the head part of the page, along with the opening html tag and
  // all head elements.
  std::string ConstructHead() const;

  std::string head_;
  std::string body_;

 private:
  std::string title_;

  // Map from id to element in the head section (excludes scripts and
  // stylesheets).
  std::map<std::string, std::string> elements_in_head_;

  // Scripts in the head section.
  std::vector<std::string> scripts_;

  // Stylesheets in the head section.
  std::vector<std::string> stylesheets_;

  // Function to be called on window load.
  JavascriptFuncion onload_;

  DISALLOW_COPY_AND_ASSIGN(HtmlPage);
};

// An HTML div element.
class HtmlDiv {
 public:
  HtmlDiv(const std::string& id, const std::string& contents)
      : id_(id), contents_(contents) {}

  // Gets a javascript function that updates the div with new contents.
  JavascriptFuncion GetUpdateFunction(const std::string& new_contents) const;

  // Renders on a page.
  void ToHTML(HtmlPage* page) const;

 private:
  std::string id_;
  std::string contents_;
};

// Renders an html table on an HtmlPage.
class HtmlTable {
 public:
  HtmlTable(const std::string& id, const std::vector<std::string>& header)
      : id_(id), header_(header) {}

  template <typename T>
  void AddRow(const std::vector<T>& items) {
    CHECK(items.size() == header_.size()) << "Header / row mismatch";
    std::vector<std::string> stringified_items(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
      stringified_items[i] = std::to_string(items[i]);
    }
    rows_.emplace_back(std::move(stringified_items));
  }

  void AddRow(const std::vector<std::string>& items) {
    CHECK(items.size() == header_.size()) << "Header / row mismatch";
    rows_.emplace_back(items);
  }

  void ToHtml(HtmlPage* page) const;

  // Adds the ability to select elements from the table. When elements are
  // selected a given element has its value updated to the JSONified list of the
  // values of the column at col_index.
  void AddSelect(const std::string& element_id, size_t col_index);

 private:
  struct SelectElement {
    std::string element_id;
    size_t col_index;
  };

  // Identifies the table. Should be unique.
  std::string id_;

  // Header elements
  std::vector<std::string> header_;

  // Data
  std::vector<std::vector<std::string>> rows_;

  // Elements to be updated on select.
  std::vector<SelectElement> select_elements_;
};

// Field in an HTML form.
struct HtmlFormField {
  HtmlFormField(const std::string& var_name, const std::string& label)
      : var_name(var_name), label(label), required(false) {}
  virtual ~HtmlFormField() {}

  virtual void ToHtml(const std::string& id, HtmlPage* out) = 0;

  std::string var_name;
  std::string label;
  bool required;
};

struct HtmlFormTextInput : HtmlFormField {
  HtmlFormTextInput(const std::string& var_name, const std::string& label,
                    const std::string& placeholder = "")
      : HtmlFormField(var_name, label), placeholder(placeholder) {}

  void ToHtml(const std::string& id, HtmlPage* out) override;

  std::string placeholder;
};

struct HtmlFormSelectInput : HtmlFormField {
  HtmlFormSelectInput(
      const std::string& var_name, const std::string& label,
      const std::vector<std::pair<std::string, std::string>>& options)
      : HtmlFormField(var_name, label), options(options) {}

  void ToHtml(const std::string& id, HtmlPage* out) override;

  std::vector<std::pair<std::string, std::string>> options;
};

struct HtmlFormCheckboxInput : HtmlFormField {
  HtmlFormCheckboxInput(const std::string& var_name, const std::string& label)
      : HtmlFormField(var_name, label) {}

  void ToHtml(const std::string& id, HtmlPage* out) override;
};

struct HtmlFormHiddenInput : HtmlFormField {
  HtmlFormHiddenInput(const std::string& var_name,
                      const std::string& initial_value)
      : HtmlFormField(var_name, ""), initial_value(initial_value) {}

  void ToHtml(const std::string& id, HtmlPage* out) override;

  std::string initial_value;
};

// Renders a form. All fields of the form will have an id of
// {id_prefix}_{field_number}.
class HtmlForm {
 public:
  // Constructs a new form with the given action as target, all elements will
  // have ids prefixed by id_prefix. If get is set to true the form's method
  // will be GET. If set to false will be POST.
  HtmlForm(const std::string& action, const std::string& id_prefix,
           bool get = true)
      : get_(get), action_(action), id_prefix_(id_prefix) {}

  void AddField(std::unique_ptr<HtmlFormField> field) {
    fields_.emplace_back(std::move(field));
  }

  void ToHtml(HtmlPage* page) const;

 private:
  bool get_;
  std::string action_;
  std::string id_prefix_;
  std::vector<std::unique_ptr<HtmlFormField>> fields_;
};

// Each one of these is a button in the navigation menu of a page.
struct NavigationEntry {
  std::string name;
  std::string url;
  bool active;
};

// A templatized www page with navigation.
class TemplatePage : public HtmlPage {
 public:
  // This marker in the template will be replaced with the contents of the head
  // part of page (if any).
  static constexpr char kHeadMarker[] = "head";

  // Same as above, but for the body.
  static constexpr char kBodyMarker[] = "body";

  // Where this marker is found a series of navigation elements will be
  // generated from navigation_element_template_.
  static constexpr char kNavigationMarker[] = "navigation";

  // When this marker appears in a navigation element it will be replaced with
  // the link.
  static constexpr char kNavigationUrlMarker[] = "navigation_url";

  // When this marker appears in a navigation element it will be replaced with
  // the name of the element.
  static constexpr char kNavigationNameMarker[] = "navigation_name";

  TemplatePage(const std::string& ctemplate_key)
      : ctemplate_key_(ctemplate_key) {}

  std::string Construct() const override;

  // Adds a new navigation entry.
  void AddNavigationEntry(const NavigationEntry& navigation_entry) {
    navigation_entries_.emplace_back(navigation_entry);
  }

 private:
  std::string ctemplate_key_;

  // The navigation entries.
  std::vector<NavigationEntry> navigation_entries_;
};

// Populates the default templates.
std::unique_ptr<TemplatePage> GetDefaultTemplate();

// Returns the link to a location.
std::string GetLink(const std::string& location, const std::string& tag);

// Wraps contents in <div></div>
std::string GetDiv(const std::string& contents);

// Wraps contents in <p></p>
std::string GetP(const std::string& contents);

// Starts an accordion section with the given title. Each start must be paired
// with AccordionEnd.
void AccordionStart(const std::string& title, HtmlPage* out);
void AccordionEnd(HtmlPage* out);

}  // namespace web
}  // namespace ncode

#endif
