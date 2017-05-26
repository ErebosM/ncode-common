#include "grapher.h"

#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <type_traits>

#include "../file.h"
#include "../stats.h"
#include "../strutil.h"
#include "../substitute.h"
#include "ctemplate/template.h"
#include "ctemplate/template_dictionary.h"
#include "ctemplate/template_enums.h"
#include "web_page.h"

namespace nc {
namespace viz {

// Resources for the template.
extern "C" const unsigned char grapher_cdf_py[];
extern "C" const unsigned grapher_cdf_py_size;
extern "C" const unsigned char grapher_line_py[];
extern "C" const unsigned grapher_line_py_size;
extern "C" const unsigned char grapher_bar_py[];
extern "C" const unsigned grapher_bar_py_size;
extern "C" const unsigned char grapher_npy_dump_py[];
extern "C" const unsigned grapher_npy_dump_py_size;

static constexpr char kPlotlyJS[] = "https://cdn.plot.ly/plotly-latest.min.js";

static constexpr char kPythonGrapherCDFPlot[] = "cdf_plot";
static constexpr char kPythonGrapherLinePlot[] = "line_plot";
static constexpr char kPythonGrapherBarPlot[] = "bar_plot";
static constexpr char kPythonNpyDump[] = "npy_dump";
static constexpr char kPythonNpyDumpDTypeMarker[] = "dtype";
static constexpr char kPythonGrapherCategoriesMarker[] = "categories";
static constexpr char kPythonGrapherTitleMarker[] = "title";
static constexpr char kPythonGrapherXLabelMarker[] = "xlabel";
static constexpr char kPythonGrapherYLabelMarker[] = "ylabel";
static constexpr char kPythonGrapherFilesAndLabelsMarker[] = "files_and_labels";

constexpr char HtmlGrapher::kDefaultGraphIdPrefix[];
constexpr const char* NpyArray::kFieldTypeNames[];

// Samples approximately N values at random, preserving order.
template <typename T>
static std::vector<T> SampleRandom(const std::vector<T>& values, size_t n) {
  CHECK(n <= values.size());
  double prob = static_cast<double>(n) / values.size();

  std::mt19937 gen(1.0);
  std::uniform_real_distribution<> dis(0, 1);

  std::vector<T> sampled;
  for (T value : values) {
    double r = dis(gen);
    if (r <= prob) {
      sampled.emplace_back(value);
    }
  }

  LOG(INFO) << "Sampled " << sampled.size() << " / " << values.size();
  return sampled;
}

static std::string Plotly2DLayoutString(const PlotParameters2D& plot_params) {
  std::string layout_string = "var layout = {";
  if (!plot_params.title.empty()) {
    StrAppend(&layout_string, Substitute("title: '$0',", plot_params.title));
  }

  StrAppend(&layout_string, "xaxis: {");
  if (!plot_params.x_label.empty()) {
    StrAppend(&layout_string, Substitute("title: '$0'", plot_params.x_label));
  }

  StrAppend(&layout_string, "}, yaxis: {");
  if (!plot_params.y_label.empty()) {
    StrAppend(&layout_string, Substitute("title: '$0'", plot_params.y_label));
  }

  StrAppend(&layout_string,
            "}, showlegend: true, legend: {\"orientation\": \"h\"}, yaxis: "
            "{rangemode: \"tozero\", autorange: true}};");
  return layout_string;
}

static std::string Plotly1DLayoutString(const PlotParameters1D& plot_params) {
  std::string layout_string = "var layout = {";
  if (!plot_params.title.empty()) {
    StrAppend(&layout_string, Substitute("title: '$0',", plot_params.title));
  }

  StrAppend(&layout_string, "}};");
  return layout_string;
}

static std::vector<DataSeries2D> Preprocess2DData(
    const PlotParameters2D& plot_parameters,
    const std::vector<DataSeries2D>& series) {
  std::vector<DataSeries2D> return_data;
  for (const DataSeries2D& input_series : series) {
    DataSeries2D processed_series;
    processed_series.label = input_series.label;
    processed_series.data = input_series.data;
    Bin(plot_parameters.x_bin_size, &processed_series.data);

    for (size_t i = 0; i < processed_series.data.size(); ++i) {
      processed_series.data[i].first *= plot_parameters.x_scale;
      processed_series.data[i].second *= plot_parameters.y_scale;
    }

    return_data.emplace_back(std::move(processed_series));
  }

  return return_data;
}

static std::vector<DataSeries1D> Preprocess1DData(
    const PlotParameters1D& plot_parameters,
    const std::vector<DataSeries1D>& series) {
  std::vector<DataSeries1D> return_data;
  for (const DataSeries1D& input_series : series) {
    DataSeries1D processed_series;
    processed_series.label = input_series.label;
    processed_series.data = input_series.data;

    for (size_t i = 0; i < processed_series.data.size(); ++i) {
      processed_series.data[i] *= plot_parameters.scale;
    }
    return_data.emplace_back(std::move(processed_series));
  }

  return return_data;
}

static std::string ToStringMaxDecimals(double value, int decimals) {
  std::ostringstream ss;
  ss << std::fixed << std::setprecision(decimals) << value;
  std::string s = ss.str();
  if (decimals > 0 && s[s.find_last_not_of('0')] == '.') {
    s.erase(s.size() - decimals + 1);
  }
  return s;
}

void HtmlGrapher::PlotLine(const PlotParameters2D& plot_params,
                           const std::vector<DataSeries2D>& series) {
  page_->AddScript(kPlotlyJS);
  std::string* b = page_->body();

  std::string div_id = Substitute("$0_$1", graph_id_prefix_, id_);
  std::string div = Substitute("<div id=\"$0\"></div>", div_id);
  StrAppend(b, div);

  std::string script = "<script>";
  std::vector<std::string> var_names;

  std::vector<DataSeries2D> processed_series =
      Preprocess2DData(plot_params, series);

  for (size_t i = 0; i < processed_series.size(); ++i) {
    std::vector<std::pair<double, double>> data = processed_series[i].data;

    // If there are too many values will sample randomly.
    if (data.size() > max_values_) {
      data = SampleRandom(data, max_values_);
    }

    std::vector<std::string> x_formatted(data.size());
    std::vector<std::string> y_formatted(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
      x_formatted[i] = ToStringMaxDecimals(data[i].first, 3);
      y_formatted[i] = ToStringMaxDecimals(data[i].second, 3);
    }

    std::string var_name = Substitute("data_$0", i);
    var_names.push_back(var_name);
    std::string series_string = StrCat("var ", var_name, " = {x: [");
    StrAppend(&series_string, Join(x_formatted, ","), "], y: [",
              Join(y_formatted, ","), "], mode: 'lines', ");
    StrAppend(&series_string, Substitute("name : '$0'", series[i].label), "};");
    StrAppend(&script, series_string);
  }

  StrAppend(&script, Plotly2DLayoutString(plot_params));
  StrAppend(&script, "var data = [", Join(var_names, ","), "];",
            Substitute("Plotly.newPlot('$0', data, layout);", div_id));
  StrAppend(&script, "</script>");
  StrAppend(b, script);

  ++id_;
}

void HtmlGrapher::PlotStackedArea(const PlotParameters2D& plot_params,
                                  const std::vector<double>& xs,
                                  const std::vector<DataSeries2D>& series) {
  page_->AddScript(kPlotlyJS);
  std::string* b = page_->body();

  std::string div_id = Substitute("$0_$1", graph_id_prefix_, id_);
  std::string div = Substitute("<div id=\"$0\"></div>", div_id);
  StrAppend(b, div);

  std::string script = "<script>";
  std::vector<std::string> var_names;

  std::vector<DataSeries2D> processed_series =
      Preprocess2DData(plot_params, series);

  size_t num_points = xs.size();
  std::vector<double> scaled_xs = xs;
  std::vector<std::string> x_formatted(num_points);
  for (size_t i = 0; i < num_points; ++i) {
    scaled_xs[i] *= plot_params.x_scale;
    x_formatted[i] = ToStringMaxDecimals(scaled_xs[i], 3);
  }

  std::vector<double> ys_cumulative(num_points, 0.0);
  for (size_t i = 0; i < processed_series.size(); ++i) {
    std::vector<std::pair<double, double>>& data = processed_series[i].data;
    Empirical2DFunction f(data, Empirical2DFunction::LINEAR);

    std::vector<std::string> y_formatted(num_points);
    for (size_t point_index = 0; point_index < num_points; ++point_index) {
      double x = scaled_xs[point_index];
      ys_cumulative[point_index] += f.Eval(x);
      y_formatted[point_index] =
          ToStringMaxDecimals(ys_cumulative[point_index], 3);
    }

    std::string var_name = Substitute("data_$0", i);
    var_names.push_back(var_name);

    std::string fill_type = i == 0 ? "tozeroy" : "tonexty";
    std::string series_string =
        Substitute("var $0 = {x: [$1], y: [$2], fill:'$3', name:'$4'};",
                   var_name, Join(x_formatted, ","), Join(y_formatted, ","),
                   fill_type, series[i].label);
    StrAppend(&script, series_string);
  }

  StrAppend(&script, Plotly2DLayoutString(plot_params));
  StrAppend(&script, "var data = [", Join(var_names, ","), "];",
            Substitute("Plotly.newPlot('$0', data, layout);", div_id));
  StrAppend(&script, "</script>");
  StrAppend(b, script);

  ++id_;
}

void HtmlGrapher::PlotCDF(const PlotParameters1D& plot_params,
                          const std::vector<DataSeries1D>& series) {
  std::vector<DataSeries2D> series_2d;

  std::vector<DataSeries1D> processed_series =
      Preprocess1DData(plot_params, series);
  for (const DataSeries1D& data_1d : processed_series) {
    std::vector<double> x = data_1d.data;

    // If there are too many values will take the k percentiles.
    if (x.size() > max_values_) {
      x = Percentiles(&x, max_values_ - 1);
    }
    std::sort(x.begin(), x.end());

    std::vector<std::pair<double, double>> xy(x.size());
    for (size_t i = 0; i < x.size(); ++i) {
      xy[i] = std::make_pair(x[i], static_cast<double>(i) / x.size());
    }

    DataSeries2D xy_series;
    xy_series.data = std::move(xy);
    xy_series.label = data_1d.label;
    series_2d.emplace_back(xy_series);
  }

  PlotParameters2D plot_params_2d;
  plot_params_2d.x_label = plot_params.data_label;
  plot_params_2d.y_label = "frequency";
  plot_params_2d.title = plot_params.title;
  PlotLine(plot_params_2d, series_2d);
}

static std::string Quote(const std::string& string) {
  CHECK(string.find("'") == std::string::npos);
  return StrCat("'", string, "'");
}

static std::string QuotedList(const std::vector<std::string>& strings) {
  std::vector<std::string> strings_quoted;
  for (const std::string& string : strings) {
    strings_quoted.emplace_back(Quote(string));
  }

  return StrCat("[", Join(strings_quoted, ","), "]");
}

void HtmlGrapher::PlotBar(const PlotParameters1D& plot_params,
                          const std::vector<std::string>& categories,
                          const std::vector<DataSeries1D>& series) {
  page_->AddScript(kPlotlyJS);
  std::string* b = page_->body();

  std::string div_id = Substitute("$0_$1", graph_id_prefix_, id_);
  std::string div = Substitute("<div id=\"$0\"></div>", div_id);
  StrAppend(b, div);

  std::string script = "<script>";
  std::vector<std::string> var_names;

  // Have to '' all the categories, since they are strings.
  std::string categores_quoted = QuotedList(categories);

  std::vector<DataSeries1D> processed_series =
      Preprocess1DData(plot_params, series);
  for (size_t i = 0; i < processed_series.size(); ++i) {
    const DataSeries1D& series_1d = processed_series[i];
    CHECK(series_1d.data.size() == categories.size());

    std::string var_name = Substitute("data_$0", i);
    var_names.push_back(var_name);
    std::string series_string =
        StrCat("var ", var_name, " = {x: ", categores_quoted);
    StrAppend(&series_string, ", y: [", Join(series_1d.data, ","),
              "], type: 'bar', ");
    StrAppend(&series_string, Substitute("name : '$0'", series[i].label), "};");
    StrAppend(&script, series_string);
  }

  StrAppend(&script, Plotly1DLayoutString(plot_params));
  StrAppend(&script, "var data = [", Join(var_names, ","), "];",
            Substitute("Plotly.newPlot('$0', data, layout);", div_id));
  StrAppend(&script, "</script>");
  StrAppend(b, script);

  ++id_;
}

static void InitPythonPlotTemplates() {
  static bool initialized = false;
  if (!initialized) {
    initialized = true;
    std::string line_template(reinterpret_cast<const char*>(grapher_line_py),
                              grapher_line_py_size);
    ctemplate::StringToTemplateCache(kPythonGrapherLinePlot, line_template,
                                     ctemplate::DO_NOT_STRIP);
    std::string cdf_template(reinterpret_cast<const char*>(grapher_cdf_py),
                             grapher_cdf_py_size);
    ctemplate::StringToTemplateCache(kPythonGrapherCDFPlot, cdf_template,
                                     ctemplate::DO_NOT_STRIP);
    std::string bar_template(reinterpret_cast<const char*>(grapher_bar_py),
                             grapher_bar_py_size);
    ctemplate::StringToTemplateCache(kPythonGrapherBarPlot, bar_template,
                                     ctemplate::DO_NOT_STRIP);
    std::string npy_parse_template(
        reinterpret_cast<const char*>(grapher_npy_dump_py),
        grapher_npy_dump_py_size);
    ctemplate::StringToTemplateCache(kPythonNpyDump, npy_parse_template,
                                     ctemplate::DO_NOT_STRIP);
  }
}

template <typename T>
void SaveSeriesToFile(const T& data_series, const std::string& file) {
  Unused(data_series);
  Unused(file);
  LOG(FATAL) << "Don't know how to do that";
}

template <>
void SaveSeriesToFile<DataSeries1D>(const DataSeries1D& data_series,
                                    const std::string& file) {
  std::string out = Join(data_series.data, "\n");
  File::WriteStringToFileOrDie(out, file);
}

template <>
void SaveSeriesToFile<DataSeries2D>(const DataSeries2D& data_series,
                                    const std::string& file) {
  std::function<std::string(const std::pair<double, double>&)> f = [](
      const std::pair<double, double>& x_and_y) {
    return StrCat(x_and_y.first, " ", x_and_y.second);
  };
  std::string out = Join(data_series.data, "\n", f);
  File::WriteStringToFileOrDie(out, file);
}

template <typename T>
static std::unique_ptr<ctemplate::TemplateDictionary> Plot(
    const PlotParameters& plot_params, const std::vector<T>& series,
    const std::string& output_dir) {
  std::vector<std::string> filenames_and_labels;
  for (size_t i = 0; i < series.size(); ++i) {
    const T& data_series = series[i];
    std::string filename = StrCat("series_", std::to_string(i));
    SaveSeriesToFile(data_series, StrCat(output_dir, "/", filename));

    filenames_and_labels.emplace_back(
        StrCat("(", Quote(filename), ",", Quote(data_series.label), ")"));
  }

  std::string files_and_labels_var_contents =
      StrCat("[", Join(filenames_and_labels, ","), "]");

  InitPythonPlotTemplates();
  auto dictionary = make_unique<ctemplate::TemplateDictionary>("Plot");
  dictionary->SetValue(kPythonGrapherFilesAndLabelsMarker,
                       files_and_labels_var_contents);
  dictionary->SetValue(kPythonGrapherTitleMarker, plot_params.title);
  return dictionary;
}

void PythonGrapher::PlotLine(const PlotParameters2D& plot_params,
                             const std::vector<DataSeries2D>& series) {
  auto dictionary = Plot<DataSeries2D>(
      plot_params, Preprocess2DData(plot_params, series), output_dir_);
  dictionary->SetValue(kPythonGrapherXLabelMarker, plot_params.x_label);
  dictionary->SetValue(kPythonGrapherYLabelMarker, plot_params.y_label);

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherLinePlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output_dir_, "/plot.py"));
}

void PythonGrapher::PlotCDF(const PlotParameters1D& plot_params,
                            const std::vector<DataSeries1D>& series) {
  auto dictionary = Plot<DataSeries1D>(
      plot_params, Preprocess1DData(plot_params, series), output_dir_);
  dictionary->SetValue(kPythonGrapherXLabelMarker, plot_params.data_label);
  dictionary->SetValue(kPythonGrapherYLabelMarker, "frequency");

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherCDFPlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output_dir_, "/plot.py"));
}

void PythonGrapher::PlotBar(const PlotParameters1D& plot_params,
                            const std::vector<std::string>& categories,
                            const std::vector<DataSeries1D>& series) {
  auto dictionary = Plot<DataSeries1D>(
      plot_params, Preprocess1DData(plot_params, series), output_dir_);
  dictionary->SetValue(kPythonGrapherCategoriesMarker, QuotedList(categories));
  dictionary->SetValue(kPythonGrapherYLabelMarker, plot_params.data_label);
  dictionary->SetValue(kPythonGrapherXLabelMarker, "category");

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherBarPlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output_dir_, "/plot.py"));
}

void PythonGrapher::PlotStackedArea(const PlotParameters2D& plot_params,
                                    const std::vector<double>& xs,
                                    const std::vector<DataSeries2D>& series) {
  LOG(ERROR) << "Not implemented yet";
  Unused(plot_params);
  Unused(xs);
  Unused(series);
}

PythonGrapher::PythonGrapher(const std::string& output_dir)
    : output_dir_(output_dir) {
  File::CreateDir(output_dir, 0700);
}

void NpyArray::AddRow(const std::vector<StringOrNumeric>& row) {
  CHECK(row.size() == types_.size());
  data_.emplace_back(row);
}

std::string NpyArray::StringOrNumeric::ToString(FieldType field) const {
  if (is_num_) {
    bool unsiged = (field == UINT8 || field == UINT16 || field == UINT32 ||
                    field == UINT64);
    if (unsiged) {
      return std::to_string(static_cast<uint64_t>(num_));
    }

    return std::to_string(num_);
  }

  CHECK(field == STRING);
  return str_;
}

NpyArray NpyArray::AddPrefixToFieldNames(const std::string& prefix) const {
  Types types = types_;
  for (auto& field_name_and_datatype : types) {
    field_name_and_datatype.first =
        StrCat(prefix, field_name_and_datatype.first);
  }

  NpyArray out(types);
  out.data_ = data_;
  return out;
}

std::string NpyArray::RowToString(
    const std::vector<StringOrNumeric>& row) const {
  std::vector<std::string> pieces;
  for (size_t i = 0; i < row.size(); ++i) {
    const StringOrNumeric& v = row[i];
    pieces.emplace_back(v.ToString(types_[i].second));
  }

  return Join(pieces, " ");
}

void NpyArray::ToDisk(const std::string& output_dir) const {
  File::CreateDir(output_dir, 0700);

  std::string rows;
  for (const std::vector<StringOrNumeric>& row : data_) {
    StrAppend(&rows, RowToString(row), "\n");
  }
  File::WriteStringToFileOrDie(rows, StrCat(output_dir, "/data"));

  std::string script;
  InitPythonPlotTemplates();
  ctemplate::TemplateDictionary dictionary("Dump");
  dictionary.SetValue(kPythonNpyDumpDTypeMarker, DTypeString());
  CHECK(ctemplate::ExpandTemplate(kPythonNpyDump, ctemplate::DO_NOT_STRIP,
                                  &dictionary, &script));
  File::WriteStringToFileOrDie(script, StrCat(output_dir, "/parse.py"));
}

NpyArray NpyArray::Combine(const NpyArray& a1, const NpyArray& a2) {
  CHECK(a1.data_.size() == a2.data_.size());
  std::vector<std::pair<std::string, FieldType>> new_types;
  new_types.insert(new_types.end(), a1.types_.begin(), a1.types_.end());
  new_types.insert(new_types.end(), a2.types_.begin(), a2.types_.end());

  NpyArray out(new_types);
  for (size_t i = 0; i < a1.data_.size(); ++i) {
    std::vector<StringOrNumeric> new_row;
    new_row.insert(new_row.end(), a1.data_[i].begin(), a1.data_[i].end());
    new_row.insert(new_row.end(), a2.data_[i].begin(), a2.data_[i].end());
    out.AddRow(new_row);
  }

  return out;
}

std::string NpyArray::DTypeString() const {
  std::vector<std::string> dtypes_as_str;
  for (const auto& name_and_type : types_) {
    dtypes_as_str.emplace_back(StrCat("('", name_and_type.first, "','",
                                      kFieldTypeNames[name_and_type.second],
                                      "')"));
  }

  return StrCat("[", Join(dtypes_as_str, ","), "]");
}

}  // namespace grapher
}  // namespace nc
