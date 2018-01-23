#include "grapher.h"

#include <unistd.h>
#include <mutex>
#include <array>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <type_traits>
#include <ftw.h>

#include "../file.h"
#include "../map_util.h"
#include "../stats.h"
#include "../strutil.h"
#include "../substitute.h"
#include "web_page.h"
#include "ctemplate/template.h"
#include "ctemplate/template_dictionary.h"
#include "ctemplate/template_enums.h"

namespace nc {
namespace viz {

// Resources for the template.
extern "C" const unsigned char grapher_cdf_py[];
extern "C" const unsigned grapher_cdf_py_size;
extern "C" const unsigned char grapher_line_py[];
extern "C" const unsigned grapher_line_py_size;
extern "C" const unsigned char grapher_bar_py[];
extern "C" const unsigned grapher_bar_py_size;
extern "C" const unsigned char grapher_stacked_py[];
extern "C" const unsigned grapher_stacked_py_size;
extern "C" const unsigned char grapher_npy_dump_py[];
extern "C" const unsigned grapher_npy_dump_py_size;
extern "C" const unsigned char grapher_hmap_py[];
extern "C" const unsigned grapher_hmap_py_size;

static constexpr char kPythonGrapherCDFPlot[] = "cdf_plot";
static constexpr char kPythonGrapherStackedPlot[] = "stacked_plot";
static constexpr char kPythonGrapherLinePlot[] = "line_plot";
static constexpr char kPythonGrapherBarPlot[] = "bar_plot";
static constexpr char kPythonGrapherHMapPlot[] = "hmap_plot";
static constexpr char kPythonGrapherHMapPlotLogScaleMarker[] = "log_scale";
static constexpr char kPythonNpyDump[] = "npy_dump";
static constexpr char kPythonNpyDumpDTypeMarker[] = "dtype";
static constexpr char kPythonGrapherCategoriesMarker[] = "categories";
static constexpr char kPythonGrapherTitleMarker[] = "title";
static constexpr char kPythonGrapherXLabelMarker[] = "xlabel";
static constexpr char kPythonGrapherYLabelMarker[] = "ylabel";
static constexpr char kLineTypeMarker[] = "line_type";
static constexpr char kPythonGrapherFilesAndLabelsMarker[] = "files_and_labels";
static constexpr char kPythonGrapherLinesAndLabelsMarker[] = "lines_and_labels";
static constexpr char kPythonGrapherAnnotationsMarker[] = "annotations";
static constexpr char kPythonGrapherRangesMarker[] = "ranges";
static constexpr char kPythonGrapherStackedXSMarker[] =
    "stacked_plot_xs_filename";

constexpr const char* NpyArray::kFieldTypeNames[];

static constexpr size_t kMaxPythonCDFDatapoints = 10000;
static constexpr size_t kChunkSize = 1024;

// Because each execute changes the working directory, only one of them can
// execute at a time.
static std::mutex kExecuteMutex;

// Command that can be used to get SVG output out of a plotting script.
static constexpr char kSVGCommand[] = "python plot.py --dump_svg True";

// Command to zip all contents of a directory into a .tgz
static constexpr char kZipCommand[] = "tar -zcvf $0 .";

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

static std::string ExecuteInDirectory(const std::string& cmd,
                                      const std::string& working_dir) {
  std::unique_lock<std::mutex> lock(kExecuteMutex);
  std::array<char, kChunkSize> current_cwd;
  CHECK(getcwd(current_cwd.data(), kChunkSize) != NULL);
  CHECK(chdir(working_dir.c_str()) == 0);

  FILE* in;
  std::string result;
  std::array<char, kChunkSize> buffer;
  in = popen(cmd.c_str(), "r");
  CHECK(in != nullptr) << "popen() failed!";
  while (fgets(buffer.data(), kChunkSize, in) != nullptr) {
    result += buffer.data();
  }

  CHECK(chdir(current_cwd.data()) == 0);
  CHECK(pclose(in) != -1);
  return result;
}

static std::string GetTmpDirectory() {
  std::unique_lock<std::mutex> lock(kExecuteMutex);
  char dir_name[] = "/tmp/grapherXXXXXX";
  CHECK(mkdtemp(dir_name) != nullptr);
  return {dir_name};
}

static int RmFiles(const char* pathname, const struct stat* sbuf, int type,
                   struct FTW* ftwb) {
  Unused(sbuf);
  Unused(type);
  Unused(ftwb);
  CHECK(remove(pathname) == 0) << "Unable to remove " << pathname;
  return 0;
}

static void RemoveDirectory(const std::string& dir) {
  std::unique_lock<std::mutex> lock(kExecuteMutex);
  CHECK(nftw(dir.c_str(), RmFiles, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS) == 0);
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
    double scale, const std::vector<DataSeries1D>& series) {
  std::vector<DataSeries1D> return_data;
  for (const DataSeries1D& input_series : series) {
    DataSeries1D processed_series;
    processed_series.label = input_series.label;
    processed_series.data = input_series.data;

    for (size_t i = 0; i < processed_series.data.size(); ++i) {
      processed_series.data[i] *= scale;
    }
    return_data.emplace_back(std::move(processed_series));
  }

  return return_data;
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
    std::string hmap_template(reinterpret_cast<const char*>(grapher_hmap_py),
                              grapher_hmap_py_size);
    ctemplate::StringToTemplateCache(kPythonGrapherHMapPlot, hmap_template,
                                     ctemplate::DO_NOT_STRIP);
    std::string stacked_template(
        reinterpret_cast<const char*>(grapher_stacked_py),
        grapher_stacked_py_size);
    ctemplate::StringToTemplateCache(kPythonGrapherStackedPlot,
                                     stacked_template, ctemplate::DO_NOT_STRIP);
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
static std::unique_ptr<ctemplate::TemplateDictionary> PlotCommon(
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

  std::vector<std::string> lines_and_labels_l2;
  for (const std::vector<VerticalLine>& vlines_with_same_color :
       plot_params.vlines) {
    std::vector<std::string> lines_and_labels;
    for (const VerticalLine& vline : vlines_with_same_color) {
      lines_and_labels.emplace_back(
          StrCat("(", vline.x, ",", Quote(vline.annotation), ")"));
    }

    lines_and_labels_l2.emplace_back(
        StrCat("[", Join(lines_and_labels, ","), "]"));
  }
  std::string lines_and_labels_var_contents =
      StrCat("[", Join(lines_and_labels_l2, ","), "]");

  std::vector<std::string> annotations;
  for (const Annotation& annotation : plot_params.annotations) {
    annotations.emplace_back(StrCat("(", Quote(annotation.annotation), ",",
                                    annotation.x, ",", annotation.y, ")"));
  }
  std::string annotation_contents = StrCat("[", Join(annotations, ","), "]");

  std::vector<std::string> ranges_l2;
  for (const std::vector<ColoredRange>& ranges_with_same_color :
       plot_params.ranges) {
    std::vector<std::string> ranges;
    for (const ColoredRange& range : ranges_with_same_color) {
      ranges.emplace_back(StrCat("(", range.x1, ",", range.x2, ")"));
    }

    ranges_l2.emplace_back(StrCat("[", Join(ranges, ","), "]"));
  }
  std::string ranges_var_contents = StrCat("[", Join(ranges_l2, ","), "]");

  InitPythonPlotTemplates();
  auto dictionary = make_unique<ctemplate::TemplateDictionary>("Plot");
  dictionary->SetValue(kPythonGrapherFilesAndLabelsMarker,
                       files_and_labels_var_contents);
  dictionary->SetValue(kPythonGrapherLinesAndLabelsMarker,
                       lines_and_labels_var_contents);
  dictionary->SetValue(kPythonGrapherAnnotationsMarker, annotation_contents);
  dictionary->SetValue(kPythonGrapherRangesMarker, ranges_var_contents);
  dictionary->SetValue(kPythonGrapherTitleMarker, plot_params.title);
  return dictionary;
}

void LinePlot::PlotToDir(const std::string& output) const {
  auto dictionary = PlotCommon<DataSeries2D>(
      params_, Preprocess2DData(params_, data_series_), output);
  dictionary->SetValue(kPythonGrapherXLabelMarker, params_.x_label);
  dictionary->SetValue(kPythonGrapherYLabelMarker, params_.y_label);
  dictionary->SetValue(kLineTypeMarker, line_type_);

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherLinePlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output, "/plot.py"));
}

// Samples a set of values. The vector will contain at most
// kMaxPythonCDFDatapoints values.
static void CDFSample(std::vector<double>* v) {
  if (v->size() <= kMaxPythonCDFDatapoints) {
    return;
  }

  std::vector<double> new_values = Percentiles(v, kMaxPythonCDFDatapoints);
  std::swap(*v, new_values);
}

void CDFPlot::PlotToDir(const std::string& output) const {
  std::vector<DataSeries1D> data_series =
      Preprocess1DData(params_.scale, data_series_);
  for (auto& series : data_series) {
    CDFSample(&series.data);
  }

  std::unique_ptr<ctemplate::TemplateDictionary> dictionary =
      PlotCommon(params_, data_series_, output);
  dictionary->SetValue(kPythonGrapherXLabelMarker, params_.data_label);
  dictionary->SetValue(kPythonGrapherYLabelMarker, "CDF");
  dictionary->SetValue(kLineTypeMarker, line_type_);

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherCDFPlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output, "/plot.py"));
}

void StackedLinePlot::PlotToDir(const std::string& output) const {
  auto dictionary = PlotCommon<DataSeries2D>(
      params_, Preprocess2DData(params_, data_series_), output);
  dictionary->SetValue(kPythonGrapherXLabelMarker, params_.x_label);
  dictionary->SetValue(kPythonGrapherYLabelMarker, params_.y_label);

  std::string xs_str = nc::StrCat("[", Join(xs_, ","), "]");
  dictionary->SetValue(kPythonGrapherStackedXSMarker, xs_str);

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherStackedPlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output, "/plot.py"));
}

void BarPlot::PlotToDir(const std::string& output) const {
  auto dictionary = PlotCommon<DataSeries1D>(
      params_, Preprocess1DData(params_.scale, data_series_), output);
  dictionary->SetValue(kPythonGrapherCategoriesMarker, QuotedList(categories_));
  dictionary->SetValue(kPythonGrapherYLabelMarker, params_.data_label);
  dictionary->SetValue(kPythonGrapherXLabelMarker, "category");

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherBarPlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output, "/plot.py"));
}

void HeatmapPlot::PlotToDir(const std::string& output) const {
  CHECK(params_.x_scale == params_.y_scale)
      << "Heatmap data should have same x/y scale";
  CHECK(params_.x_bin_size == 1) << "Cannot bin heatmap data";
  auto dictionary = PlotCommon<DataSeries1D>(
      params_, Preprocess1DData(params_.x_scale, data_series_), output);
  dictionary->SetValue(kPythonGrapherXLabelMarker, params_.x_label);
  dictionary->SetValue(kPythonGrapherYLabelMarker, params_.y_label);
  dictionary->SetValue(kPythonGrapherHMapPlotLogScaleMarker,
                       symlog_ ? "True" : "False");

  std::string script;
  CHECK(ctemplate::ExpandTemplate(kPythonGrapherHMapPlot,
                                  ctemplate::DO_NOT_STRIP, dictionary.get(),
                                  &script));
  File::WriteStringToFileOrDie(script, StrCat(output, "/plot.py"));
}

void Plot::PlotToSVGFile(const std::string& filename) const {
  std::string svg = PlotToSVG();
  nc::File::WriteStringToFileOrDie(svg, filename);
}

std::string Plot::PlotToSVG() const {
  std::string tmp_dir = GetTmpDirectory();
  PlotToDir(tmp_dir);
  std::string svg = ExecuteInDirectory(kSVGCommand, tmp_dir);
  RemoveDirectory(tmp_dir);
  return svg;
}

void Plot::PlotToArchiveFile(const std::string& filename) const {
  CHECK(!filename.empty());
  std::string abs_filename = filename;
  if (filename.front() != '/') {
    // Path is relative to cwd, need to prepend the current working directory.
    std::array<char, kChunkSize> cwd;
    CHECK(getcwd(cwd.data(), kChunkSize) != NULL);
    abs_filename = nc::StrCat(cwd.data(), "/", filename);
  }

  std::string tmp_dir = GetTmpDirectory();
  PlotToDir(tmp_dir);
  ExecuteInDirectory(nc::Substitute(kZipCommand, abs_filename), tmp_dir);
  RemoveDirectory(tmp_dir);
  return;
}

void Plot::PlotToHtml(HtmlPage* page) const {
  std::string svg = PlotToSVG();
  page->body()->append(svg);
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

NpyArray& NpyArray::AddPrefixToFieldNames(const std::string& prefix) {
  for (auto& field_name_and_datatype : types_) {
    field_name_and_datatype.first =
        StrCat(prefix, field_name_and_datatype.first);
  }

  return *this;
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

template <typename T>
static void RemoveFields(const std::vector<char>& to_remove,
                         std::vector<T>* data) {
  std::vector<T> new_vector;
  new_vector.reserve(data->size() - to_remove.size());
  for (size_t i = 0; i < data->size(); ++i) {
    if (to_remove[i]) {
      continue;
    }

    new_vector.emplace_back((*data)[i]);
  }

  std::swap(*data, new_vector);
}

NpyArray& NpyArray::Isolate(const std::set<std::string>& fields) {
  std::vector<char> to_remove;
  for (size_t i = 0; i < types_.size(); ++i) {
    const std::string& field_name = types_[i].first;
    to_remove[i] = nc::ContainsKey(fields, field_name);
  }

  for (auto& data : data_) {
    RemoveFields(to_remove, &data);
  }

  return *this;
}

void NpyArray::ToDisk(const std::string& output_dir, bool append) const {
  File::CreateDir(output_dir, 0700);

  std::string rows;
  for (const std::vector<StringOrNumeric>& row : data_) {
    StrAppend(&rows, RowToString(row), "\n");
  }
  File::WriteStringToFileOrDie(rows, StrCat(output_dir, "/data"), append);

  std::string script_file = StrCat(output_dir, "/parse.py");
  if (append && File::Exists(script_file)) {
    return;
  }

  std::string script;
  InitPythonPlotTemplates();
  ctemplate::TemplateDictionary dictionary("Dump");
  dictionary.SetValue(kPythonNpyDumpDTypeMarker, DTypeString());
  CHECK(ctemplate::ExpandTemplate(kPythonNpyDump, ctemplate::DO_NOT_STRIP,
                                  &dictionary, &script));
  File::WriteStringToFileOrDie(script, script_file);
}

NpyArray& NpyArray::Combine(const NpyArray& other) {
  CHECK(data_.size() == other.data_.size());
  types_.insert(types_.end(), other.types_.begin(), other.types_.end());

  for (size_t i = 0; i < data_.size(); ++i) {
    data_[i].insert(data_[i].end(), other.data_[i].begin(),
                    other.data_[i].end());
  }

  return *this;
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
