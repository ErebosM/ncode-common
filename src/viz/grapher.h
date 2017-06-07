#ifndef NCODE_GRAPHER_H_
#define NCODE_GRAPHER_H_

#include <stddef.h>
#include <algorithm>
#include <limits>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../common.h"
#include "../logging.h"

namespace nc {
namespace viz {
class HtmlPage;
} /* namespace web */
} /* namespace nc */

namespace nc {
namespace viz {

// One dimensional data.
struct DataSeries1D {
  std::string label;
  std::vector<double> data;
};

// 2D data.
struct DataSeries2D {
  std::string label;
  std::vector<std::pair<double, double>> data;
};

// Parameters for a plot.
struct PlotParameters {
  PlotParameters() {}

  // Title of the plot.
  std::string title;
};

// Parameters for a 2d line plot.
struct PlotParameters2D : public PlotParameters {
  PlotParameters2D() : x_scale(1.0), y_scale(1.0), x_bin_size(1) {}

  // X/Y values will be multiplied by these numbers before plotting.
  double x_scale;
  double y_scale;

  // If this is > 1 values will be binned. For example if x_bin_size is 10 every
  // 10 consecutive (in x) points will be replaced by a single point whose x
  // value will be the first of the 10 x values and y value will be the mean of
  // the 10 y values.
  size_t x_bin_size;

  // Labels for the axes.
  std::string x_label;
  std::string y_label;
};

// Parameters for a CDF or a bar plot.
struct PlotParameters1D : public PlotParameters {
  PlotParameters1D() : scale(1.0) {}

  // Values will be multiplied by this number before plotting.
  double scale;

  // Label for the data. If this is a CDF plot this will be the label of the x
  // axis, if it is a bar plot this will be the label of the y axis.
  std::string data_label;
};

// Plots graphs.
class Grapher {
 public:
  virtual ~Grapher() {}

  virtual void PlotCDF(const PlotParameters1D& plot_params,
                       const std::vector<DataSeries1D>& series) = 0;

  virtual void PlotLine(const PlotParameters2D& plot_params,
                        const std::vector<DataSeries2D>& series) = 0;

  // A stacked plot. The data series will be interpolated (linearly) at the
  // given points (xs) and a stacked plot will be produced.
  virtual void PlotStackedArea(const PlotParameters2D& plot_params,
                               const std::vector<double>& xs,
                               const std::vector<DataSeries2D>& series) = 0;

  // 1D data grouped in categories. All series should be the same
  // length (L) and the number of categories should be L.
  virtual void PlotBar(const PlotParameters1D& plot_params,
                       const std::vector<std::string>& categories,
                       const std::vector<DataSeries1D>& series) = 0;
};

// Plots graphs to an HTML page. This class does not own the page.
class HtmlGrapher : public Grapher {
 public:
  static constexpr size_t kDefaultMaxValues = 100000;
  static constexpr char kDefaultGraphIdPrefix[] = "graph";

  HtmlGrapher(HtmlPage* page, const std::string& id = kDefaultGraphIdPrefix)
      : max_values_(kDefaultMaxValues),
        graph_id_prefix_(id),
        id_(0),
        page_(page) {}

  void PlotLine(const PlotParameters2D& plot_params,
                const std::vector<DataSeries2D>& series) override;

  void PlotCDF(const PlotParameters1D& plot_params,
               const std::vector<DataSeries1D>& series) override;

  void PlotBar(const PlotParameters1D& plot_params,
               const std::vector<std::string>& categories,
               const std::vector<DataSeries1D>& series) override;

  void PlotStackedArea(const PlotParameters2D& plot_params,
                       const std::vector<double>& xs,
                       const std::vector<DataSeries2D>& series) override;

  void set_max_values(size_t max_values) { max_values_ = max_values; }

 private:
  // When plotting the values will be uniformly sampled to only contain this
  // many values.
  size_t max_values_;

  // Identifies each graph on the page.
  std::string graph_id_prefix_;

  // Sequentially incremented for each graph and added to graph_id_prefix_ to
  // get a unique id for each graph.
  size_t id_;

  HtmlPage* page_;
};

// Writes python scripts that plot the given graphs.
class PythonGrapher : public Grapher {
 public:
  PythonGrapher(const std::string& output_dir);

  void PlotLine(const PlotParameters2D& plot_params,
                const std::vector<DataSeries2D>& series) override;

  void PlotCDF(const PlotParameters1D& plot_params,
               const std::vector<DataSeries1D>& series) override;

  void PlotBar(const PlotParameters1D& plot_params,
               const std::vector<std::string>& categories,
               const std::vector<DataSeries1D>& series) override;

  void PlotStackedArea(const PlotParameters2D& plot_params,
                       const std::vector<double>& xs,
                       const std::vector<DataSeries2D>& series) override;

 private:
  // Directory where the scripts will be saved.
  std::string output_dir_;
};

// A sequence of real numbers, each paired with a period.
class PeriodicSequenceIntefrace {
 public:
  PeriodicSequenceIntefrace() {}
  virtual ~PeriodicSequenceIntefrace() {}

  // Returns the number of non-zero elements in the sequence.
  virtual size_t size() const = 0;

  // Populates the period/value pair at index i.
  virtual void at(size_t i, size_t* period_index, double* value) const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(PeriodicSequenceIntefrace);
};

// A 1D array of records that can be serialized and parsed from python.
class NpyArray {
 public:
  enum FieldType { STRING, UINT8, UINT16, UINT32, UINT64, FLOAT, DOUBLE };

  // Contains either a string or a number.
  class StringOrNumeric {
   public:
    StringOrNumeric(const std::string& str)
        : str_(str), num_(0), is_num_(false) {}
    StringOrNumeric(const char* str) : str_(str), num_(0), is_num_(false) {}
    StringOrNumeric(double num) : num_(num), is_num_(true) {}
    StringOrNumeric(int num) : num_(num), is_num_(true) {}
    StringOrNumeric(long num) : num_(num), is_num_(true) {}
    StringOrNumeric(long long num) : num_(num), is_num_(true) {}
    StringOrNumeric(unsigned long num) : num_(num), is_num_(true) {}
    StringOrNumeric(unsigned long long num) : num_(num), is_num_(true) {}

    std::string ToString(FieldType field) const;

   private:
    std::string str_;
    double num_;
    bool is_num_;
  };

  // Describes the types of columns in the array. Each one also has a name.
  using Types = std::vector<std::pair<std::string, FieldType>>;

  // Combines two arrays so that the resulting array has a2's data 'glued' to
  // the right of a1's data. Both arrays should have the same number of rows.
  NpyArray& Combine(const NpyArray& other);

  NpyArray(const Types& types) : types_(types) {}

  void AddRow(const std::vector<StringOrNumeric>& row);

  // Adds a prefix to all field names.
  NpyArray& AddPrefixToFieldNames(const std::string& prefix);

  // Removes all columns except for the ones listed.
  NpyArray& Isolate(const std::set<std::string>& fields);

  // In the folder 'out_dir' will create a text file called data where the data
  // will be serialized one row at a time, whitespace-terminated. Will also
  // create a file called parse.py where the data will be read into a structured
  // numpy array. If 'fields' is specified, will only
  void ToDisk(const std::string& output_dir, bool append = false) const;

 private:
  static constexpr const char* kFieldTypeNames[] = {"S256", "u1", "u2", "u4",
                                                    "u8",   "f4", "f8"};

  // Produces a dtype string that describes the data.
  std::string DTypeString() const;

  // Serializes a single row to a space-delimited string.
  std::string RowToString(const std::vector<StringOrNumeric>& row) const;

  // A list of names an types.
  Types types_;

  // The array's data.
  std::vector<std::vector<StringOrNumeric>> data_;
};

// Ranks a sequence based on the total of its values.
struct DefaultRankChooser {
 public:
  double operator()(const PeriodicSequenceIntefrace& sequence,
                    size_t period_min, size_t period_max) const {
    double total = 0;
    double value;
    size_t period_index;
    for (size_t i = 0; i < sequence.size(); ++i) {
      sequence.at(i, &period_index, &value);
      if (period_index < period_min || period_index >= period_max) {
        continue;
      }

      total += value;
    }

    return total;
  }
};

template <typename Key, typename RankChooser = DefaultRankChooser>
class Ranker {
 public:
  Ranker(size_t n, size_t period_min = 0,
         size_t period_max = std::numeric_limits<size_t>::max())
      : n_(n), period_min_(period_min), period_max_(period_max) {
    CHECK(period_min <= period_max);
  }

  // Adds a new key/sequence pair.
  void AddData(const Key& key, const PeriodicSequenceIntefrace& sequence);

  // Returns a vector with the top N keys over a range. The vector will have one
  // element for each key, each element will be a vector with all the values for
  // that key. Only the top N keys by total value over the range will be
  // returned. The last element in the returned vector will be a pair
  // ('default_key', sum of values not in top n).
  std::vector<std::pair<Key, std::vector<double>>> GetTopN(
      const Key& default_key) const;

 private:
  struct KeyAndSequence {
    KeyAndSequence(Key key, double total,
                   const PeriodicSequenceIntefrace& periodic_sequence,
                   size_t period_min, size_t period_max)
        : key(key), total(total) {
      double value;
      size_t period_index;
      for (size_t i = 0; i < periodic_sequence.size(); ++i) {
        periodic_sequence.at(i, &period_index, &value);
        if (period_index < period_min || period_index >= period_max) {
          continue;
        }

        if (!sequence.empty()) {
          std::pair<size_t, double>& current_last_in_sequence = sequence.back();
          CHECK(period_index >= current_last_in_sequence.first);
          if (current_last_in_sequence.first == period_index) {
            current_last_in_sequence.second += value;
            continue;
          }
        }

        sequence.emplace_back(period_index, value);
      }
    }

    Key key;
    double total;
    std::vector<std::pair<size_t, double>> sequence;
  };

  struct KeyAndSequenceCompare {
    bool operator()(const KeyAndSequence& a, const KeyAndSequence& b) const {
      return a.total > b.total;
    }
  };

  // How many elements to keep track of.
  size_t n_;

  // Min-heap with the top n elements.
  VectorPriorityQueue<KeyAndSequence, KeyAndSequenceCompare> top_n_;

  // Total value per period.
  std::vector<double> per_period_totals_;

  // Chooses the rank of each sequence.
  RankChooser rank_chooser_;

  // Ranges for the periods -- only data in this range will be considered.
  size_t period_min_;
  size_t period_max_;
};

template <typename Key, typename RankChooser>
void Ranker<Key, RankChooser>::AddData(
    const Key& key, const PeriodicSequenceIntefrace& sequence) {
  double rank = rank_chooser_(sequence, period_min_, period_max_);
  double value;
  size_t period_index;
  for (size_t i = 0; i < sequence.size(); ++i) {
    sequence.at(i, &period_index, &value);
    if (period_index < period_min_ || period_index >= period_max_) {
      continue;
    }

    if (per_period_totals_.size() <= period_index) {
      per_period_totals_.resize(period_index + 1, 0);
    }
    per_period_totals_[period_index] += value;
  }

  if (top_n_.size() && top_n_.size() == n_) {
    // top_n.top is the min element of the top n.
    if (rank < top_n_.top().total) {
      return;
    }
  }

  top_n_.emplace(key, rank, sequence, period_min_, period_max_);
  if (top_n_.size() > n_) {
    top_n_.pop();
  }
}

template <typename Key, typename RankChooser>
std::vector<std::pair<Key, std::vector<double>>>
Ranker<Key, RankChooser>::GetTopN(const Key& default_key) const {
  std::vector<std::pair<Key, std::vector<double>>> return_vector;
  if (period_min_ >= per_period_totals_.size()) {
    return {};
  }

  for (const KeyAndSequence& key_and_sequence : top_n_.containter()) {
    return_vector.emplace_back();
    std::pair<Key, std::vector<double>>& return_key_and_sequence =
        return_vector.back();
    return_key_and_sequence.first = key_and_sequence.key;
    std::vector<double>& v = return_key_and_sequence.second;

    size_t prev_index = period_min_;
    for (const auto& period_index_and_value : key_and_sequence.sequence) {
      size_t period_index = period_index_and_value.first;
      double value = period_index_and_value.second;

      if (period_index < prev_index) {
        continue;
      }

      bool done = false;
      size_t delta = period_index - prev_index;
      for (size_t i = 0; i < delta; ++i) {
        v.emplace_back(0);
        if (v.size() == period_max_ - period_min_) {
          done = true;
        }
      }

      v.emplace_back(value);
      if (done || v.size() == period_max_ - period_min_) {
        break;
      }

      prev_index = period_index + 1;
    }
  }

  // Will pad all vectors in the return map to be the size of the longest one.
  size_t return_period_count = std::min(
      period_max_ - period_min_, per_period_totals_.size() - period_min_);
  for (auto& key_and_values : return_vector) {
    std::vector<double>& values = key_and_values.second;
    values.resize(return_period_count, 0);
  }

  std::vector<double> totals_in_return_vector(return_period_count, 0);
  for (const auto& key_and_values : return_vector) {
    const std::vector<double>& values = key_and_values.second;
    for (size_t i = 0; i < return_period_count; ++i) {
      totals_in_return_vector[i] += values[i];
    }
  }

  // Have to add a default key with per_period_totals_ - totals_in_return_map
  std::vector<double> rest(return_period_count, 0);
  for (size_t i = 0; i < return_period_count; ++i) {
    size_t period_index = period_min_ + i;
    rest[i] = per_period_totals_[period_index] - totals_in_return_vector[i];
    CHECK(rest[i] >= 0) << "Negative rest for " << period_index << ": "
                        << per_period_totals_[period_index] << " vs "
                        << totals_in_return_vector[i];
  }

  if (std::accumulate(rest.begin(), rest.end(), 0.0) > 0) {
    return_vector.emplace_back(default_key, std::move(rest));
  }

  return return_vector;
}

}  // namespace grapher
}  // namespace ncode

#endif
