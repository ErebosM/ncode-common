#include "net_gen.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "net_common.h"
#include "../strutil.h"

namespace nc {
namespace net {

using namespace std::chrono;

static void AddLink(Bandwidth bw, const std::string& src, Delay delay,
                    const std::string& dst, Delay delay_add,
                    double delay_multiply, Bandwidth bw_add, double bw_multiply,
                    GraphBuilder* out) {
  microseconds total_delay =
      duration_cast<microseconds>(delay * delay_multiply) + delay_add;
  total_delay = std::max(microseconds(1), total_delay);
  uint64_t total_bw = bw.bps() * bw_multiply + bw_add.bps();
  out->AddLink({src, dst, Bandwidth::FromBitsPerSecond(total_bw), total_delay});
}

static void AddBiLink(Bandwidth bw, const std::string& src, double delay_sec,
                      const std::string& dst, Delay delay_add,
                      double delay_multiply, GraphBuilder* out) {
  auto delay = duration_cast<Delay>(duration<double>(delay_sec));
  AddLink(bw, src, delay, dst, delay_add, delay_multiply,
          Bandwidth::FromBitsPerSecond(0), 1.0, out);
  AddLink(bw, dst, delay, src, delay_add, delay_multiply,
          Bandwidth::FromBitsPerSecond(0), 1.0, out);
}

static void AddBiLink(uint64_t bw_bps, const std::string& src, double delay_sec,
                      const std::string& dst, Delay delay_add,
                      double delay_multiply, Bandwidth bw_add,
                      double bw_multiply, GraphBuilder* out) {
  auto delay = duration_cast<microseconds>(duration<double>(delay_sec));
  AddLink(Bandwidth::FromBitsPerSecond(bw_bps), src, delay, dst, delay_add,
          delay_multiply, bw_add, bw_multiply, out);
  AddLink(Bandwidth::FromBitsPerSecond(bw_bps), dst, delay, src, delay_add,
          delay_multiply, bw_add, bw_multiply, out);
}

static void AddBiLink(Bandwidth bw, const std::string& src, Delay delay,
                      const std::string& dst, Delay delay_add,
                      double delay_multiply, GraphBuilder* out) {
  AddLink(bw, src, delay, dst, delay_add, delay_multiply,
          Bandwidth::FromBitsPerSecond(0), 1.0, out);
  AddLink(bw, dst, delay, src, delay_add, delay_multiply,
          Bandwidth::FromBitsPerSecond(0), 1.0, out);
}

GraphBuilder GenerateHE(Bandwidth bw, Delay delay_add, double delay_multiply) {
  GraphBuilder out;

  AddBiLink(bw, "HongKong", 0.09899999999999999, "LosAngeles", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "HongKong", 0.02197946590441512, "Singapore", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "HongKong", 0.024491937340699977, "Tokyo", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Tokyo", 0.045182269059776056, "Singapore", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "SanJose", 0.07082018626712423, "Tokyo", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Fremont", 0.00020944320149470966, "SanJose", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "PaloAlto", 0.0002098538430130341, "SanJose", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "PaloAlto", 0.00015330384758825228, "Fremont", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Portland", 0.007750976557599637, "SanJose", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Seattle", 0.00970358928467878, "SanJose", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Seattle", 0.001980189853586593, "Portland", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Seattle", 0.0016290555332783203, "Vancouver", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Denver", 0.013942438885259888, "Seattle", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "LosAngeles", 0.004379930992404047, "PaloAlto", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Phoenix", 0.004878751961226585, "LosAngeles", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "LasVegas", 0.003127585787117664, "LosAngeles", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Denver", 0.008271265381124625, "LasVegas", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Denver", 0.012679134906052427, "SanJose", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Denver", 0.009549009601746527, "Minneapolis", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Denver", 0.00761386951297863, "KansasCity", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Dallas", 0.012085544227624674, "Phoenix", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Dallas", 0.019896482083487595, "Fremont", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Dallas", 0.006205167979753415, "KansasCity", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Dallas", 0.010971931036320862, "Chicago", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Dallas", 0.01588028050776086, "Ashburn", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Dallas", 0.009845557514101322, "Atlanta", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Dallas", 0.015180864669691761, "Miami", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Miami", 0.00827928825214428, "Atlanta", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Ashburn", 0.007244818676274683, "Atlanta", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "KansasCity", 0.005613545439041082, "Chicago", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "NewYork", 0.009734695650495928, "Chicago", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Toronto", 0.0059679334825478835, "Chicago", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Minneapolis", 0.004854188292269742, "Chicago", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Ashburn", 0.0029763178724357656, "NewYork", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Toronto", 0.004719392909665924, "NewYork", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "PaloAlto", 0.03499418206738756, "NewYork", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "SanJose", 0.03485362282421041, "NewYork", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Ashburn", 0.03268078197594825, "SanJose", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "London", 0.04732289625262841, "NewYork", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Ashburn", 0.050272198201872294, "London", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Amsterdam", 0.049801303976444715, "NewYork", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Ashburn", 0.052553436093461164, "Paris", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "London", 0.0029205763165338866, "Paris", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Zurich", 0.004154523412587286, "Paris", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Frankfurt am Main", 0.004065115464977358, "Paris", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Amsterdam", 0.003657458411802766, "Paris", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Amsterdam", 0.0030323776775934013, "London", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Frankfurt am Main", 0.005416362162592938, "London", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Amsterdam", 0.009561504025009172, "Stockholm", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Amsterdam", 0.0049050665652040255, "Berlin", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Amsterdam", 0.0030944299935970203, "Frankfurt am Main",
            delay_add, delay_multiply, &out);
  AddBiLink(bw, "Amsterdam", 0.005227380278661229, "Zurich", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Prague", 0.0034757563350856072, "Frankfurt am Main", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Zurich", 0.0025991945628872828, "Frankfurt am Main", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Warsaw", 0.004395690813733776, "Prague", delay_add,
            delay_multiply, &out);
  AddBiLink(bw, "Berlin", 0.004390355474006233, "Warsaw", delay_add,
            delay_multiply, &out);

  return out;
}

GraphBuilder GenerateNTT(Delay delay_add, double delay_multiply,
                         Bandwidth bw_add, double bw_multiply) {
  GraphBuilder out;

  AddBiLink(400000000000ul, "tokyo", 0.0440402438126, "LA", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(170000000000ul, "tokyo", 0.0384675854525, "seattle", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(320000000000ul, "tokyo", 0.0416617032847, "sanjose", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(660000000000ul, "osaka", 0.00200511038863, "tokyo", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "tokyo", 0.0391165806529, "sydney", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(260000000000ul, "tokyo", 0.0144204243915, "hongkong", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(80000000000ul, "tokyo", 0.0104964514544, "taipei", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(140000000000ul, "tokyo", 0.0265926970908, "singapore", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "tokyo", 0.00579455563354, "seoul", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(80000000000ul, "osaka", 0.0402090563017, "seattle", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(140000000000ul, "osaka", 0.043534645896, "sanjose", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(370000000000ul, "osaka", 0.0459271862836, "LA", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(100000000000ul, "osaka", 0.0247606172858, "singapore", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(290000000000ul, "osaka", 0.0124464408122, "hongkong", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "osaka", 0.00414594196265, "seoul", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(60000000000ul, "osaka", 0.00857148496165, "taipei", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "hongkong", 0.00407591702825, "taipei", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(110000000000ul, "hongkong", 0.0129549377611, "singapore", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(70000000000ul, "singapore", 0.0542395406027, "london", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "singapore", 0.00154630285189, "kualalumpur",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "singapore", 0.0682342201114, "sanjose", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "sydney", 0.0602720584442, "LA", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(220000000000ul, "amsterdam", 0.00182209284912, "frankfurt",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "amsterdam", 0.000909833341748, "dusseldorf",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "amsterdam", 0.000869200417323, "brussels",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(180000000000ul, "amsterdam", 0.00414870408579, "milan", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(300000000000ul, "frankfurt", 0.00318691684306, "london", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "frankfurt", 0.000913053085595, "dusseldorf",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(30000000000ul, "frankfurt", 0.00445101062351, "warsaw", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(610000000000ul, "frankfurt", 0.00726475139602, "bucharest",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(130000000000ul, "frankfurt", 0.00405812441165, "budapest",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(40000000000ul, "frankfurt", 0.00694408831625, "sofia", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "frankfurt", 0.00152045524726, "munich", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(210000000000ul, "frankfurt", 0.00259092434959, "milan", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(490000000000ul, "london", 0.00178661083351, "amsterdam", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "london", 0.0016034321954, "brussels", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(160000000000ul, "madrid", 0.00631305134948, "london", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(120000000000ul, "madrid", 0.00740711873213, "amsterdam", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(130000000000ul, "paris", 0.00171381959824, "london", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "paris", 0.00215062165847, "amsterdam", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "newyork", 0.0278523715932, "london", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "newyork", 0.0310121935076, "frankfurt", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "dc", 0.0309484781572, "amsterdam", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dc", 0.0326528976101, "frankfurt", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "seattle", 0.00571088101251, "sanjose", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "seattle", 0.013944272859, "chicago", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "seattle", 0.0193272573202, "newyork", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "sanjose", 0.0147912336407, "chicago", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "sanjose", 0.0194414156732, "dc", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "sanjose", 0.00246677083724, "LA", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "LA", 0.0100216743653, "dallas", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dallas", 0.00647050831368, "chicago", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(30000000000ul, "dallas", 0.00182148583442, "houston", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dallas", 0.0116488251407, "sanjose", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dallas", 0.00579917359141, "atlanta", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dallas", 0.00951351743117, "dc", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "chicago", 0.00572131169056, "newyork", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "miami", 0.00894378578209, "dallas", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "dc", 0.00744267357058, "miami", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(400000000000ul, "dc", 0.00164245029422, "newyork", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dc", 0.00435963693841, "atlanta", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dc", 0.0185521687536, "LA", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "atlanta", 0.00487263179876, "miami", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(30000000000ul, "newyork", 0.0384269996315, "saopaulo", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(30000000000ul, "miami", 0.032832730137, "saopaulo", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(120000000000ul, "amsterdam", 0.00560044467656, "stockholm",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(260000000000ul, "frankfurt", 0.00298833201945, "vienna", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(100000000000ul, "frankfurt", 0.00591448704821, "stockholm",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(40000000000ul, "osaka", 0.0247445962548, "kualalumpur", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(30000000000ul, "barcelona", 0.00252419850939, "madrid", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(40000000000ul, "barcelona", 0.00619614168801, "amsterdam",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "singapore", 0.00452808193141, "jakarta", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "singapore", 0.00630897776255, "brunei", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "hongkong", 0.0099359167697, "brunei", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "amsterdam", 0.00731156034184, "valencia", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(180000000000ul, "madrid", 0.00134688070725, "valencia", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(100000000000ul, "luxembourg", 0.0013869784069, "amsterdam",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "luxembourg", 0.000880679729202, "frankfurt",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(6000000000ul, "singapore", 0.0071197805129, "bangkok", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(15000000000ul, "kualalumpur", 0.0059220982258, "bangkok", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(20000000000ul, "osaka", 0.0389890992677, "sydney", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(100000000000ul, "boston", 0.0015962339139, "newyork", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(100000000000ul, "boston", 0.00680157749372, "chicago", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(200000000000ul, "dc", 0.00477882625241, "chicago", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(100000000000ul, "marseille", 0.00504988093448, "amsterdam",
            delay_add, delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(100000000000ul, "marseille", 0.003992628199, "frankfurt", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "berlin", 0.00288467585939, "amsterdam", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);
  AddBiLink(10000000000ul, "berlin", 0.00211788639232, "frankfurt", delay_add,
            delay_multiply, bw_add, bw_multiply, &out);

  return out;
}

GraphBuilder GenerateFullGraph(uint32_t size, Bandwidth bw, Delay delay) {
  std::mt19937 rnd(1.0);
  return GenerateRandom(size, 1.0, delay, delay, bw, bw, &rnd);
}

GraphBuilder GenerateRandom(size_t n, double edge_prob, Delay delay_min,
                            Delay delay_max, Bandwidth bw_min, Bandwidth bw_max,
                            std::mt19937* generator) {
  CHECK(delay_min <= delay_max);
  CHECK(bw_min.bps() <= bw_max.bps());
  if (n == 0) {
    return {};
  }

  std::vector<size_t> ids(n);
  std::iota(ids.begin(), ids.end(), 0);
  std::shuffle(ids.begin(), ids.end(), *generator);

  auto delay_dist = std::uniform_int_distribution<uint64_t>(delay_min.count(),
                                                            delay_max.count());
  auto bw_dist =
      std::uniform_int_distribution<uint64_t>(bw_min.bps(), bw_max.bps());
  auto edge_add_dist = std::uniform_real_distribution<double>(0.0, 1.0);

  // Add links between ids, then add randomly pick pairs and add links.
  std::set<std::pair<size_t, size_t>> added;
  for (size_t i = 0; i < n - 1; ++i) {
    size_t src = ids[i];
    size_t dst = ids[i + 1];
    if (src > dst) {
      std::swap(src, dst);
    }

    added.emplace(src, dst);
  }

  for (uint32_t i = 0; i < n; ++i) {
    for (uint32_t j = 0; j < n; ++j) {
      size_t src = i;
      size_t dst = j;
      if (src >= dst) {
        continue;
      }

      double p = edge_add_dist(*generator);
      if (p > edge_prob) {
        continue;
      }

      added.emplace(src, dst);
    }
  }

  GraphBuilder out;
  for (const auto& src_and_dst : added) {
    std::string src = "N" + std::to_string(src_and_dst.first);
    std::string dst = "N" + std::to_string(src_and_dst.second);

    std::chrono::microseconds delay =
        std::chrono::microseconds(delay_dist(*generator));
    uint64_t bw = bw_dist(*generator);
    out.AddLink({src, dst, Bandwidth::FromBitsPerSecond(bw), delay});
    out.AddLink({dst, src, Bandwidth::FromBitsPerSecond(bw), delay});
  }

  return out;
}

GraphBuilder GenerateLadder(size_t levels, Bandwidth bw, Delay delay,
                            double central_rate_multiplier,
                            const std::vector<microseconds>& central_delays) {
  GraphBuilder out;
  CHECK(levels > 0) << "Tried to generate ladder with no edges,";

  if (!central_delays.empty()) {
    CHECK(central_delays.size() == levels);
  }

  for (size_t level = 0; level < levels; ++level) {
    std::string node_left = StrCat("N", std::to_string(4 * level));
    std::string node_right = StrCat("N", std::to_string(4 * level + 1));

    microseconds central_link_delay = delay;
    if (!central_delays.empty()) {
      central_link_delay = central_delays[level];
    }

    Bandwidth central_bw =
        Bandwidth::FromMBitsPerSecond(bw.Mbps() * central_rate_multiplier);
    AddBiLink(central_bw, node_left, central_link_delay, node_right,
              Delay::zero(), 1.0, &out);

    if (level != 0) {
      std::string mid_node_left =
          StrCat("N", std::to_string(4 * (level - 1) + 2));

      std::string mid_node_right =
          StrCat("N", std::to_string(4 * (level - 1) + 3));

      std::string prev_node_left = StrCat("N", std::to_string(4 * (level - 1)));
      std::string prev_node_right =
          StrCat("N", std::to_string(4 * (level - 1) + 1));

      AddBiLink(bw, node_left, delay, mid_node_left, Delay::zero(), 1.0, &out);
      AddBiLink(bw, mid_node_left, delay, prev_node_left, Delay::zero(), 1.0,
                &out);

      AddBiLink(bw, node_right, delay, mid_node_right, Delay::zero(), 1.0,
                &out);
      AddBiLink(bw, mid_node_right, delay, prev_node_right, Delay::zero(), 1.0,
                &out);
    }
  }

  return out;
}

GraphBuilder GenerateBraess(Bandwidth bw) {
  GraphBuilder out;
  AddBiLink(bw, "C", milliseconds(10), "D", Delay::zero(), 1.0, &out);
  AddBiLink(bw, "B", milliseconds(8), "D", Delay::zero(), 1.0, &out);
  AddBiLink(bw, "A", milliseconds(10), "B", Delay::zero(), 1.0, &out);
  AddBiLink(bw, "A", milliseconds(5), "C", Delay::zero(), 1.0, &out);
  out.AddLink({"B", "C", bw, milliseconds(1)});

  return out;
}

// Parses a line of the form <tag> <count> and returns count.
static uint32_t ParseCountOrDie(const std::string& tag,
                                const std::string& line) {
  std::vector<std::string> line_split = Split(line, " ");
  CHECK(line_split.size() == 2) << line;
  CHECK(line_split[0] == tag) << line_split[0] << " vs " << tag;

  uint32_t count;
  CHECK(safe_strtou32(line_split[1], &count));
  return count;
}

GraphBuilder LoadRepetitaOrDie(
    const std::string& topology_string, std::vector<std::string>* node_order,
    std::map<std::string, std::pair<double, double>>* locations) {
  std::vector<std::string> all_lines = Split(topology_string, "\n");
  auto it = all_lines.begin();

  const std::string& nodes_line = *it;
  uint32_t num_nodes = ParseCountOrDie("NODES", nodes_line);

  // Skip free form line.
  ++it;

  std::vector<std::string> nodes;
  for (uint32_t i = 0; i < num_nodes; ++i) {
    ++it;

    std::vector<std::string> line_split = Split(*it, " ");
    CHECK(line_split.size() == 3);

    const std::string& node_id = StrCat(line_split[0], "_", i);
    nodes.emplace_back(node_id);

    if (locations == nullptr) {
      continue;
    }

    double x;
    double y;
    CHECK(safe_strtod(line_split[1], &x));
    CHECK(safe_strtod(line_split[2], &y));

    (*locations)[node_id] = {x, y};
  }

  if (node_order != nullptr) {
    *node_order = nodes;
  }

  // Skip line.
  ++it;

  const std::string& edges_line = *it;
  uint32_t num_edges = ParseCountOrDie("EDGES", edges_line);

  // Skip free form line.
  ++it;

  GraphBuilder builder;
  for (uint32_t i = 0; i < num_edges; ++i) {
    ++it;
    CHECK(it != all_lines.end());

    std::vector<std::string> line_split = Split(*it, " ");
    CHECK(line_split.size() == 6);

    uint32_t src_index;
    uint32_t dst_index;
    double bw_kbps;
    double delay_microseconds;

    CHECK(safe_strtou32(line_split[1], &src_index));
    CHECK(safe_strtou32(line_split[2], &dst_index));
    CHECK(safe_strtod(line_split[4], &bw_kbps));
    CHECK(safe_strtod(line_split[5], &delay_microseconds));
    CHECK(src_index < nodes.size());
    CHECK(dst_index < nodes.size());

    if (nodes[src_index] == nodes[dst_index]) {
      LOG(INFO) << "Ignoring loop at " << nodes[dst_index];
      continue;
    }

    builder.AddLink(
        {nodes[src_index], nodes[dst_index],
         Bandwidth::FromKBitsPerSecond(bw_kbps),
         std::chrono::microseconds(static_cast<uint64_t>(delay_microseconds))});
  }

  return builder;
}

}  // namespace net
}  // namespace ncode
