#ifndef NCODE_NET_COMMON_H
#define NCODE_NET_COMMON_H

#include <stddef.h>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <chrono>

#include "../common.h"
#include "../logging.h"
#include "../perfect_hash.h"

namespace nc {
namespace net {

// An IP address is just an integer -- currently v6 addresses are not supported.
struct IPAddressTag {};
using IPAddress = TypesafeUintWrapper<IPAddressTag, uint32_t, '*'>;

// The access-layer port is a 16bit integer.
struct AccessLayerPortTag {};
using AccessLayerPort = TypesafeUintWrapper<AccessLayerPortTag, uint16_t, '*'>;

// The IP protocol is an 8bit integer.
struct IPProtoTag {};
using IPProto = TypesafeUintWrapper<IPProtoTag, uint8_t, '*'>;

// The number of a network port on a device.
struct DevicePortNumberTag {};
using DevicePortNumber =
    TypesafeUintWrapper<DevicePortNumberTag, uint32_t, '*'>;

// Bandwidth.
struct BandwidthTag {};
class Bandwidth : public TypesafeUintWrapper<BandwidthTag, uint64_t> {
 public:
  static constexpr Bandwidth FromBitsPerSecond(uint64_t bps) {
    return Bandwidth(bps);
  }
  static constexpr Bandwidth FromKBitsPerSecond(uint64_t bps) {
    return Bandwidth(bps * 1000.0);
  }
  static constexpr Bandwidth FromMBitsPerSecond(double Mbps) {
    return Bandwidth(Mbps * 1000.0 * 1000.0);
  }
  static constexpr Bandwidth FromGBitsPerSecond(double Gbps) {
    return Bandwidth(Gbps * 1000.0 * 1000.0 * 1000.0);
  }
  static constexpr Bandwidth Zero() { return Bandwidth::FromBitsPerSecond(0); }
  static constexpr Bandwidth Max() {
    return Bandwidth::FromBitsPerSecond(std::numeric_limits<uint64_t>::max());
  }
  uint64_t bps() const { return m_val_; }
  double Kbps() const { return m_val_ / 1000.0; }
  double Mbps() const { return m_val_ / 1000.0 / 1000.0; }
  double Gbps() const { return m_val_ / 1000.0 / 1000.0 / 1000.0; }

  constexpr Bandwidth() : TypesafeUintWrapper<BandwidthTag, uint64_t>(0ul) {}

  Bandwidth& operator+=(const Bandwidth& other) {
    m_val_ += other.m_val_;
    return *this;
  }

  Bandwidth& operator-=(const Bandwidth& other) {
    m_val_ -= other.m_val_;
    return *this;
  }

  double operator/(const Bandwidth& other) const {
    return m_val_ / static_cast<double>(other.m_val_);
  }

  Bandwidth operator/(double fraction) const {
    return Bandwidth(m_val_ / fraction);
  }

  Bandwidth operator*(double fraction) const {
    return Bandwidth(m_val_ * fraction);
  }

  friend Bandwidth operator+(Bandwidth a, Bandwidth b) {
    return Bandwidth(a.m_val_ + b.m_val_);
  }

  friend Bandwidth operator-(Bandwidth a, Bandwidth b) {
    DCHECK(a.m_val_ >= b.m_val_);
    return Bandwidth(a.m_val_ - b.m_val_);
  }

 private:
  constexpr Bandwidth(uint64_t value_bps)
      : TypesafeUintWrapper<BandwidthTag, uint64_t>(value_bps) {}
};

// Delay in microseconds.
using Delay = std::chrono::microseconds;

// Define some constants.
static constexpr IPProto kProtoTCP = IPProto(6);
static constexpr IPProto kProtoUDP = IPProto(17);
static constexpr IPProto kProtoICMP = IPProto(1);

// Forward reference for the class that produces GraphLinkIndices and
// GraphNodeIndices.
class GraphStorage;

// The node is just an id.
class GraphNode {
 public:
  const std::string& id() const { return id_; }

 private:
  GraphNode(const std::string& id) : id_(id) {}

  std::string id_;

  friend class GraphStorage;
  DISALLOW_COPY_AND_ASSIGN(GraphNode);
};

using GraphNodeIndex = Index<GraphNode, uint16_t>;
using GraphNodeSet = PerfectHashSet<uint16_t, GraphNode>;

// Prints a set of nodes.
std::string GraphNodeSetToString(const GraphNodeSet& nodes,
                                 const GraphStorage& graph_storage);

template <typename V>
using GraphNodeMap = PerfectHashMap<uint16_t, GraphNode, V>;

// Contains all properties of a link.
class GraphLinkBase {
 public:
  GraphLinkBase(const std::string& src, const std::string& dst,
                DevicePortNumber src_port, DevicePortNumber dst_port,
                Bandwidth bw, Delay delay)
      : src_id_(src),
        dst_id_(dst),
        src_port_(src_port),
        dst_port_(dst_port),
        bandwidth_(bw),
        delay_(delay) {
    CHECK(delay_ != Delay::zero()) << "Link has zero delay";
    CHECK(bandwidth_ != Bandwidth::Zero()) << "Link has zero bandwidth";
  }

  GraphLinkBase(const std::string& src, const std::string& dst, Bandwidth bw,
                Delay delay)
      : GraphLinkBase(src, dst, DevicePortNumber::Zero(),
                      DevicePortNumber::Zero(), bw, delay) {}

  const std::string& src_id() const { return src_id_; }

  const std::string& dst_id() const { return dst_id_; }

  DevicePortNumber src_port() const { return src_port_; }

  DevicePortNumber dst_port() const { return dst_port_; }

  Delay delay() const { return delay_; }

  Bandwidth bandwidth() const { return bandwidth_; }

  // Returns a string in the form A:sport->B:dport
  std::string ToString() const;

  // Returns a string in the form A->B
  std::string ToStringNoPorts() const;

  friend bool operator==(const GraphLinkBase& a, const GraphLinkBase& b);

  void AddToBandwidth(Bandwidth bw) { bandwidth_ += bw; }

 private:
  std::string src_id_;
  std::string dst_id_;
  DevicePortNumber src_port_;
  DevicePortNumber dst_port_;
  Bandwidth bandwidth_;
  Delay delay_;
};

// Constructed by GraphStorage, extends GraphLinkBase with src/dst indices.
class GraphLink : public GraphLinkBase {
 public:
  GraphNodeIndex src() const { return src_; }

  const GraphNode* src_node() const { return src_node_; }

  GraphNodeIndex dst() const { return dst_; }

  const GraphNode* dst_node() const { return dst_node_; }

 private:
  GraphLink(const GraphLinkBase& base, GraphNodeIndex src, GraphNodeIndex dst,
            const GraphNode* src_node, const GraphNode* dst_node)
      : GraphLinkBase(base),
        src_(src),
        dst_(dst),
        src_node_(src_node),
        dst_node_(dst_node) {}

  GraphNodeIndex src_;
  GraphNodeIndex dst_;

  // For convenience we also store pointers to the edpoints.
  const GraphNode* src_node_;
  const GraphNode* dst_node_;

  friend class GraphStorage;
  DISALLOW_COPY_AND_ASSIGN(GraphLink);
};

// Will assign indices to graph links and the indices should be used instead of
// pointers to GraphLink objects. The GraphStorage class will be able to relate
// from indices back to GraphLink instances. This has two advantages -- the ids
// can be shorter than the 8 bytes required to hold a pointer, which results in
// significant memory savings if we store a lot of paths, and the indices can be
// allocated sequentially allowing for O(1) set/map operations with links.
using GraphLinkIndex = Index<GraphLink, uint16_t>;
using GraphLinkSet = PerfectHashSet<uint16_t, GraphLink>;

// Prints a set of links.
std::string GraphLinkSetToString(const GraphLinkSet& links,
                                 const GraphStorage& graph_storage);

template <typename V>
using GraphLinkMap = PerfectHashMap<uint16_t, GraphLink, V>;

// Just a bunch of links.
using Links = std::vector<GraphLinkIndex>;

// Sums up the delay along a series of links.
Delay TotalDelayOfLinks(const Links& links, const GraphStorage& graph_storage);

// Returns true if the given array of links has duplicates.
bool HasDuplicateLinks(const Links& links);

// Returns true if the given array of links has nodes.
bool HasDuplicateNodes(const Links& links, const GraphStorage& graph_storage);

// Returns the detour of 'path_two' from 'path_one'. Paths should start/end at
// the same node. The detour is returned as a pair of indices into path_two that
// indicate the start/end of the detour. If the two paths are the same {-1, -1}
// is returned.
std::pair<size_t, size_t> LinksDetour(const Links& path_one,
                                      const Links& path_two);

// A sequence of links along with a delay.
class Walk {
 public:
  Walk();
  Walk(const Links& links, Delay delay);
  Walk(const Links&& links, Delay delay);
  Walk(const Links& links, const GraphStorage& storage);

  // Returns true if any of the links in this walk is equal to link.
  bool Contains(GraphLinkIndex link) const;

  // Returns true if any of the links in this walk are in the set.
  bool ContainsAny(GraphLinkSet links) const;

  // Returns true if any of the nodes in this walk are in the set.
  bool ContainsAny(GraphNodeSet nodes, const GraphStorage& storage) const;

  // Returns true if there are no duplicate links in the walk.
  bool IsTrail() const;

  // Returns true if there are no duplicate nodes in the walk.
  bool IsPath(const GraphStorage& graph_storage) const;

  // The delay of all links in this walk.
  Delay delay() const;

  // Number of links in the walk.
  size_t size() const { return links_.size(); }

  // Whether or not there are any links in the walk.
  bool empty() const { return links_.empty(); }

  // The list of links.
  const Links& links() const { return links_; }

  // The set of links in the path.
  GraphLinkSet LinkSet() const;

  // String representation in the form [A:p1->B:p2, B:p3->C:p3]
  std::string ToString(const GraphStorage& storage) const;

  // Shorter string representation in the form [A->B->C]
  std::string ToStringNoPorts(const GraphStorage& storage) const;

  // Only prints out the ids of the nodes on the path, not their string
  // representation.
  std::string ToStringIdsOnly(const GraphStorage& storage) const;

  // Id of the first node along the path.
  GraphNodeIndex FirstHop(const GraphStorage& storage) const;

  // Id of the last node along the path.
  GraphNodeIndex LastHop(const GraphStorage& storage) const;

  // Returns the link(s) with the smallest bandwidth along the path. If the
  // optional bandwidth argument is specified will populate it with the
  // bandwidth of the links.
  GraphLinkSet BottleneckLinks(const GraphStorage& storage,
                               nc::net::Bandwidth* bandwidth = nullptr) const;

  // Rough estimate of the number of bytes of memory this Walk uses.
  size_t InMemBytesEstimate() const;

  friend bool operator<(const Walk& a, const Walk& b);
  friend bool operator>(const Walk& a, const Walk& b);
  friend bool operator==(const Walk& a, const Walk& b);
  friend bool operator!=(const Walk& a, const Walk& b);

 private:
  // The links in this walk.
  Links links_;

  // The total delay of all links in the walk.
  Delay delay_;

  DISALLOW_COPY_AND_ASSIGN(Walk);
};

// General statistics about a graph.
struct GraphStats {
  size_t links_count;
  size_t nodes_count;

  size_t unidirectional_links;
  size_t multiple_links;

  // The following vectors have 101 values each.
  std::vector<Bandwidth> link_capacity_percentiles;
  std::vector<Delay> link_delay_percentiles;
  std::vector<size_t> node_out_degree_percentiles;
  std::vector<size_t> node_in_degree_percentiles;

  // Percentiles of the delays of all shortest paths in the network. The max is
  // the diameter of the network.
  std::vector<Delay> sp_delay_percentiles;

  std::string ToString();
};

// Used to build a graph.
class GraphBuilder {
 public:
  GraphBuilder(bool auto_port_numbers = true)
      : auto_port_numbers_(auto_port_numbers) {}

  void AddLink(const GraphLinkBase& link);

  // Combines multiple links between the same nodes into a single link.
  void RemoveMultipleLinks();

  const std::vector<GraphLinkBase>& links() const { return links_; }

  // Scales the capacity of all links by a fraction.
  void ScaleCapacity(double fraction);

  // Scales the delay of all links by a fraction.
  void ScaleDelay(double fraction);

  // Serializes this builder's graph in the format from
  // https://bitbucket.org/StevenGay/repetita/src.
  std::string ToRepetita(const std::vector<std::string>& node_order = {}) const;

  // Returns the names of all nodes.
  std::set<std::string> AllNodeNames() const {
    std::set<std::string> out;
    for (const auto& link : links_) {
      out.emplace(link.src_id());
      out.emplace(link.dst_id());
    }

    return out;
  }

 private:
  // If true port numbers will be auto-assigned.
  bool auto_port_numbers_;

  // Each one of these will become a link in GraphStorage.
  std::vector<GraphLinkBase> links_;
};

// Maintains connectivity information about a graph, and allows for quick
// retrieval of link information (without going to a GraphStorage etc.).
class AdjacencyList {
 public:
  struct LinkInfo {
    GraphLinkIndex link_index;
    GraphNodeIndex src_index;
    GraphNodeIndex dst_index;
    Delay delay;
  };

  void AddLink(GraphLinkIndex link_index, GraphNodeIndex src,
               GraphNodeIndex dst, Delay delay) {
    adj_[src].push_back({link_index, src, dst, delay});
    all_nodes_.Insert(src);
    all_nodes_.Insert(dst);
  }

  void Clear() {
    adj_.Clear();
    all_nodes_.Clear();
  }

  // The neighbors of a node. Empty if the node is a leaf.
  const std::vector<LinkInfo>& GetNeighbors(const GraphNodeIndex node) const {
    if (!adj_.HasValue(node)) {
      return empty_;
    }

    return adj_.GetValueOrDie(node);
  }

  const GraphNodeSet& AllNodes() const { return all_nodes_; }

  const GraphNodeMap<std::vector<LinkInfo>>& Adjacencies() const {
    return adj_;
  }

 private:
  // An empty vector that GetNeighbors can return a reference to.
  std::vector<LinkInfo> empty_;

  // For each node its neighbors.
  GraphNodeMap<std::vector<LinkInfo>> adj_;

  // All nodes/links.
  GraphNodeSet all_nodes_;
};

// Stores and maintains a graph.
class GraphStorage {
 public:
  GraphStorage(const GraphBuilder& graph_builder);

  // Returns a GraphBuilder with the topology.
  GraphBuilder ToBuilder() const;

  // Returns a new GraphStorage, with some nodes from this GraphStorage
  // clustered. Creating clusters may lead to multiple links between two nodes,
  // even if the original graph is simple. This function will also assign names
  // to cluster nodes. For example if a cluster combines nodes N1 and N2 the
  // name of the cluster node will be C_N1_N2. Each node from this graph should
  // be contained in exactly one cluster.
  std::unique_ptr<GraphStorage> ClusterNodes(
      const std::vector<GraphNodeSet>& clusters,
      GraphLinkMap<GraphLinkIndex>* real_to_clustered_links,
      GraphNodeMap<GraphNodeIndex>* real_to_clustered_nodes) const;

  // Finds a link between src and dst. If multiple links exist will return only
  // one of them. Will die if no links exist.
  GraphLinkIndex LinkOrDie(const std::string& src,
                           const std::string& dst) const;

  // Returns a pointer to the index of a node or null if there is no node with
  // that name.
  const GraphNodeIndex* NodeFromStringOrNull(const std::string& id) const;

  // Same as NodeFromString, but dies if the node does not exist.
  GraphNodeIndex NodeFromStringOrDie(const std::string& id) const;

  // Attempts to find the unique inverse of a link. If the link has no inverse,
  // or has multiple inverses will die.
  GraphLinkIndex FindUniqueInverseOrDie(const GraphLink* link) const;

  // Same as above, but will return null if there is no inverse.
  const GraphLinkIndex* FindUniqueInverseOrNull(const GraphLink* link) const;

  // Relates from indices to link objects.
  const GraphLink* GetLink(GraphLinkIndex index) const;

  // Relates from indices to node objects.
  const GraphNode* GetNode(GraphNodeIndex index) const;

  // Number of links stored.
  size_t LinkCount() const { return link_store_.size(); }

  // Number of nodes stored.
  size_t NodeCount() const { return node_store_.size(); }

  // Returns the set of all nodes in the graph.
  GraphNodeSet AllNodes() const {
    return GraphNodeSet::FullSetFromStore(node_store_);
  }

  // Returns the set of all links in the graph.
  GraphLinkSet AllLinks() const {
    return GraphLinkSet::FullSetFromStore(link_store_);
  }

  // Returns statistics about the network.
  GraphStats Stats() const;

  // Combines LinkOrDie with GetLink for convenience.
  const GraphLink* LinkPtrOrDie(const std::string& src,
                                const std::string& dst) const {
    return GetLink(LinkOrDie(src, dst));
  }

  // True if there is at least one link between src and dst.
  bool HasLink(const std::string& src, const std::string& dst) const;

  // Node ID to node index.
  const std::map<std::string, GraphNodeIndex>& NodeIdToIndex() const {
    return nodes_;
  }

  // Returns a walk from a string of the form [A->B, B->C]. Port
  // numbers cannot be specified -- do not use if double edges are possible.
  std::unique_ptr<Walk> WalkFromStringOrDie(
      const std::string& path_string) const;

  // A convenience function equivalent to calling StringToPath followed by
  // std::find to check if haystack contains the walk needle.
  bool IsInWalks(const std::string& needle,
                 const std::vector<Walk>& haystack) const;

  // Returns the adjacency list.
  const net::AdjacencyList& AdjacencyList() const { return adjacency_list_; }

  // Returns true if there is at most one link between any two nodes.
  bool IsSimple() const { return simple_; }

 private:
  GraphStorage() : simple_(true) {}

  using LinkStore =
      PerfectHashStore<std::unique_ptr<GraphLink>, uint16_t, GraphLink>;
  using NodeStore =
      PerfectHashStore<std::unique_ptr<GraphNode>, uint16_t, GraphNode>;

  // Builds adjacency_list_. Called once upon construction.
  void PopulateAdjacencyList();

  // Returns the name of a cluster of nodes.
  std::string GetClusterName(const GraphNodeSet& nodes) const;

  // Returns the index of a node identified by a string.
  GraphNodeIndex NodeFromString(const std::string& id);

  // A map from src to dst to a list of links between that (src, dst) pair. The
  // list will only contain more than one element if double edges are used.
  std::map<std::string, std::map<std::string, Links>> links_;

  // Node id to node index.
  std::map<std::string, NodeStore::IndexType> nodes_;

  LinkStore link_store_;
  NodeStore node_store_;

  // The graph's adjacency list.
  net::AdjacencyList adjacency_list_;

  // True if there are no multiple edges between any two nodes.
  bool simple_;

  DISALLOW_COPY_AND_ASSIGN(GraphStorage);
};

// A five-tuple is a combination of ip src/dst, access layer src/dst ports and
// protocol type. It uniquely identifies an IP connection and can be used for
// matching.
class FiveTuple {
 public:
  static const FiveTuple kDefaultTuple;

  static constexpr std::size_t GetHash(IPAddress ip_src, IPAddress ip_dst,
                                       IPProto ip_proto,
                                       AccessLayerPort src_port,
                                       AccessLayerPort dst_port) {
    return 37 * (37 * (37 * (37 * (37 * 17 + ip_proto.Raw()) + ip_src.Raw()) +
                       ip_dst.Raw()) +
                 src_port.Raw()) +
           dst_port.Raw();
  }

  constexpr FiveTuple()
      : ip_src_(0),
        ip_dst_(0),
        ip_proto_(0),
        src_port_(0),
        dst_port_(0),
        hash_(0) {}

  constexpr FiveTuple(IPAddress ip_src, IPAddress ip_dst, IPProto ip_proto,
                      AccessLayerPort src_port, AccessLayerPort dst_port)
      : ip_src_(ip_src),
        ip_dst_(ip_dst),
        ip_proto_(ip_proto),
        src_port_(src_port),
        dst_port_(dst_port),
        hash_(GetHash(ip_src, ip_dst, ip_proto, src_port, dst_port)) {}

  // The access layer destination port.
  AccessLayerPort dst_port() const { return dst_port_; }

  // The IP destination address.
  IPAddress ip_dst() const { return ip_dst_; }

  // The IP protocol.
  IPProto ip_proto() const { return ip_proto_; }

  // The IP source address.
  IPAddress ip_src() const { return ip_src_; }

  // The access layer source port.
  AccessLayerPort src_port() const { return src_port_; }

  // This tuple's hash value.
  std::size_t hash() const { return hash_; }

  // Returns a FiveTuple that will match the other side of the connection that
  // this tuple matches. The new tuple will have src/dst address and ports
  // swapped.
  FiveTuple Reverse() const {
    return FiveTuple(ip_dst_, ip_src_, ip_proto_, dst_port_, src_port_);
  }

  // Same as the << operator, but returns a string.
  std::string ToString() const;

  // Pretty-printing comparion and equality.
  friend std::ostream& operator<<(std::ostream& output, const FiveTuple& op);
  friend bool operator==(const FiveTuple& a, const FiveTuple& b);
  friend bool operator!=(const FiveTuple& a, const FiveTuple& b);
  friend bool operator<(const FiveTuple& a, const FiveTuple& b);
  friend bool operator>(const FiveTuple& a, const FiveTuple& b);

 private:
  IPAddress ip_src_;
  IPAddress ip_dst_;
  IPProto ip_proto_;
  AccessLayerPort src_port_;
  AccessLayerPort dst_port_;

  // The hash value of the tuple is cached here.
  std::size_t hash_;
};

// Hashes a FiveTuple.
struct FiveTupleHasher {
  std::size_t operator()(const FiveTuple& k) const { return k.hash(); }
};

// Converts between strings and IPAddresses.
std::string IPToStringOrDie(IPAddress ip);
IPAddress StringToIPOrDie(const std::string& str);

// Applies a mask to a given IPAddress.
static constexpr uint8_t kMaxIPAddressMaskLen = 32;
IPAddress MaskAddress(IPAddress ip_address, uint8_t mask);

// An IPv4 range (combination of address and mask).
class IPRange {
 public:
  static constexpr const char* kDelimiter = "/";

  IPRange(IPAddress address, uint8_t mask_len);

  IPRange(const std::string& range_str);

  // The number of bits in the mask.
  uint8_t mask_len() const { return mask_len_; }

  // The base address.
  IPAddress base_address() const { return base_address_; }

  std::string ToString() const;

 private:
  void Init(IPAddress address, uint8_t mask_len);

  IPAddress base_address_;
  uint8_t mask_len_;
};

// Helper function for GetDisjointSets below.
template <typename T, typename V>
void RecursiveAddToDisjointSets(
    const std::map<V, std::vector<T>>& value_to_keys,
    const std::map<T, std::vector<V>>& key_to_values, const T& key,
    std::set<T>* out) {
  if (ContainsKey(*out, key)) {
    return;
  }
  out->insert(key);

  const std::vector<V>& values = FindOrDie(key_to_values, key);
  for (const V& value : values) {
    const std::vector<T>& keys_for_value = FindOrDie(value_to_keys, value);
    for (const T& key_for_value : keys_for_value) {
      if (key_for_value == key) {
        continue;
      }

      RecursiveAddToDisjointSets(value_to_keys, key_to_values, key_for_value,
                                 out);
    }
  }
}

// Given a map from a generic type to a list of values, this type will return
// lists of keys that do not share any single value.
template <typename T, typename V>
std::vector<std::set<T>> GetDisjointSets(
    const std::map<T, std::vector<V>>& key_to_values) {
  std::map<V, std::vector<T>> value_to_keys;
  for (const auto& key_and_values : key_to_values) {
    const T& key = key_and_values.first;
    const std::vector<V>& values = key_and_values.second;
    CHECK(!values.empty());

    for (const V& value : values) {
      value_to_keys[value].emplace_back(key);
    }
  }

  std::vector<std::set<T>> out;
  for (const auto& key_and_values : key_to_values) {
    const T& key = key_and_values.first;
    bool found = false;
    for (const auto& output_set : out) {
      if (ContainsKey(output_set, key)) {
        found = true;
        break;
      }
    }

    if (found) {
      continue;
    }

    std::set<T> new_set;
    RecursiveAddToDisjointSets(value_to_keys, key_to_values, key, &new_set);
    out.emplace_back(new_set);
  }

  return out;
}

}  // namespace nc
}  // namespace ncode

#endif
