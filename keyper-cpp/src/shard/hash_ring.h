#pragma once
#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace kvraft {

// Consistent-hashing ring mapping keys to shard IDs. Each shard gets
// `vnodes` virtual points on the ring so adding/removing a shard only
// moves ~1/N of the keyspace instead of a full rehash, which is what
// makes online partition migration cheap.
class HashRing {
public:
    explicit HashRing(int vnodes = 128) : vnodes_(vnodes) {}

    void addShard(uint32_t shardId) {
        std::lock_guard<std::mutex> l(mu_);
        for (int v = 0; v < vnodes_; v++) {
            uint64_t h = hash(std::to_string(shardId) + "#" + std::to_string(v));
            ring_[h] = shardId;
        }
        shards_.push_back(shardId);
    }

    void removeShard(uint32_t shardId) {
        std::lock_guard<std::mutex> l(mu_);
        for (int v = 0; v < vnodes_; v++) {
            uint64_t h = hash(std::to_string(shardId) + "#" + std::to_string(v));
            ring_.erase(h);
        }
        shards_.erase(std::remove(shards_.begin(), shards_.end(), shardId), shards_.end());
    }

    uint32_t shardFor(const std::string& key) const {
        std::lock_guard<std::mutex> l(mu_);
        if (ring_.empty()) return 0;
        uint64_t h = hash(key);
        auto it = ring_.lower_bound(h);
        if (it == ring_.end()) it = ring_.begin();
        return it->second;
    }

    std::vector<uint32_t> shards() const { std::lock_guard<std::mutex> l(mu_); return shards_; }

    static uint64_t hash(const std::string& s) {
        // FNV-1a 64-bit
        uint64_t h = 1469598103934665603ULL;
        for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
        return h;
    }

private:
    int vnodes_;
    mutable std::mutex mu_;
    std::map<uint64_t, uint32_t> ring_; // ring position -> shard id
    std::vector<uint32_t> shards_;
};

} // namespace kvraft
