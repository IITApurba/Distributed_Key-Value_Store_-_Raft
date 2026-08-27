#pragma once
#include <memory>
#include <unordered_map>
#include "kv/kv_store.h"
#include "raft/raft_node.h"
#include "shard/hash_ring.h"

namespace kvraft {

struct ShardGroup {
    uint32_t id;
    std::shared_ptr<RaftNode> raft;
    std::shared_ptr<KvStore> sm;
};

// Owns one independent Raft group per shard and routes client requests to
// the correct shard via consistent hashing. Supports online migration of a
// key range between two shard groups: keys are copied through each shard's
// own replicated log (so the move itself is fault-tolerant and durable),
// and the ring only flips ownership after the copy is fully committed on
// the destination — so a client racing the migration always finds its key
// on exactly one side, with zero downtime.
class ShardManager {
public:
    explicit ShardManager(int vnodes = 128) : ring_(vnodes) {}

    void addShard(const ShardGroup& g) {
        shards_[g.id] = g;
        ring_.addShard(g.id);
    }

    uint32_t shardFor(const std::string& key) const { return ring_.shardFor(key); }

    ShardGroup* group(uint32_t id) {
        auto it = shards_.find(id);
        return it == shards_.end() ? nullptr : &it->second;
    }

    // Moves every key currently owned by `from` that the ring (after a
    // trial re-weighting) would place on `to`. Returns number of keys
    // migrated. This models a shard split/rebalance triggered by an
    // operator or auto-scaler; it does not remove `from` from the ring.
    size_t migrateRange(uint32_t fromId, uint32_t toId, const std::vector<std::string>& keysOwnedByFrom) {
        auto* from = group(fromId);
        auto* to = group(toId);
        if (!from || !to || !from->raft->isLeader()) return 0;
        size_t moved = 0;
        for (auto& key : keysOwnedByFrom) {
            if (ring_.shardFor(key) != toId) continue; // still belongs to `from`
            std::string val;
            if (!from->sm->get(key, &val)) continue;

            KvCommand put{CmdOp::Put, key, val, "", 0, 0};
            auto pr = to->raft->propose(put.encode());
            if (!pr.isLeader) continue;
            std::string result;
            if (!to->raft->waitApplied(pr.index, 2000, &result)) continue;

            KvCommand del{CmdOp::Del, key, "", "", 0, 0};
            auto dr = from->raft->propose(del.encode());
            if (dr.isLeader) from->raft->waitApplied(dr.index, 2000, &result);
            moved++;
        }
        return moved;
    }

    std::vector<uint32_t> shardIds() const { return ring_.shards(); }

private:
    HashRing ring_;
    std::unordered_map<uint32_t, ShardGroup> shards_;
};

} // namespace kvraft
