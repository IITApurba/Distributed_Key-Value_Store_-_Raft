#pragma once
#include <shared_mutex>
#include <unordered_map>
#include "raft/state_machine.h"
#include "raft/types.h"

namespace kvraft {

// Deterministic key-value state machine applied from the committed Raft
// log. De-duplicates client requests by (clientId, requestSeq) so a
// retried write after a leader failover is not applied twice.
class KvStore : public StateMachine {
public:
    std::string apply(const std::string& commandBytes, uint64_t logIndex) override {
        KvCommand cmd = KvCommand::decode(commandBytes);
        std::unique_lock<std::shared_mutex> l(mu_);
        auto dedupKey = std::make_pair(cmd.clientId, cmd.requestSeq);
        if (cmd.clientId != 0) {
            auto it = lastSeq_.find(cmd.clientId);
            if (it != lastSeq_.end() && it->second.first >= cmd.requestSeq) {
                return it->second.second; // already applied; return cached result
            }
        }
        std::string result;
        switch (cmd.op) {
            case CmdOp::Put: data_[cmd.key] = cmd.value; result = "OK"; break;
            case CmdOp::Del: data_.erase(cmd.key); result = "OK"; break;
            case CmdOp::Cas: {
                auto it = data_.find(cmd.key);
                std::string cur = it == data_.end() ? "" : it->second;
                if (cur == cmd.expect) { data_[cmd.key] = cmd.value; result = "OK"; }
                else result = "CAS_MISMATCH";
                break;
            }
        }
        if (cmd.clientId != 0) lastSeq_[cmd.clientId] = {cmd.requestSeq, result};
        return result;
    }

    bool get(const std::string& key, std::string* out) const {
        std::shared_lock<std::shared_mutex> l(mu_);
        auto it = data_.find(key);
        if (it == data_.end()) return false;
        *out = it->second;
        return true;
    }

    std::string snapshot() const override {
        std::shared_lock<std::shared_mutex> l(mu_);
        Writer w;
        w.putU32(static_cast<uint32_t>(data_.size()));
        for (auto& [k, v] : data_) { w.putBytes(k); w.putBytes(v); }
        w.putU32(static_cast<uint32_t>(lastSeq_.size()));
        for (auto& [cid, seqRes] : lastSeq_) { w.putU64(cid); w.putU64(seqRes.first); w.putBytes(seqRes.second); }
        return w.str();
    }

    void restore(const std::string& snapshotBytes) override {
        std::unique_lock<std::shared_mutex> l(mu_);
        data_.clear();
        lastSeq_.clear();
        if (snapshotBytes.empty()) return;
        Reader r(snapshotBytes);
        uint32_t n = r.getU32();
        for (uint32_t i = 0; i < n; i++) { std::string k = r.getBytes(); std::string v = r.getBytes(); data_[k] = v; }
        uint32_t m = r.getU32();
        for (uint32_t i = 0; i < m; i++) {
            uint64_t cid = r.getU64(); uint64_t seq = r.getU64(); std::string res = r.getBytes();
            lastSeq_[cid] = {seq, res};
        }
    }

    size_t size() const { std::shared_lock<std::shared_mutex> l(mu_); return data_.size(); }

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::string> data_;
    std::unordered_map<uint64_t, std::pair<uint64_t, std::string>> lastSeq_;
};

} // namespace kvraft
