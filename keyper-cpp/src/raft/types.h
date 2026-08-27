#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "util/buffer.h"

namespace kvraft {

using NodeId = uint32_t;
using Term = uint64_t;
using LogIndex = uint64_t;

enum class EntryType : uint8_t { Command = 0, Config = 1, NoOp = 2 };

// A single Raft log entry. `data` holds an opaque command for Command
// entries, or a serialized ClusterConfig for Config entries.
struct LogEntry {
    Term term = 0;
    LogIndex index = 0;
    EntryType type = EntryType::Command;
    std::string data;

    void encode(Writer& w) const {
        w.putU64(term);
        w.putU64(index);
        w.putU8(static_cast<uint8_t>(type));
        w.putBytes(data);
    }
    static LogEntry decode(Reader& r) {
        LogEntry e;
        e.term = r.getU64();
        e.index = r.getU64();
        e.type = static_cast<EntryType>(r.getU8());
        e.data = r.getBytes();
        return e;
    }
};

// Cluster membership: set of voting node ids and their host:port addresses.
struct ClusterConfig {
    std::vector<NodeId> voters;
    std::vector<std::pair<NodeId, std::string>> addresses; // id -> host:port

    void encode(Writer& w) const {
        w.putU32(static_cast<uint32_t>(voters.size()));
        for (auto v : voters) w.putU64(v);
        w.putU32(static_cast<uint32_t>(addresses.size()));
        for (auto& p : addresses) {
            w.putU64(p.first);
            w.putBytes(p.second);
        }
    }
    static ClusterConfig decode(Reader& r) {
        ClusterConfig c;
        uint32_t n = r.getU32();
        for (uint32_t i = 0; i < n; i++) c.voters.push_back(static_cast<NodeId>(r.getU64()));
        uint32_t m = r.getU32();
        for (uint32_t i = 0; i < m; i++) {
            NodeId id = static_cast<NodeId>(r.getU64());
            std::string addr = r.getBytes();
            c.addresses.emplace_back(id, addr);
        }
        return c;
    }
    bool hasVoter(NodeId id) const {
        for (auto v : voters) if (v == id) return true;
        return false;
    }
};

// Command applied to the KV state machine. Serialized inside LogEntry::data.
enum class CmdOp : uint8_t { Put = 0, Del = 1, Cas = 2 };

struct KvCommand {
    CmdOp op;
    std::string key;
    std::string value;
    std::string expect; // for CAS
    uint64_t clientId = 0;
    uint64_t requestSeq = 0; // for de-duplication (exactly-once semantics)

    std::string encode() const {
        Writer w;
        w.putU8(static_cast<uint8_t>(op));
        w.putBytes(key);
        w.putBytes(value);
        w.putBytes(expect);
        w.putU64(clientId);
        w.putU64(requestSeq);
        return w.str();
    }
    static KvCommand decode(const std::string& s) {
        Reader r(s);
        KvCommand c;
        c.op = static_cast<CmdOp>(r.getU8());
        c.key = r.getBytes();
        c.value = r.getBytes();
        c.expect = r.getBytes();
        c.clientId = r.getU64();
        c.requestSeq = r.getU64();
        return c;
    }
};

} // namespace kvraft
