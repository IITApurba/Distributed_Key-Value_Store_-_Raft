#pragma once
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "net/wire.h"
#include "shard/hash_ring.h"

namespace kvraft {

// Client library: routes each key to its shard via the same consistent
// hash ring the servers use, pools one TCP connection per server address,
// and retries against the leader hint returned on a NOT_LEADER response.
class KvClient {
public:
    KvClient(HashRing ring, std::unordered_map<uint32_t, std::vector<std::string>> shardServers)
        : ring_(std::move(ring)), shardServers_(std::move(shardServers)), clientId_(nextClientId()) {}

    bool put(const std::string& key, const std::string& value) {
        std::string result;
        return request(1, key, value, "", &result);
    }
    bool get(const std::string& key, std::string* value) {
        return request(0, key, "", "", value);
    }
    bool del(const std::string& key) {
        std::string result;
        return request(2, key, "", "", &result);
    }
    bool cas(const std::string& key, const std::string& expect, const std::string& value) {
        std::string result;
        return request(3, key, value, expect, &result);
    }

private:
    static uint64_t nextClientId() {
        static std::atomic<uint64_t> counter{1};
        return counter.fetch_add(1) * 1000003ULL ^ reinterpret_cast<uint64_t>(&counter);
    }

    int pooledConn(const std::string& addr) {
        std::lock_guard<std::mutex> l(mu_);
        auto it = conns_.find(addr);
        if (it != conns_.end() && it->second >= 0) return it->second;
        auto pos = addr.find(':');
        std::string host = addr.substr(0, pos);
        int port = std::stoi(addr.substr(pos + 1));
        int fd = connectTo(host, port);
        conns_[addr] = fd;
        return fd;
    }
    void dropConn(const std::string& addr) {
        std::lock_guard<std::mutex> l(mu_);
        auto it = conns_.find(addr);
        if (it != conns_.end()) { if (it->second >= 0) close(it->second); it->second = -1; }
    }

    bool request(uint8_t op, const std::string& key, const std::string& value,
                 const std::string& expect, std::string* outValue) {
        uint32_t shard = ring_.shardFor(key);
        auto it = shardServers_.find(shard);
        if (it == shardServers_.end() || it->second.empty()) return false;
        std::string addr = it->second[0]; // start with a cached/likely leader

        for (int attempt = 0; attempt < 5; attempt++) {
            int fd = pooledConn(addr);
            if (fd < 0) { addr = it->second[attempt % it->second.size()]; continue; }
            Writer w;
            w.putU8(op); w.putBytes(key);
            if (op != 0) { w.putBytes(value); w.putBytes(expect); w.putU64(clientId_); w.putU64(seq_.fetch_add(1)); }
            if (!writeFrame(fd, WireType::ClientRequest, w.str())) { dropConn(addr); continue; }
            WireType t; std::string resp;
            if (!readFrame(fd, &t, &resp)) { dropConn(addr); continue; }
            Reader r(resp);
            uint8_t status = r.getU8();
            std::string leaderHint = r.getBytes();
            std::string val = r.getBytes();
            if (status == 0) { if (outValue) *outValue = val; return true; }
            if (status == 2 && !leaderHint.empty()) { addr = leaderHint; continue; } // redirect to leader
            if (status == 1) return false; // not found
            // cas mismatch / timeout: surface as failure to caller
            return false;
        }
        return false;
    }

    HashRing ring_;
    std::unordered_map<uint32_t, std::vector<std::string>> shardServers_;
    uint64_t clientId_;
    std::atomic<uint64_t> seq_{1};
    std::mutex mu_;
    std::unordered_map<std::string, int> conns_;
};

} // namespace kvraft
