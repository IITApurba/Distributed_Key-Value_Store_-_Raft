#pragma once
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "net/transport.h"
#include "net/wire.h"

namespace kvraft {

// Real peer-to-peer transport over TCP. Keeps one pooled, reused
// connection per peer (reconnecting lazily on failure) and dispatches RPCs
// from a small fixed worker pool so a slow/partitioned peer never blocks
// the Raft node's timer thread. Each call is a simple blocking
// request/response over the pooled socket; concurrency comes from the
// worker pool plus one connection per peer running independently.
class TcpTransport : public Transport {
public:
    TcpTransport(uint32_t shardId, std::unordered_map<NodeId, std::pair<std::string, int>> peerAddrs, size_t workers = 8)
        : shardId_(shardId), peerAddrs_(std::move(peerAddrs)) {
        for (size_t i = 0; i < workers; i++) workers_.emplace_back([this] { workerLoop(); });
    }
    ~TcpTransport() {
        {
            std::lock_guard<std::mutex> l(qmu_);
            stopping_ = true;
        }
        qcv_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
        std::lock_guard<std::mutex> l(connMu_);
        for (auto& [id, fd] : conns_) if (fd >= 0) close(fd);
    }

    void sendRequestVote(NodeId to, const RequestVoteReq& req, std::function<void(bool, RequestVoteResp)> cb) override {
        enqueue([this, to, req, cb] {
            Writer w; req.encode(w);
            std::string resp;
            bool ok = roundTrip(to, WireType::RequestVoteReq, w.str(), &resp);
            if (!ok) { cb(false, {}); return; }
            Reader r(resp);
            cb(true, RequestVoteResp::decode(r));
        });
    }
    void sendAppendEntries(NodeId to, const AppendEntriesReq& req, std::function<void(bool, AppendEntriesResp)> cb) override {
        enqueue([this, to, req, cb] {
            Writer w; req.encode(w);
            std::string resp;
            bool ok = roundTrip(to, WireType::AppendEntriesReq, w.str(), &resp);
            if (!ok) { cb(false, {}); return; }
            Reader r(resp);
            cb(true, AppendEntriesResp::decode(r));
        });
    }
    void sendInstallSnapshot(NodeId to, const InstallSnapshotReq& req, std::function<void(bool, InstallSnapshotResp)> cb) override {
        enqueue([this, to, req, cb] {
            Writer w; req.encode(w);
            std::string resp;
            bool ok = roundTrip(to, WireType::InstallSnapshotReq, w.str(), &resp);
            if (!ok) { cb(false, {}); return; }
            Reader r(resp);
            cb(true, InstallSnapshotResp::decode(r));
        });
    }

private:
    void enqueue(std::function<void()> job) {
        std::lock_guard<std::mutex> l(qmu_);
        if (stopping_) return;
        queue_.push_back(std::move(job));
        qcv_.notify_one();
    }
    void workerLoop() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> l(qmu_);
                qcv_.wait(l, [this] { return stopping_ || !queue_.empty(); });
                if (stopping_ && queue_.empty()) return;
                job = std::move(queue_.front());
                queue_.pop_front();
            }
            job();
        }
    }

    int getConn(NodeId to) {
        std::lock_guard<std::mutex> l(connMu_);
        auto it = conns_.find(to);
        if (it != conns_.end() && it->second >= 0) return it->second;
        auto addrIt = peerAddrs_.find(to);
        if (addrIt == peerAddrs_.end()) return -1;
        int fd = connectTo(addrIt->second.first, addrIt->second.second);
        conns_[to] = fd;
        return fd;
    }
    void dropConn(NodeId to) {
        std::lock_guard<std::mutex> l(connMu_);
        auto it = conns_.find(to);
        if (it != conns_.end()) { if (it->second >= 0) close(it->second); it->second = -1; }
    }

    bool roundTrip(NodeId to, WireType type, const std::string& payload, std::string* respPayload) {
        int fd = getConn(to);
        if (fd < 0) return false;
        Writer w;
        w.putU32(shardId_);
        std::string framed = w.str() + payload;
        if (!writeFrame(fd, type, framed)) { dropConn(to); return false; }
        WireType respType;
        if (!readFrame(fd, &respType, respPayload)) { dropConn(to); return false; }
        return true;
    }

    uint32_t shardId_;
    std::unordered_map<NodeId, std::pair<std::string, int>> peerAddrs_;
    std::mutex connMu_;
    std::unordered_map<NodeId, int> conns_;

    std::mutex qmu_;
    std::condition_variable qcv_;
    std::deque<std::function<void()>> queue_;
    bool stopping_ = false;
    std::vector<std::thread> workers_;
};

} // namespace kvraft
