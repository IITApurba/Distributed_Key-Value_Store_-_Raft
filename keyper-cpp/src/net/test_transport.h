#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>
#include "net/transport.h"
#include "raft/raft_node.h"

namespace kvraft {

// Shared in-process "network" for a set of RaftNode instances plus a
// Jepsen-style fault-injection surface: full partitions between node
// groups, random packet loss, random delay, and message duplication.
// Deterministic wall-clock aside, this lets tests exercise real Raft
// safety/liveness properties under adversarial network conditions without
// touching real sockets.
class FaultyNetwork {
public:
    FaultyNetwork(size_t workers = 8) : stopping_(false) {
        for (size_t i = 0; i < workers; i++) workerThreads_.emplace_back([this] { workerLoop(); });
        schedulerThread_ = std::thread([this] { schedulerLoop(); });
    }
    ~FaultyNetwork() {
        { std::lock_guard<std::mutex> l(mu2_); stopping_ = true; }
        cv2_.notify_all();
        if (schedulerThread_.joinable()) schedulerThread_.join();
        for (auto& t : workerThreads_) if (t.joinable()) t.join();
    }

    void registerNode(NodeId id, std::shared_ptr<RaftNode> node) {
        std::lock_guard<std::mutex> l(mu_);
        nodes_[id] = node;
    }

    // Splits the cluster into two partitions; nodes in different partitions
    // cannot reach each other until healPartition() is called.
    void partition(const std::set<NodeId>& groupA) {
        std::lock_guard<std::mutex> l(mu_);
        partitioned_ = true;
        groupA_ = groupA;
    }
    void healPartition() { std::lock_guard<std::mutex> l(mu_); partitioned_ = false; }

    void setDropRate(double p) { dropRate_ = p; }
    void setDuplicateRate(double p) { duplicateRate_ = p; }
    void setDelayMs(int minMs, int maxMs) { delayMinMs_ = minMs; delayMaxMs_ = maxMs; }
    void killNode(NodeId id) { std::lock_guard<std::mutex> l(mu_); dead_.insert(id); }
    void reviveNode(NodeId id) { std::lock_guard<std::mutex> l(mu_); dead_.erase(id); }

    bool reachable(NodeId a, NodeId b) {
        std::lock_guard<std::mutex> l(mu_);
        if (dead_.count(a) || dead_.count(b)) return false;
        if (!partitioned_) return true;
        bool aIn = groupA_.count(a) > 0, bIn = groupA_.count(b) > 0;
        return aIn == bIn;
    }

    std::shared_ptr<RaftNode> find(NodeId id) {
        std::lock_guard<std::mutex> l(mu_);
        auto it = nodes_.find(id);
        return it == nodes_.end() ? nullptr : it->second;
    }

    // Schedules `fn` to run after a random fault-injected delay, on a small
    // fixed worker pool (never a thread-per-message -- important once a test
    // fuzzes thousands of RPCs across many iterations), possibly multiple
    // times (duplication) or never (drop). `roll()` drives all random
    // decisions so tests can seed it for reproducibility.
    template <typename Fn>
    void deliver(Fn fn) {
        if (roll() < dropRate_) return; // simulate packet loss
        int copies = (roll() < duplicateRate_) ? 2 : 1;
        int lo = delayMinMs_.load(), hi = delayMaxMs_.load();
        int delay = lo >= hi ? lo : lo + static_cast<int>(roll() * (hi - lo));
        auto fireAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay);
        std::lock_guard<std::mutex> l(mu2_);
        for (int i = 0; i < copies; i++) timers_.emplace(fireAt, fn);
        cv2_.notify_all();
    }

    double roll() {
        static thread_local std::mt19937 rng(std::random_device{}());
        static thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(rng);
    }

private:
    void schedulerLoop() {
        std::unique_lock<std::mutex> l(mu2_);
        while (!stopping_) {
            if (timers_.empty()) { cv2_.wait_for(l, std::chrono::milliseconds(20)); continue; }
            auto next = timers_.begin()->first;
            if (cv2_.wait_until(l, next, [this] { return stopping_ || (!timers_.empty() && timers_.begin()->first <= std::chrono::steady_clock::now()); })) {
                if (stopping_) return;
                while (!timers_.empty() && timers_.begin()->first <= std::chrono::steady_clock::now()) {
                    auto fn = std::move(timers_.begin()->second);
                    timers_.erase(timers_.begin());
                    l.unlock();
                    { std::lock_guard<std::mutex> ql(qmu_); jobQueue_.push_back(std::move(fn)); }
                    qcv_.notify_one();
                    l.lock();
                }
            }
        }
    }
    void workerLoop() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> l(qmu_);
                qcv_.wait(l, [this] { return stopping_ || !jobQueue_.empty(); });
                if (stopping_ && jobQueue_.empty()) return;
                if (jobQueue_.empty()) continue;
                job = std::move(jobQueue_.front());
                jobQueue_.pop_front();
            }
            job();
        }
    }

    std::mutex mu_;
    std::unordered_map<NodeId, std::shared_ptr<RaftNode>> nodes_;
    bool partitioned_ = false;
    std::set<NodeId> groupA_;
    std::set<NodeId> dead_;
    std::atomic<double> dropRate_{0.0};
    std::atomic<double> duplicateRate_{0.0};
    std::atomic<int> delayMinMs_{0};
    std::atomic<int> delayMaxMs_{0};

    // delay-scheduling + bounded worker pool (replaces thread-per-message)
    std::mutex mu2_;
    std::condition_variable cv2_;
    std::multimap<std::chrono::steady_clock::time_point, std::function<void()>> timers_;
    std::mutex qmu_;
    std::condition_variable qcv_;
    std::deque<std::function<void()>> jobQueue_;
    std::vector<std::thread> workerThreads_;
    std::thread schedulerThread_;
    bool stopping_;
};

class TestTransport : public Transport {
public:
    TestTransport(NodeId self, std::shared_ptr<FaultyNetwork> net) : self_(self), net_(std::move(net)) {}

    void sendRequestVote(NodeId to, const RequestVoteReq& req,
                          std::function<void(bool, RequestVoteResp)> cb) override {
        if (!net_->reachable(self_, to)) { cb(false, {}); return; }
        auto target = net_->find(to);
        auto net = net_;
        auto self = self_;
        net_->deliver([target, net, self, to, req, cb] {
            if (!target || !net->reachable(self, to)) { cb(false, {}); return; }
            cb(true, target->handleRequestVote(req));
        });
    }
    void sendAppendEntries(NodeId to, const AppendEntriesReq& req,
                            std::function<void(bool, AppendEntriesResp)> cb) override {
        if (!net_->reachable(self_, to)) { cb(false, {}); return; }
        auto target = net_->find(to);
        auto net = net_;
        auto self = self_;
        net_->deliver([target, net, self, to, req, cb] {
            if (!target || !net->reachable(self, to)) { cb(false, {}); return; }
            cb(true, target->handleAppendEntries(req));
        });
    }
    void sendInstallSnapshot(NodeId to, const InstallSnapshotReq& req,
                              std::function<void(bool, InstallSnapshotResp)> cb) override {
        if (!net_->reachable(self_, to)) { cb(false, {}); return; }
        auto target = net_->find(to);
        auto net = net_;
        auto self = self_;
        net_->deliver([target, net, self, to, req, cb] {
            if (!target || !net->reachable(self, to)) { cb(false, {}); return; }
            cb(true, target->handleInstallSnapshot(req));
        });
    }

private:
    NodeId self_;
    std::shared_ptr<FaultyNetwork> net_;
};

} // namespace kvraft
