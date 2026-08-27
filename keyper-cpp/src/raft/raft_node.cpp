#include "raft/raft_node.h"
#include <algorithm>
#include <chrono>
#include <iostream>

namespace kvraft {

using namespace std::chrono;

RaftNode::RaftNode(RaftOptions opts, ClusterConfig initialConfig,
                     std::shared_ptr<Transport> transport, std::shared_ptr<StateMachine> sm)
    : opts_(std::move(opts)), transport_(std::move(transport)), sm_(std::move(sm)),
      config_(std::move(initialConfig)), rng_(std::random_device{}() ^ opts_.id) {
    log_ = std::make_unique<PersistentLog>(opts_.walDir);
    auto snap = log_->loadSnapshotBytes();
    if (snap) sm_->restore(*snap);
    commitIndex_ = log_->snapshotIndex();
    lastApplied_ = log_->snapshotIndex();
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::start() {
    running_ = true;
    resetElectionDeadline();
    loopThread_ = std::thread([this] { runLoop(); });
}

void RaftNode::stop() {
    if (!running_.exchange(false)) return;
    if (loopThread_.joinable()) loopThread_.join();
}

void RaftNode::resetElectionDeadline() {
    std::uniform_int_distribution<int> dist(opts_.electionTimeoutMinMs, opts_.electionTimeoutMaxMs);
    electionDeadline_ = steady_clock::now() + milliseconds(dist(rng_));
}

void RaftNode::runLoop() {
    auto lastHeartbeat = steady_clock::now();
    while (running_) {
        std::this_thread::sleep_for(milliseconds(10));
        Role r = role_.load();
        auto now = steady_clock::now();
        if (r == Role::Leader) {
            if (now - lastHeartbeat >= milliseconds(opts_.heartbeatIntervalMs)) {
                broadcastAppendEntries();
                lastHeartbeat = now;
            }
        } else {
            bool expired;
            { std::lock_guard<std::mutex> l(mu_); expired = now >= electionDeadline_; }
            if (expired) becomeCandidate();
        }
        maybeSnapshot();
    }
}

void RaftNode::becomeFollower(Term term, NodeId leader) {
    std::lock_guard<std::mutex> l(mu_);
    if (term > log_->currentTerm()) log_->setHardState(term, 0);
    role_ = Role::Follower;
    leaderId_ = leader;
    resetElectionDeadline();
}

void RaftNode::becomeCandidate() {
    Term newTerm;
    ClusterConfig cfg;
    LogIndex lastIdx; Term lastTerm;
    {
        std::lock_guard<std::mutex> l(mu_);
        newTerm = log_->currentTerm() + 1;
        log_->setHardState(newTerm, opts_.id);
        role_ = Role::Candidate;
        leaderId_ = 0;
        resetElectionDeadline();
        cfg = config_;
        lastIdx = log_->lastIndex();
        lastTerm = log_->lastTerm();
    }
    if (!cfg.hasVoter(opts_.id) && cfg.voters.size() > 0) return; // not a voting member

    auto votes = std::make_shared<std::atomic<int>>(1); // vote for self
    auto majority = static_cast<int>(cfg.voters.size() / 2 + 1);
    auto self = shared_from_this();

    if (*votes >= majority) { becomeLeader(); return; } // single-node cluster

    RequestVoteReq req{newTerm, opts_.id, lastIdx, lastTerm};
    for (NodeId peer : cfg.voters) {
        if (peer == opts_.id) continue;
        transport_->sendRequestVote(peer, req, [self, newTerm, votes, majority](bool ok, RequestVoteResp resp) {
            if (!ok) return;
            std::unique_lock<std::mutex> l(self->mu_);
            if (self->role_.load() != Role::Candidate || self->log_->currentTerm() != newTerm) return;
            if (resp.term > newTerm) { self->mu_.unlock(); self->becomeFollower(resp.term, 0); self->mu_.lock(); return; }
            if (resp.voteGranted) {
                int v = ++(*votes);
                if (v >= majority) {
                    self->mu_.unlock();
                    self->becomeLeader();
                    self->mu_.lock();
                }
            }
        });
    }
}

void RaftNode::becomeLeader() {
    ClusterConfig cfg;
    LogIndex last;
    {
        std::lock_guard<std::mutex> l(mu_);
        if (role_.load() != Role::Candidate) return;
        role_ = Role::Leader;
        leaderId_ = opts_.id;
        cfg = config_;
        last = log_->lastIndex();
        nextIndex_.clear();
        matchIndex_.clear();
        for (auto v : cfg.voters) { nextIndex_[v] = last + 1; matchIndex_[v] = 0; }
    }
    // Commit a no-op entry so this leader's commitIndex can advance past
    // entries from prior terms (Raft §5.4.2 safety requirement).
    propose("", EntryType::NoOp);
    tryAdvanceCommitIndex(); // handles the single-node-cluster case, where no peer ack ever arrives
    broadcastAppendEntries();
}

void RaftNode::broadcastAppendEntries() {
    ClusterConfig cfg;
    { std::lock_guard<std::mutex> l(mu_); if (role_.load() != Role::Leader) return; cfg = config_; }
    for (auto peer : cfg.voters) {
        if (peer == opts_.id) continue;
        LogIndex ni;
        { std::lock_guard<std::mutex> l(mu_); ni = nextIndex_.count(peer) ? nextIndex_[peer] : 1; }
        if (ni <= log_->snapshotIndex()) sendInstallSnapshotTo(peer);
        else sendAppendEntriesTo(peer);
    }
}

void RaftNode::sendAppendEntriesTo(NodeId peer) {
    Term term; LogIndex prevIdx; Term prevTerm; std::vector<LogEntry> entries; LogIndex commit;
    {
        std::lock_guard<std::mutex> l(mu_);
        if (role_.load() != Role::Leader) return;
        term = log_->currentTerm();
        LogIndex ni = nextIndex_.count(peer) ? nextIndex_[peer] : 1;
        prevIdx = ni - 1;
        prevTerm = log_->termAt(prevIdx);
        entries = log_->entriesAfter(prevIdx, 200);
        commit = commitIndex_;
    }
    AppendEntriesReq req{term, opts_.id, prevIdx, prevTerm, entries, commit};
    auto self = shared_from_this();
    transport_->sendAppendEntries(peer, req, [self, peer, term](bool ok, AppendEntriesResp resp) {
        if (!ok) return;
        std::unique_lock<std::mutex> l(self->mu_);
        if (resp.term > self->log_->currentTerm()) { self->mu_.unlock(); self->becomeFollower(resp.term, 0); self->mu_.lock(); return; }
        if (self->role_.load() != Role::Leader || self->log_->currentTerm() != term) return;
        if (resp.success) {
            self->matchIndex_[peer] = resp.matchIndex;
            self->nextIndex_[peer] = resp.matchIndex + 1;
            self->mu_.unlock();
            self->tryAdvanceCommitIndex();
            self->mu_.lock();
        } else {
            // back off using the follower's conflict hint (Raft optimization)
            LogIndex newNext = resp.conflictIndex > 0 ? resp.conflictIndex : 1;
            self->nextIndex_[peer] = std::max<LogIndex>(1, newNext);
        }
    });
}

void RaftNode::sendInstallSnapshotTo(NodeId peer) {
    Term term; LogIndex idx; Term snapTerm; std::string bytes;
    {
        std::lock_guard<std::mutex> l(mu_);
        if (role_.load() != Role::Leader) return;
        term = log_->currentTerm();
        idx = log_->snapshotIndex();
        snapTerm = log_->snapshotTerm();
    }
    auto s = log_->loadSnapshotBytes();
    bytes = s ? *s : sm_->snapshot();
    InstallSnapshotReq req{term, opts_.id, idx, snapTerm, bytes};
    auto self = shared_from_this();
    transport_->sendInstallSnapshot(peer, req, [self, peer, idx](bool ok, InstallSnapshotResp resp) {
        if (!ok) return;
        std::unique_lock<std::mutex> l(self->mu_);
        if (resp.term > self->log_->currentTerm()) { self->mu_.unlock(); self->becomeFollower(resp.term, 0); self->mu_.lock(); return; }
        self->matchIndex_[peer] = idx;
        self->nextIndex_[peer] = idx + 1;
    });
}

void RaftNode::tryAdvanceCommitIndex() {
    std::unique_lock<std::mutex> l(mu_);
    if (role_.load() != Role::Leader) return;
    std::vector<LogIndex> matches;
    matches.push_back(log_->lastIndex()); // leader's own match
    for (auto& [id, m] : matchIndex_) if (id != opts_.id) matches.push_back(m);
    std::sort(matches.begin(), matches.end(), std::greater<LogIndex>());
    size_t majorityOff = config_.voters.size() / 2; // index of the median-high value
    if (majorityOff >= matches.size()) return;
    LogIndex candidate = matches[majorityOff];
    if (candidate > commitIndex_ && log_->termAt(candidate) == log_->currentTerm()) {
        commitIndex_ = candidate;
        mu_.unlock();
        applyCommittedEntries();
        mu_.lock();
    }
}

void RaftNode::applyCommittedEntries() {
    std::vector<LogEntry> toApply;
    {
        std::unique_lock<std::mutex> l(mu_);
        while (lastApplied_ < commitIndex_) {
            auto e = log_->at(lastApplied_ + 1);
            if (!e) break;
            toApply.push_back(*e);
            lastApplied_++;
        }
    }
    for (auto& e : toApply) {
        std::string result;
        if (e.type == EntryType::Command) result = sm_->apply(e.data, e.index);
        else if (e.type == EntryType::Config) applyConfigEntry(e);
        std::lock_guard<std::mutex> l(mu_);
        pendingResults_[e.index] = result;
        // satisfy any linearizable-read barriers now covered by this apply
        pendingReadBarriers_.erase(
            std::remove_if(pendingReadBarriers_.begin(), pendingReadBarriers_.end(),
                            [&](const ReadBarrier& b) { return e.index >= b.targetCommit; }),
            pendingReadBarriers_.end());
    }
    if (!toApply.empty()) appliedCv_.notify_all();
}

void RaftNode::applyConfigEntry(const LogEntry& e) {
    std::lock_guard<std::mutex> l(mu_);
    Reader r(e.data);
    config_ = ClusterConfig::decode(r);
    if (role_.load() == Role::Leader) {
        for (auto v : config_.voters) {
            if (!nextIndex_.count(v)) { nextIndex_[v] = log_->lastIndex() + 1; matchIndex_[v] = 0; }
        }
    }
}

RequestVoteResp RaftNode::handleRequestVote(const RequestVoteReq& req) {
    std::unique_lock<std::mutex> l(mu_);
    if (req.term < log_->currentTerm()) return {log_->currentTerm(), false};
    if (req.term > log_->currentTerm()) {
        mu_.unlock(); becomeFollower(req.term, 0); mu_.lock();
    }
    bool canVote = (log_->votedFor() == 0 || log_->votedFor() == req.candidateId);
    LogIndex myLast = log_->lastIndex();
    Term myLastTerm = log_->lastTerm();
    bool logOk = (req.lastLogTerm > myLastTerm) ||
                 (req.lastLogTerm == myLastTerm && req.lastLogIndex >= myLast);
    if (canVote && logOk) {
        log_->setHardState(log_->currentTerm(), req.candidateId);
        resetElectionDeadline();
        return {log_->currentTerm(), true};
    }
    return {log_->currentTerm(), false};
}

AppendEntriesResp RaftNode::handleAppendEntries(const AppendEntriesReq& req) {
    std::unique_lock<std::mutex> l(mu_);
    if (req.term < log_->currentTerm()) return {log_->currentTerm(), false, 0, 0, 0};
    mu_.unlock();
    becomeFollower(req.term, req.leaderId);
    mu_.lock();
    resetElectionDeadline();
    leaderId_ = req.leaderId;

    if (req.prevLogIndex > 0) {
        Term t = log_->termAt(req.prevLogIndex);
        if (t == 0 && req.prevLogIndex > log_->snapshotIndex()) {
            return {log_->currentTerm(), false, log_->lastIndex() + 1, 0, log_->lastIndex()};
        }
        if (t != 0 && t != req.prevLogTerm) {
            // find first index of the conflicting term to let leader skip fast
            LogIndex ci = req.prevLogIndex;
            while (ci > log_->snapshotIndex() + 1 && log_->termAt(ci - 1) == t) ci--;
            return {log_->currentTerm(), false, ci, t, 0};
        }
    }
    if (!req.entries.empty()) log_->appendAndSync(req.entries);
    LogIndex newLast = req.entries.empty() ? req.prevLogIndex : req.entries.back().index;
    if (req.leaderCommit > commitIndex_) commitIndex_ = std::min(req.leaderCommit, newLast);
    mu_.unlock();
    applyCommittedEntries();
    mu_.lock();
    return {log_->currentTerm(), true, newLast, 0, 0};
}

InstallSnapshotResp RaftNode::handleInstallSnapshot(const InstallSnapshotReq& req) {
    std::unique_lock<std::mutex> l(mu_);
    if (req.term < log_->currentTerm()) return {log_->currentTerm()};
    mu_.unlock();
    becomeFollower(req.term, req.leaderId);
    mu_.lock();
    if (req.lastIncludedIndex <= log_->snapshotIndex()) return {log_->currentTerm()};
    log_->compactUpTo(req.lastIncludedIndex, req.lastIncludedTerm, req.data);
    sm_->restore(req.data);
    commitIndex_ = std::max(commitIndex_, req.lastIncludedIndex);
    lastApplied_ = std::max(lastApplied_, req.lastIncludedIndex);
    return {log_->currentTerm()};
}

ProposeResult RaftNode::propose(const std::string& commandBytes, EntryType type) {
    std::unique_lock<std::mutex> l(mu_);
    if (role_.load() != Role::Leader) return {false, leaderId_, 0, 0};
    LogEntry e;
    e.term = log_->currentTerm();
    e.index = log_->lastIndex() + 1;
    e.type = type;
    e.data = commandBytes;
    log_->appendAndSync({e});
    l.unlock();
    tryAdvanceCommitIndex(); // no-op unless this is a single-node cluster
    return {true, opts_.id, e.index, e.term};
}

bool RaftNode::waitApplied(LogIndex index, int timeoutMs, std::string* outResult) {
    std::unique_lock<std::mutex> l(mu_);
    bool ok = appliedCv_.wait_for(l, milliseconds(timeoutMs), [&] { return lastApplied_ >= index; });
    if (ok && outResult) {
        auto it = pendingResults_.find(index);
        if (it != pendingResults_.end()) *outResult = it->second;
    }
    return ok;
}

bool RaftNode::linearizableReadBarrier(int timeoutMs) {
    LogIndex target;
    {
        std::lock_guard<std::mutex> l(mu_);
        if (role_.load() != Role::Leader) return false;
        target = commitIndex_;
    }
    // A successful heartbeat round after recording `target` proves this
    // node was still leader when target was the commit index, which is
    // sufficient to serve reads as of `target` linearizably.
    broadcastAppendEntries();
    std::unique_lock<std::mutex> l(mu_);
    return appliedCv_.wait_for(l, milliseconds(timeoutMs), [&] { return lastApplied_ >= target; });
}

ProposeResult RaftNode::addServer(NodeId id, const std::string& address) {
    ClusterConfig newCfg;
    {
        std::lock_guard<std::mutex> l(mu_);
        newCfg = config_;
        if (!newCfg.hasVoter(id)) newCfg.voters.push_back(id);
        bool found = false;
        for (auto& p : newCfg.addresses) if (p.first == id) { p.second = address; found = true; }
        if (!found) newCfg.addresses.emplace_back(id, address);
    }
    Writer w; newCfg.encode(w);
    return propose(w.str(), EntryType::Config);
}

ProposeResult RaftNode::removeServer(NodeId id) {
    ClusterConfig newCfg;
    {
        std::lock_guard<std::mutex> l(mu_);
        newCfg = config_;
        newCfg.voters.erase(std::remove(newCfg.voters.begin(), newCfg.voters.end(), id), newCfg.voters.end());
    }
    Writer w; newCfg.encode(w);
    return propose(w.str(), EntryType::Config);
}

void RaftNode::maybeSnapshot() {
    LogIndex last, snapIdx;
    {
        std::lock_guard<std::mutex> l(mu_);
        last = lastApplied_;
        snapIdx = log_->snapshotIndex();
    }
    if (last - snapIdx >= opts_.snapshotThresholdEntries) forceSnapshot();
}

void RaftNode::forceSnapshot() {
    LogIndex idx;
    Term term;
    {
        std::lock_guard<std::mutex> l(mu_);
        idx = lastApplied_;
        if (idx <= log_->snapshotIndex()) return;
        term = log_->termAt(idx);
    }
    std::string bytes = sm_->snapshot();
    log_->compactUpTo(idx, term, bytes);
}

} // namespace kvraft
