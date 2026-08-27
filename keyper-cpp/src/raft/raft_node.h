#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>

#include "net/transport.h"
#include "raft/persistent_log.h"
#include "raft/state_machine.h"
#include "raft/types.h"

namespace kvraft {

enum class Role { Follower, Candidate, Leader };

struct RaftOptions {
    NodeId id;
    std::string walDir;
    int electionTimeoutMinMs = 150;
    int electionTimeoutMaxMs = 300;
    int heartbeatIntervalMs = 50;
    size_t snapshotThresholdEntries = 10000; // compact after this many log entries since last snapshot
};

// Result of a client proposal submitted to the leader.
struct ProposeResult {
    bool isLeader;
    NodeId leaderHint;
    LogIndex index;
    Term term;
};

// A single Raft consensus group. Owns persistence, replication, membership
// changes, snapshotting and read-index linearizable reads. Networked via
// the injected Transport so the same class runs over real TCP or an
// in-process fault-injecting test transport.
class RaftNode : public std::enable_shared_from_this<RaftNode> {
public:
    RaftNode(RaftOptions opts, ClusterConfig initialConfig,
              std::shared_ptr<Transport> transport, std::shared_ptr<StateMachine> sm);
    ~RaftNode();

    void start();
    void stop();

    // ---- RPC handlers (invoked by the network layer on message receipt) ----
    RequestVoteResp handleRequestVote(const RequestVoteReq& req);
    AppendEntriesResp handleAppendEntries(const AppendEntriesReq& req);
    InstallSnapshotResp handleInstallSnapshot(const InstallSnapshotReq& req);

    // ---- client-facing API ----
    // Appends `commandBytes` to the log if this node is leader. Non-blocking;
    // caller polls/awaits via `waitCommitted` or the apply callback.
    ProposeResult propose(const std::string& commandBytes, EntryType type = EntryType::Command);

    // Blocks (with timeout) until `index` is applied to the state machine,
    // returning the apply() result. Used by the KV server to answer writes
    // only after they're durably committed on a majority.
    bool waitApplied(LogIndex index, int timeoutMs, std::string* outResult);

    // Linearizable read: confirms leadership via a heartbeat quorum round,
    // then blocks until the state machine has applied at least that index.
    bool linearizableReadBarrier(int timeoutMs);

    // Single-server membership change (Raft §6): safe because only one
    // server's membership differs between the old and new configuration at
    // any time, so overlapping quorums are guaranteed without joint consensus.
    ProposeResult addServer(NodeId id, const std::string& address);
    ProposeResult removeServer(NodeId id);

    Role role() const { return role_.load(); }
    Term currentTerm() const { return log_->currentTerm(); }
    NodeId id() const { return opts_.id; }
    NodeId leaderHint() const { std::lock_guard<std::mutex> l(mu_); return leaderId_; }
    bool isLeader() const { return role_.load() == Role::Leader; }
    LogIndex commitIndex() const { std::lock_guard<std::mutex> l(mu_); return commitIndex_; }
    ClusterConfig currentConfig() const { std::lock_guard<std::mutex> l(mu_); return config_; }

    // For tests: force an immediate snapshot regardless of threshold.
    void forceSnapshot();

private:
    void runLoop();                     // background timer thread
    void resetElectionDeadline();
    void becomeFollower(Term term, NodeId leader);
    void becomeCandidate();
    void becomeLeader();
    void broadcastAppendEntries();      // leader heartbeat/replication fan-out
    void sendAppendEntriesTo(NodeId peer);
    void sendInstallSnapshotTo(NodeId peer);
    void tryAdvanceCommitIndex();
    void applyCommittedEntries();
    void maybeSnapshot();
    void applyConfigEntry(const LogEntry& e);

    RaftOptions opts_;
    std::shared_ptr<Transport> transport_;
    std::shared_ptr<StateMachine> sm_;
    std::unique_ptr<PersistentLog> log_;

    mutable std::mutex mu_;
    std::condition_variable appliedCv_;
    std::atomic<Role> role_{Role::Follower};
    ClusterConfig config_;
    NodeId leaderId_ = 0;
    LogIndex commitIndex_ = 0;
    LogIndex lastApplied_ = 0;
    std::chrono::steady_clock::time_point electionDeadline_;
    std::unordered_map<NodeId, LogIndex> nextIndex_;
    std::unordered_map<NodeId, LogIndex> matchIndex_;
    std::unordered_map<LogIndex, std::string> pendingResults_;

    // Read-index bookkeeping: outstanding heartbeat quorum rounds used to
    // confirm leadership before answering a linearizable read.
    struct ReadBarrier { LogIndex targetCommit; int acks; };
    std::vector<ReadBarrier> pendingReadBarriers_;

    std::atomic<bool> running_{false};
    std::thread loopThread_;
    std::mt19937 rng_;
};

} // namespace kvraft
