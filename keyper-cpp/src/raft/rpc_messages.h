#pragma once
#include "raft/types.h"

namespace kvraft {

enum class MsgType : uint8_t {
    RequestVoteReq = 1, RequestVoteResp = 2,
    AppendEntriesReq = 3, AppendEntriesResp = 4,
    InstallSnapshotReq = 5, InstallSnapshotResp = 6,
    ReadIndexReq = 7, ReadIndexResp = 8,
};

struct RequestVoteReq {
    Term term; NodeId candidateId; LogIndex lastLogIndex; Term lastLogTerm;
    void encode(Writer& w) const { w.putU64(term); w.putU64(candidateId); w.putU64(lastLogIndex); w.putU64(lastLogTerm); }
    static RequestVoteReq decode(Reader& r) { return {r.getU64(), static_cast<NodeId>(r.getU64()), r.getU64(), r.getU64()}; }
};
struct RequestVoteResp {
    Term term; bool voteGranted;
    void encode(Writer& w) const { w.putU64(term); w.putBool(voteGranted); }
    static RequestVoteResp decode(Reader& r) { Term t = r.getU64(); bool g = r.getBool(); return {t, g}; }
};

struct AppendEntriesReq {
    Term term; NodeId leaderId; LogIndex prevLogIndex; Term prevLogTerm;
    std::vector<LogEntry> entries; LogIndex leaderCommit;
    void encode(Writer& w) const {
        w.putU64(term); w.putU64(leaderId); w.putU64(prevLogIndex); w.putU64(prevLogTerm);
        w.putU32(static_cast<uint32_t>(entries.size()));
        for (auto& e : entries) e.encode(w);
        w.putU64(leaderCommit);
    }
    static AppendEntriesReq decode(Reader& r) {
        AppendEntriesReq req;
        req.term = r.getU64(); req.leaderId = static_cast<NodeId>(r.getU64());
        req.prevLogIndex = r.getU64(); req.prevLogTerm = r.getU64();
        uint32_t n = r.getU32();
        for (uint32_t i = 0; i < n; i++) req.entries.push_back(LogEntry::decode(r));
        req.leaderCommit = r.getU64();
        return req;
    }
};
struct AppendEntriesResp {
    Term term; bool success; LogIndex matchIndex; LogIndex conflictIndex; Term conflictTerm;
    void encode(Writer& w) const { w.putU64(term); w.putBool(success); w.putU64(matchIndex); w.putU64(conflictIndex); w.putU64(conflictTerm); }
    static AppendEntriesResp decode(Reader& r) {
        AppendEntriesResp resp;
        resp.term = r.getU64(); resp.success = r.getBool(); resp.matchIndex = r.getU64();
        resp.conflictIndex = r.getU64(); resp.conflictTerm = r.getU64();
        return resp;
    }
};

struct InstallSnapshotReq {
    Term term; NodeId leaderId; LogIndex lastIncludedIndex; Term lastIncludedTerm; std::string data;
    void encode(Writer& w) const { w.putU64(term); w.putU64(leaderId); w.putU64(lastIncludedIndex); w.putU64(lastIncludedTerm); w.putBytes(data); }
    static InstallSnapshotReq decode(Reader& r) {
        InstallSnapshotReq req;
        req.term = r.getU64(); req.leaderId = static_cast<NodeId>(r.getU64());
        req.lastIncludedIndex = r.getU64(); req.lastIncludedTerm = r.getU64(); req.data = r.getBytes();
        return req;
    }
};
struct InstallSnapshotResp {
    Term term;
    void encode(Writer& w) const { w.putU64(term); }
    static InstallSnapshotResp decode(Reader& r) { return {r.getU64()}; }
};

// Read-index request: leader confirms it is still leader (via a quorum of
// heartbeat acks) before answering, giving linearizable reads without
// putting the read through the log.
struct ReadIndexReq {
    NodeId requester;
    void encode(Writer& w) const { w.putU64(requester); }
    static ReadIndexReq decode(Reader& r) { return {static_cast<NodeId>(r.getU64())}; }
};
struct ReadIndexResp {
    bool ok; LogIndex readIndex;
    void encode(Writer& w) const { w.putBool(ok); w.putU64(readIndex); }
    static ReadIndexResp decode(Reader& r) { bool ok = r.getBool(); return {ok, r.getU64()}; }
};

} // namespace kvraft
