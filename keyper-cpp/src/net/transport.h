#pragma once
#include <functional>
#include "raft/rpc_messages.h"

namespace kvraft {

// Transport abstracts how RaftNode instances exchange RPCs. Two
// implementations exist:
//   - TcpTransport: real epoll-driven sockets for a live cluster.
//   - TestTransport: in-process, deterministic, and fault-injectable
//     (drop/delay/partition/duplicate) for Jepsen-style testing.
// RaftNode only depends on this interface, never on sockets directly.
class Transport {
public:
    virtual ~Transport() = default;

    virtual void sendRequestVote(NodeId to, const RequestVoteReq& req,
                                  std::function<void(bool ok, RequestVoteResp)> cb) = 0;
    virtual void sendAppendEntries(NodeId to, const AppendEntriesReq& req,
                                    std::function<void(bool ok, AppendEntriesResp)> cb) = 0;
    virtual void sendInstallSnapshot(NodeId to, const InstallSnapshotReq& req,
                                      std::function<void(bool ok, InstallSnapshotResp)> cb) = 0;
};

} // namespace kvraft
