#pragma once
#include <thread>
#include <unordered_map>
#include "net/wire.h"
#include "raft/raft_node.h"

namespace kvraft {

// Accepts Raft peer RPC connections. One thread per connection is fine
// here: peer fan-in is O(cluster size), unlike the client-facing path
// (KvServer) which needs epoll to scale to thousands of clients.
// The wire payload for every peer RPC is prefixed with a 4-byte shard id
// so a single port can serve multiple Raft groups per process.
class PeerServer {
public:
    explicit PeerServer(int port) : port_(port) {}

    void registerShard(uint32_t shardId, std::shared_ptr<RaftNode> node) {
        std::lock_guard<std::mutex> l(mu_);
        shards_[shardId] = std::move(node);
    }

    void start() {
        listenFd_ = socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        listen(listenFd_, 256);
        acceptThread_ = std::thread([this] { acceptLoop(); });
    }
    void stop() {
        running_ = false;
        if (listenFd_ >= 0) shutdown(listenFd_, SHUT_RDWR);
        if (listenFd_ >= 0) close(listenFd_);
        if (acceptThread_.joinable()) acceptThread_.join();
    }

private:
    void acceptLoop() {
        while (running_) {
            int fd = accept(listenFd_, nullptr, nullptr);
            if (fd < 0) { if (!running_) break; continue; }
            std::thread(&PeerServer::connLoop, this, fd).detach();
        }
    }
    void connLoop(int fd) {
        for (;;) {
            WireType type;
            std::string payload;
            if (!readFrame(fd, &type, &payload)) break;
            if (payload.size() < 4) break;
            Reader idR(payload.substr(0, 4));
            uint32_t shardId = idR.getU32();
            std::string body = payload.substr(4);
            std::shared_ptr<RaftNode> node;
            {
                std::lock_guard<std::mutex> l(mu_);
                auto it = shards_.find(shardId);
                if (it != shards_.end()) node = it->second;
            }
            if (!node) break;
            Writer respPayload;
            WireType respType;
            Reader r(body);
            if (type == WireType::RequestVoteReq) {
                auto resp = node->handleRequestVote(RequestVoteReq::decode(r));
                resp.encode(respPayload);
                respType = WireType::RequestVoteResp;
            } else if (type == WireType::AppendEntriesReq) {
                auto resp = node->handleAppendEntries(AppendEntriesReq::decode(r));
                resp.encode(respPayload);
                respType = WireType::AppendEntriesResp;
            } else if (type == WireType::InstallSnapshotReq) {
                auto resp = node->handleInstallSnapshot(InstallSnapshotReq::decode(r));
                resp.encode(respPayload);
                respType = WireType::InstallSnapshotResp;
            } else break;
            if (!writeFrame(fd, respType, respPayload.str())) break;
        }
        close(fd);
    }

    int port_;
    int listenFd_ = -1;
    std::atomic<bool> running_{true};
    std::thread acceptThread_;
    std::mutex mu_;
    std::unordered_map<uint32_t, std::shared_ptr<RaftNode>> shards_;
};

} // namespace kvraft
