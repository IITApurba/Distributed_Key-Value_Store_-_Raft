#pragma once
#include <atomic>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <vector>
#include "kv/kv_store.h"
#include "net/event_loop.h"
#include "net/wire.h"
#include "shard/shard_manager.h"

namespace kvraft {

// Client-facing KV server: one listening socket, N epoll worker threads
// (each its own event loop) round-robin the accepted connections across
// them. This is the "concurrent RPC processing" path the resume bullet
// refers to -- non-blocking sockets, edge-triggered epoll, no per-connection
// thread, and critical sections limited to the RaftNode's internal mutex
// (locked only for the duration of a log append / commit-index read).
class KvServer {
public:
    KvServer(int port, std::shared_ptr<ShardManager> shards, int numWorkers = 4)
        : port_(port), shards_(std::move(shards)), numWorkers_(numWorkers) {}

    void start() {
        listenFd_ = socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        listen(listenFd_, 1024);
        setNonBlocking(listenFd_);

        for (int i = 0; i < numWorkers_; i++) {
            loops_.push_back(std::make_unique<EventLoop>());
            workers_.emplace_back([this, i] { workerLoop(*loops_[i]); });
        }
        acceptThread_ = std::thread([this] { acceptLoop(); });
    }

    void stop() {
        running_ = false;
        if (listenFd_ >= 0) close(listenFd_);
        if (acceptThread_.joinable()) acceptThread_.join();
        for (auto& t : workers_) if (t.joinable()) t.join();
    }

private:
    struct ConnState { std::string inbuf; };

    void acceptLoop() {
        int rr = 0;
        while (running_) {
            sockaddr_in cli{};
            socklen_t len = sizeof(cli);
            int fd = accept(listenFd_, reinterpret_cast<sockaddr*>(&cli), &len);
            if (fd < 0) { if (!running_) break; continue; }
            setNonBlocking(fd);
            {
                std::lock_guard<std::mutex> l(stateMu_);
                connState_[fd] = ConnState{};
            }
            loops_[rr % loops_.size()]->addRead(fd);
            rr++;
        }
    }

    void workerLoop(EventLoop& loop) {
        while (running_) {
            auto ready = loop.wait(200);
            for (auto& [fd, hup] : ready) {
                if (hup) { closeConn(loop, fd); continue; }
                handleReadable(loop, fd);
            }
        }
    }

    void handleReadable(EventLoop& loop, int fd) {
        char buf[65536];
        for (;;) {
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                std::lock_guard<std::mutex> l(stateMu_);
                connState_[fd].inbuf.append(buf, n);
            } else if (n == 0) { closeConn(loop, fd); return; }
            else { if (errno == EAGAIN || errno == EWOULDBLOCK) break; closeConn(loop, fd); return; }
        }
        // drain complete frames (may be several pipelined requests)
        for (;;) {
            std::string frame;
            {
                std::lock_guard<std::mutex> l(stateMu_);
                auto& in = connState_[fd].inbuf;
                if (in.size() < 5) break;
                uint32_t len = (static_cast<uint8_t>(in[1]) << 24) | (static_cast<uint8_t>(in[2]) << 16) |
                               (static_cast<uint8_t>(in[3]) << 8) | static_cast<uint8_t>(in[4]);
                if (in.size() < 5 + len) break;
                frame = in.substr(5, len);
                in.erase(0, 5 + len);
            }
            std::string respPayload = dispatch(frame);
            writeFrame(fd, WireType::ClientResponse, respPayload);
        }
    }

    void closeConn(EventLoop& loop, int fd) {
        loop.remove(fd);
        close(fd);
        std::lock_guard<std::mutex> l(stateMu_);
        connState_.erase(fd);
    }

    // Client request wire format: 1 byte op, key, value/expect (Cas), clientId, seq.
    // Response: 1 byte status(0=ok,1=not_found,2=not_leader,3=cas_mismatch,4=timeout), leaderHint(bytes), value(bytes)
    std::string dispatch(const std::string& frame) {
        Reader r(frame);
        uint8_t op = r.getU8();
        std::string key = r.getBytes();
        uint32_t shardId = shards_->shardFor(key);
        auto* grp = shards_->group(shardId);
        Writer w;
        if (!grp) { w.putU8(2); w.putBytes(""); w.putBytes(""); return w.str(); }

        if (op == 0) { // GET (linearizable via read-index)
            if (!grp->raft->isLeader() || !grp->raft->linearizableReadBarrier(500)) {
                w.putU8(2); w.putBytes(addrOf(grp)); w.putBytes("");
                return w.str();
            }
            std::string val;
            if (grp->sm->get(key, &val)) { w.putU8(0); w.putBytes(""); w.putBytes(val); }
            else { w.putU8(1); w.putBytes(""); w.putBytes(""); }
            return w.str();
        }
        // PUT / DEL / CAS go through the log
        std::string value = r.getBytes();
        std::string expect = r.getBytes();
        uint64_t clientId = r.getU64();
        uint64_t seq = r.getU64();
        KvCommand cmd{static_cast<CmdOp>(op == 1 ? 0 : op == 2 ? 1 : 2), key, value, expect, clientId, seq};
        auto pr = grp->raft->propose(cmd.encode());
        if (!pr.isLeader) { w.putU8(2); w.putBytes(addrOf(grp)); w.putBytes(""); return w.str(); }
        std::string result;
        if (!grp->raft->waitApplied(pr.index, 2000, &result)) { w.putU8(4); w.putBytes(""); w.putBytes(""); return w.str(); }
        if (result == "CAS_MISMATCH") { w.putU8(3); w.putBytes(""); w.putBytes(""); return w.str(); }
        w.putU8(0); w.putBytes(""); w.putBytes(result);
        return w.str();
    }

    std::string addrOf(ShardGroup* grp) {
        auto cfg = grp->raft->currentConfig();
        NodeId hint = grp->raft->leaderHint();
        for (auto& [id, addr] : cfg.addresses) if (id == hint) return addr;
        return "";
    }

    int port_;
    std::shared_ptr<ShardManager> shards_;
    int numWorkers_;
    int listenFd_ = -1;
    std::atomic<bool> running_{true};
    std::vector<std::unique_ptr<EventLoop>> loops_;
    std::vector<std::thread> workers_;
    std::thread acceptThread_;
    std::mutex stateMu_;
    std::unordered_map<int, ConnState> connState_;
};

} // namespace kvraft
