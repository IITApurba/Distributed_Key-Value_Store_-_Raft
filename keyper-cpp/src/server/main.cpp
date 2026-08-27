// Node process entry point.
//
// Usage: keyper_server --id=1 --shard=0 --wal=/tmp/keyper/1 \
//          --peer-port=6100 --client-port=7100 \
//          --peers=1:127.0.0.1:6100,2:127.0.0.1:6101,3:127.0.0.1:6102
//
// Starts one Raft group (one shard) in this process, listening for peer
// RPCs on --peer-port and client requests on --client-port. Run one
// process per node; run several processes with different --shard values
// (each its own 3-5 node Raft group) to build a sharded cluster.
#include <filesystem>
#include <iostream>
#include <sstream>
#include <thread>
#include "kv/kv_store.h"
#include "net/tcp_transport.h"
#include "raft/raft_node.h"
#include "server/kv_server.h"
#include "server/peer_server.h"
#include "shard/shard_manager.h"

using namespace kvraft;

static std::unordered_map<std::string, std::string> parseArgs(int argc, char** argv) {
    std::unordered_map<std::string, std::string> args;
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        auto pos = a.find('=');
        if (a.rfind("--", 0) == 0 && pos != std::string::npos) args[a.substr(2, pos - 2)] = a.substr(pos + 1);
    }
    return args;
}

int main(int argc, char** argv) {
    auto args = parseArgs(argc, argv);
    NodeId id = static_cast<NodeId>(std::stoul(args.at("id")));
    uint32_t shardId = args.count("shard") ? static_cast<uint32_t>(std::stoul(args["shard"])) : 0;
    std::string walDir = args.count("wal") ? args["wal"] : ("/tmp/keyper/" + args["id"]);
    int peerPort = std::stoi(args.at("peer-port"));
    int clientPort = std::stoi(args.at("client-port"));

    ClusterConfig cfg;
    std::unordered_map<NodeId, std::pair<std::string, int>> peerAddrs;
    std::stringstream ss(args.at("peers"));
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        auto p1 = tok.find(':');
        auto p2 = tok.find(':', p1 + 1);
        NodeId pid = static_cast<NodeId>(std::stoul(tok.substr(0, p1)));
        std::string host = tok.substr(p1 + 1, p2 - p1 - 1);
        int port = std::stoi(tok.substr(p2 + 1));
        cfg.voters.push_back(pid);
        cfg.addresses.emplace_back(pid, host + ":" + std::to_string(port));
        if (pid != id) peerAddrs[pid] = {host, port};
    }

    std::filesystem::create_directories(walDir);
    auto sm = std::make_shared<KvStore>();
    auto transport = std::make_shared<TcpTransport>(shardId, peerAddrs);
    RaftOptions opts;
    opts.id = id;
    opts.walDir = walDir;
    auto node = std::make_shared<RaftNode>(opts, cfg, transport, sm);
    node->start();

    PeerServer peerServer(peerPort);
    peerServer.registerShard(shardId, node);
    peerServer.start();

    auto shards = std::make_shared<ShardManager>();
    shards->addShard(ShardGroup{shardId, node, sm});

    KvServer kvServer(clientPort, shards);
    kvServer.start();

    std::cout << "keyper node " << id << " shard " << shardId
              << " peer_port=" << peerPort << " client_port=" << clientPort
              << " wal=" << walDir << std::endl;

    // Block forever; SIGTERM/SIGKILL to stop the process.
    for (;;) std::this_thread::sleep_for(std::chrono::seconds(60));
}
