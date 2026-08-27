#pragma once
#include <cstdint>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>

namespace kvraft {

// Wire framing shared by the Raft peer transport and the client-facing KV
// server: [1 byte msg type][4 byte big-endian length][payload].
enum class WireType : uint8_t {
    RequestVoteReq = 1, RequestVoteResp = 2,
    AppendEntriesReq = 3, AppendEntriesResp = 4,
    InstallSnapshotReq = 5, InstallSnapshotResp = 6,
    ClientRequest = 10, ClientResponse = 11,
};

inline void setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

inline int connectTo(const std::string& host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) { close(fd); return -1; }
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { close(fd); return -1; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

inline bool writeAll(int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, data + sent, len - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

inline bool writeFrame(int fd, WireType type, const std::string& payload) {
    uint8_t hdr[5];
    hdr[0] = static_cast<uint8_t>(type);
    uint32_t len = static_cast<uint32_t>(payload.size());
    hdr[1] = (len >> 24) & 0xFF; hdr[2] = (len >> 16) & 0xFF; hdr[3] = (len >> 8) & 0xFF; hdr[4] = len & 0xFF;
    if (!writeAll(fd, reinterpret_cast<char*>(hdr), 5)) return false;
    return writeAll(fd, payload.data(), payload.size());
}

inline bool readExact(int fd, char* buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd, buf + got, len - got, 0);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

inline bool readFrame(int fd, WireType* type, std::string* payload) {
    uint8_t hdr[5];
    if (!readExact(fd, reinterpret_cast<char*>(hdr), 5)) return false;
    *type = static_cast<WireType>(hdr[0]);
    uint32_t len = (static_cast<uint32_t>(hdr[1]) << 24) | (static_cast<uint32_t>(hdr[2]) << 16) |
                   (static_cast<uint32_t>(hdr[3]) << 8) | static_cast<uint32_t>(hdr[4]);
    payload->resize(len);
    if (len == 0) return true;
    return readExact(fd, &(*payload)[0], len);
}

} // namespace kvraft
