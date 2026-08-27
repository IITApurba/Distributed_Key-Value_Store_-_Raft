#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <stdexcept>

// Minimal binary encode/decode buffer used for on-disk WAL records and
// wire-format RPC messages. Avoids pulling in protobuf/flatbuffers so the
// whole project builds with nothing but the standard library + POSIX sockets.
namespace kvraft {

class Writer {
public:
    void putU8(uint8_t v) { buf_.push_back(v); }
    void putU32(uint32_t v) {
        for (int i = 0; i < 4; i++) buf_.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    }
    void putU64(uint64_t v) {
        for (int i = 0; i < 8; i++) buf_.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    }
    void putBool(bool v) { putU8(v ? 1 : 0); }
    void putBytes(const std::string& s) {
        putU32(static_cast<uint32_t>(s.size()));
        buf_.insert(buf_.end(), s.begin(), s.end());
    }
    const std::vector<uint8_t>& data() const { return buf_; }
    std::string str() const { return std::string(buf_.begin(), buf_.end()); }
    size_t size() const { return buf_.size(); }

private:
    std::vector<uint8_t> buf_;
};

class Reader {
public:
    Reader(const uint8_t* data, size_t len) : data_(data), len_(len), pos_(0) {}
    explicit Reader(const std::string& s) : owned_(s), data_(reinterpret_cast<const uint8_t*>(owned_.data())), len_(owned_.size()), pos_(0) {}

    uint8_t getU8() {
        require(1);
        return data_[pos_++];
    }
    uint32_t getU32() {
        require(4);
        uint32_t v = 0;
        for (int i = 0; i < 4; i++) v |= (static_cast<uint32_t>(data_[pos_++]) << (i * 8));
        return v;
    }
    uint64_t getU64() {
        require(8);
        uint64_t v = 0;
        for (int i = 0; i < 8; i++) v |= (static_cast<uint64_t>(data_[pos_++]) << (i * 8));
        return v;
    }
    bool getBool() { return getU8() != 0; }
    std::string getBytes() {
        uint32_t n = getU32();
        require(n);
        std::string s(reinterpret_cast<const char*>(data_ + pos_), n);
        pos_ += n;
        return s;
    }
    bool eof() const { return pos_ >= len_; }
    size_t remaining() const { return len_ - pos_; }

private:
    void require(size_t n) const {
        if (pos_ + n > len_) throw std::runtime_error("buffer underrun");
    }
    std::string owned_;
    const uint8_t* data_;
    size_t len_;
    size_t pos_;
};

} // namespace kvraft
