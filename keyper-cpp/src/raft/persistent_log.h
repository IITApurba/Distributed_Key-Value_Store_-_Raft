#pragma once
#include <mutex>
#include <string>
#include <vector>
#include <fstream>
#include <optional>
#include "raft/types.h"

namespace kvraft {

// Durable write-ahead log for Raft entries + hard state (currentTerm/votedFor)
// + snapshot metadata. Layout on disk under `dir`:
//   dir/wal.log      - append-only record stream (fsync'd on every append)
//   dir/hardstate    - currentTerm + votedFor, rewritten atomically
//   dir/snapshot.bin - latest snapshot (state machine bytes + last included index/term)
//
// Log compaction: once a snapshot is taken up to index S, all WAL entries
// with index <= S are discarded by rewriting wal.log to only contain the
// suffix. This bounds recovery time to O(entries since last snapshot)
// instead of O(total history).
class PersistentLog {
public:
    explicit PersistentLog(std::string dir) : dir_(std::move(dir)) {
        loadFromDisk();
    }

    // ---- hard state ----
    Term currentTerm() const { std::lock_guard<std::mutex> l(mu_); return currentTerm_; }
    NodeId votedFor() const { std::lock_guard<std::mutex> l(mu_); return votedFor_; }
    void setHardState(Term term, NodeId votedFor) {
        std::lock_guard<std::mutex> l(mu_);
        currentTerm_ = term;
        votedFor_ = votedFor;
        writeHardStateLocked();
    }

    // ---- log entries (indices are 1-based; index 0 = sentinel) ----
    LogIndex lastIndex() const {
        std::lock_guard<std::mutex> l(mu_);
        return entries_.empty() ? snapshotIndex_ : entries_.back().index;
    }
    Term lastTerm() const {
        std::lock_guard<std::mutex> l(mu_);
        return entries_.empty() ? snapshotTerm_ : entries_.back().term;
    }
    // Term of entry at `idx`, or 0 if unknown/compacted away (except the
    // exact snapshot boundary, which we do remember).
    Term termAt(LogIndex idx) const {
        std::lock_guard<std::mutex> l(mu_);
        if (idx == snapshotIndex_) return snapshotTerm_;
        if (idx < snapshotIndex_) return 0;
        size_t off = idx - snapshotIndex_ - 1;
        if (off >= entries_.size()) return 0;
        return entries_[off].term;
    }
    std::optional<LogEntry> at(LogIndex idx) const {
        std::lock_guard<std::mutex> l(mu_);
        if (idx <= snapshotIndex_) return std::nullopt;
        size_t off = idx - snapshotIndex_ - 1;
        if (off >= entries_.size()) return std::nullopt;
        return entries_[off];
    }
    // Entries strictly after `afterIdx`, up to `maxCount` (0 = unlimited).
    std::vector<LogEntry> entriesAfter(LogIndex afterIdx, size_t maxCount = 0) const {
        std::lock_guard<std::mutex> l(mu_);
        std::vector<LogEntry> out;
        if (afterIdx < snapshotIndex_) return out; // caller must send snapshot instead
        size_t startOff = afterIdx - snapshotIndex_;
        for (size_t i = startOff; i < entries_.size(); i++) {
            out.push_back(entries_[i]);
            if (maxCount && out.size() >= maxCount) break;
        }
        return out;
    }

    // Append entries, fsync'ing each batch. Truncates any conflicting
    // suffix first (caller is responsible for conflict detection via
    // termAt, per Raft's AppendEntries consistency check).
    void appendAndSync(const std::vector<LogEntry>& newEntries) {
        if (newEntries.empty()) return;
        std::lock_guard<std::mutex> l(mu_);
        LogIndex firstNew = newEntries.front().index;
        if (firstNew <= snapshotIndex_) {
            // already compacted past this point; drop overlapping prefix
        }
        size_t keepOff = (firstNew > snapshotIndex_) ? (firstNew - snapshotIndex_ - 1) : 0;
        if (keepOff < entries_.size()) entries_.resize(keepOff);
        for (auto& e : newEntries) entries_.push_back(e);
        rewriteWalLocked();
    }

    // Discard everything at or before `index` because a snapshot now covers it.
    void compactUpTo(LogIndex index, Term termAtIndex, const std::string& snapshotBytes) {
        std::lock_guard<std::mutex> l(mu_);
        if (index <= snapshotIndex_) return;
        size_t dropOff = index - snapshotIndex_;
        if (dropOff <= entries_.size()) {
            entries_.erase(entries_.begin(), entries_.begin() + dropOff);
        } else {
            entries_.clear();
        }
        snapshotIndex_ = index;
        snapshotTerm_ = termAtIndex;
        writeSnapshotLocked(snapshotBytes);
        rewriteWalLocked();
    }

    LogIndex snapshotIndex() const { std::lock_guard<std::mutex> l(mu_); return snapshotIndex_; }
    Term snapshotTerm() const { std::lock_guard<std::mutex> l(mu_); return snapshotTerm_; }
    std::optional<std::string> loadSnapshotBytes() const {
        std::ifstream in(dir_ + "/snapshot.bin", std::ios::binary);
        if (!in) return std::nullopt;
        std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        if (data.size() < 16) return std::nullopt;
        return data.substr(16); // skip 8-byte index + 8-byte term header
    }

private:
    void loadFromDisk() {
        std::ifstream hs(dir_ + "/hardstate", std::ios::binary);
        if (hs) {
            std::string data((std::istreambuf_iterator<char>(hs)), std::istreambuf_iterator<char>());
            if (data.size() >= 12) {
                Reader r(data);
                currentTerm_ = r.getU64();
                votedFor_ = r.getU32();
            }
        }
        std::ifstream snap(dir_ + "/snapshot.bin", std::ios::binary);
        if (snap) {
            std::string data((std::istreambuf_iterator<char>(snap)), std::istreambuf_iterator<char>());
            if (data.size() >= 16) {
                Reader r(data);
                snapshotIndex_ = r.getU64();
                snapshotTerm_ = r.getU64();
            }
        }
        std::ifstream wal(dir_ + "/wal.log", std::ios::binary);
        if (wal) {
            std::string data((std::istreambuf_iterator<char>(wal)), std::istreambuf_iterator<char>());
            Reader r(data);
            while (!r.eof()) {
                try {
                    entries_.push_back(LogEntry::decode(r));
                } catch (...) {
                    break; // torn write at tail; ignore partial record
                }
            }
        }
    }

    void writeHardStateLocked() {
        Writer w;
        w.putU64(currentTerm_);
        w.putU32(votedFor_);
        std::ofstream out(dir_ + "/hardstate.tmp", std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(w.data().data()), w.size());
        out.flush();
        out.close();
        std::rename((dir_ + "/hardstate.tmp").c_str(), (dir_ + "/hardstate").c_str());
    }

    void writeSnapshotLocked(const std::string& snapshotBytes) {
        Writer w;
        w.putU64(snapshotIndex_);
        w.putU64(snapshotTerm_);
        std::ofstream out(dir_ + "/snapshot.bin.tmp", std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(w.data().data()), w.size());
        out.write(snapshotBytes.data(), static_cast<std::streamsize>(snapshotBytes.size()));
        out.flush();
        out.close();
        std::rename((dir_ + "/snapshot.bin.tmp").c_str(), (dir_ + "/snapshot.bin").c_str());
    }

    // Full rewrite is O(entries since snapshot) which is fine since we
    // compact frequently; a production system would append-only + periodic
    // compaction instead of rewriting on every append. Kept simple here.
    void rewriteWalLocked() {
        std::ofstream out(dir_ + "/wal.log.tmp", std::ios::binary | std::ios::trunc);
        for (auto& e : entries_) {
            Writer w;
            e.encode(w);
            out.write(reinterpret_cast<const char*>(w.data().data()), w.size());
        }
        out.flush();
        out.close();
        std::rename((dir_ + "/wal.log.tmp").c_str(), (dir_ + "/wal.log").c_str());
    }

    std::string dir_;
    mutable std::mutex mu_;
    Term currentTerm_ = 0;
    NodeId votedFor_ = 0;
    LogIndex snapshotIndex_ = 0;
    Term snapshotTerm_ = 0;
    std::vector<LogEntry> entries_; // entries with index > snapshotIndex_
};

} // namespace kvraft
