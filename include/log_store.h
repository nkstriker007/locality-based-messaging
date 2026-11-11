#pragma once
#include "log_entry.h"
#include "lamport_clock.h"
#include <vector>
#include <optional>
#include <mutex>
#include <string>
#include <unordered_map>

// Append-only CRDT log (grow-only set ordered by (lamport_time, message_id)).
//
// Performance notes (v2):
//   * entries_ is kept sorted at all times; merge() sorts only the *new*
//     entries and splices them in (append fast-path or std::inplace_merge),
//     instead of re-sorting the whole log on every sync.
//   * index_ maps message_id -> lamport_time, giving O(1) dedup and
//     O(log n) read() (binary search on the sorted vector).
//   * The log hash is an order-independent sum of per-entry FNV-1a hashes,
//     maintained incrementally, so get_hash() is O(1) and stable across
//     machines/compilers (std::hash is not).
class LogStore {
public:
    explicit LogStore(const std::string& node_id, int32_t epoch = 0);

    // local write — called by your own client
    ChatEntry append(const std::string& payload);

    // merge incoming entries from a peer (called inside SyncLogs handler).
    // Duplicates are ignored; entries stamped with a *future* epoch
    // (epoch > current_epoch_) are rejected. Returns number of new entries.
    size_t merge(const std::vector<ChatEntry>& incoming);

    std::optional<ChatEntry> read(const std::string& message_id) const;
    std::vector<ChatEntry>   read_range(size_t from, size_t to) const;
    std::vector<ChatEntry>   all() const;
    // entries with lamport_time > since (for delta sync)
    std::vector<ChatEntry>   since(int64_t lamport_time) const;
    uint32_t                 get_hash() const;
    int64_t                  get_lamport_time() const;
    int64_t                  max_entry_lamport() const;
    size_t                   size() const;
    void    set_epoch(int32_t epoch);   // called when the control plane bumps the epoch
    void    raise_epoch(int32_t epoch); // set_epoch, but never goes backwards
    int32_t epoch() const;
    void    apply(const ChatEntry& entry);
    void    tick();

private:
    std::string                              node_id_;
    int32_t                                  current_epoch_;
    std::vector<ChatEntry>                   entries_;   // sorted
    std::unordered_map<std::string, int64_t> index_;     // message_id -> lamport_time
    uint32_t                                 hash_ = 0;
    LamportClock                             clock_;
    mutable std::mutex                       mutex_;

    static uint32_t fnv1a(const std::string& s, uint32_t h = 2166136261u);
    uint32_t computeChecksum(const ChatEntry& e) const;
    void     insertSortedLocked(std::vector<ChatEntry>&& fresh);
};
