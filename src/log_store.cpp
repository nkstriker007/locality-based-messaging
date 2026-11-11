#include "log_store.h"
#include <algorithm>

namespace {
inline bool entryLess(const ChatEntry& a, const ChatEntry& b) {
    if (a.lamport_time != b.lamport_time) return a.lamport_time < b.lamport_time;
    return a.message_id < b.message_id;
}

// Per-entry contribution to the log hash. Summing makes the log hash
// order-independent and incrementally maintainable (CRDT-friendly).
inline uint32_t entryHash(const ChatEntry& e) {
    uint32_t h = 2166136261u;
    for (unsigned char c : e.message_id) { h ^= c; h *= 16777619u; }
    h ^= 0x1f; h *= 16777619u;
    for (unsigned char c : e.payload)    { h ^= c; h *= 16777619u; }
    // final avalanche so the sum spreads well
    h ^= h >> 16; h *= 0x85ebca6bu; h ^= h >> 13; h *= 0xc2b2ae35u; h ^= h >> 16;
    return h;
}
}  // namespace

LogStore::LogStore(const std::string& node_id, int32_t epoch)
    : node_id_(node_id), current_epoch_(epoch) {}

// ---------------------------------------------------------------
// Hashing — FNV-1a, stable across platforms (std::hash is not)
// ---------------------------------------------------------------
uint32_t LogStore::fnv1a(const std::string& s, uint32_t h) {
    for (unsigned char c : s) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

// Checksum — payload only (metadata is mutable)
uint32_t LogStore::computeChecksum(const ChatEntry& e) const {
    return fnv1a(e.payload);
}

// ---------------------------------------------------------------
// Insert a batch of *new, deduplicated* entries keeping entries_ sorted.
// Caller holds mutex_.
// ---------------------------------------------------------------
void LogStore::insertSortedLocked(std::vector<ChatEntry>&& fresh) {
    if (fresh.empty()) return;
    std::sort(fresh.begin(), fresh.end(), entryLess);

    const bool append_only = entries_.empty() || !entryLess(fresh.front(), entries_.back());
    const size_t mid = entries_.size();
    entries_.reserve(entries_.size() + fresh.size());
    for (auto& e : fresh) {
        index_.emplace(e.message_id, e.lamport_time);
        hash_ += entryHash(e);
        entries_.push_back(std::move(e));
    }
    if (!append_only) {
        std::inplace_merge(entries_.begin(), entries_.begin() + mid, entries_.end(), entryLess);
    }
}

// ---------------------------------------------------------------
// Append
// ---------------------------------------------------------------
ChatEntry LogStore::append(const std::string& payload) {
    std::lock_guard<std::mutex> lock(mutex_);

    ChatEntry e;
    e.lamport_time = static_cast<int64_t>(clock_.tick());   // was int32 cast (overflow)
    e.sender_id    = node_id_;
    e.message_id   = node_id_ + "_" + std::to_string(e.lamport_time);
    e.payload      = payload;
    e.epoch        = current_epoch_;
    e.checksum     = computeChecksum(e);

    std::vector<ChatEntry> one{e};
    insertSortedLocked(std::move(one));
    return e;
}

void LogStore::tick() {
    clock_.tick();
}

// ---------------------------------------------------------------
// Read single — O(1) index lookup + O(log n) binary search (was O(n))
// ---------------------------------------------------------------
std::optional<ChatEntry> LogStore::read(const std::string& message_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = index_.find(message_id);
    if (it == index_.end()) return std::nullopt;

    ChatEntry key;
    key.lamport_time = it->second;
    key.message_id   = message_id;
    auto pos = std::lower_bound(entries_.begin(), entries_.end(), key, entryLess);
    if (pos != entries_.end() && pos->message_id == message_id) return *pos;
    return std::nullopt;
}

// ---------------------------------------------------------------
// Apply a single entry (used by the control plane's audit log).
// Was O(n) dedup + full O(n log n) sort per call.
// ---------------------------------------------------------------
void LogStore::apply(const ChatEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (index_.count(entry.message_id)) return;

    ChatEntry e = entry;
    e.checksum = computeChecksum(e);
    clock_.update(static_cast<uint64_t>(e.lamport_time));

    std::vector<ChatEntry> one{std::move(e)};
    insertSortedLocked(std::move(one));
}

// ---------------------------------------------------------------
// Read range — by index (0-based, inclusive)
// ---------------------------------------------------------------
std::vector<ChatEntry> LogStore::read_range(size_t from, size_t to) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (entries_.empty() || from >= entries_.size()) return {};
    size_t end = std::min(to, entries_.size() - 1);
    return std::vector<ChatEntry>(entries_.begin() + from, entries_.begin() + end + 1);
}

std::vector<ChatEntry> LogStore::all() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_;
}

std::vector<ChatEntry> LogStore::since(int64_t lamport_time) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::partition_point(entries_.begin(), entries_.end(),
        [&](const ChatEntry& e) { return e.lamport_time <= lamport_time; });
    return std::vector<ChatEntry>(it, entries_.end());
}

// ---------------------------------------------------------------
// Hash — O(1), maintained incrementally (was O(n) per call)
// ---------------------------------------------------------------
uint32_t LogStore::get_hash() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return hash_;
}

size_t LogStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

int64_t LogStore::max_entry_lamport() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.empty() ? 0 : entries_.back().lamport_time;
}

// ---------------------------------------------------------------
// Merge — CRDT set union from a peer
//   O(k log k + n) for k incoming entries (was O(n log n) per call),
//   and O(k) when the batch is newer than everything we hold.
// ---------------------------------------------------------------
size_t LogStore::merge(const std::vector<ChatEntry>& incoming) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<ChatEntry> fresh;
    fresh.reserve(incoming.size());
    uint64_t max_seen = 0;

    for (const auto& remote : incoming) {
        // An epoch the control plane hasn't issued yet is forged/invalid.
        if (remote.epoch > current_epoch_) continue;

        max_seen = std::max<uint64_t>(max_seen, static_cast<uint64_t>(remote.lamport_time));

        // emplace doubles as the dedup check (against the log *and* this batch)
        if (!index_.emplace(remote.message_id, remote.lamport_time).second) continue;

        ChatEntry e = remote;
        e.checksum = computeChecksum(e);   // recompute — don't trust sender
        fresh.push_back(std::move(e));
    }
    if (max_seen) clock_.update(max_seen);

    const size_t added = fresh.size();
    insertSortedLocked(std::move(fresh));
    return added;
}

// ---------------------------------------------------------------
// Epoch
// ---------------------------------------------------------------
void LogStore::set_epoch(int32_t epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    current_epoch_ = epoch;
}

void LogStore::raise_epoch(int32_t epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (epoch > current_epoch_) current_epoch_ = epoch;
}

int32_t LogStore::epoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_epoch_;
}

int64_t LogStore::get_lamport_time() const {
    return static_cast<int64_t>(clock_.get());
}
