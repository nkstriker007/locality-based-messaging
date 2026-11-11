#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "log_store.h"
#include "locality_messaging.grpc.pb.h"

using namespace locality_messaging;

enum class RaftRole { Follower, Candidate, Leader };

// Raft control plane for the ACL.
//
// v2 changes (performance + correctness):
//   * One long-lived replication thread per peer instead of spawning a
//     detached std::thread per peer on every heartbeat/proposal. Each peer
//     keeps exactly one AppendEntries in flight; everything proposed while it
//     is in flight goes out in the next RPC (batching / group commit).
//   * ProposeCommand() blocks until the entry is *committed and applied*
//     (previously it returned as soon as the leader appended locally).
//   * The Raft log itself is persisted (raft_log_<id>.bin, length-prefixed
//     protobuf records) together with term/vote and an ACL snapshot.
//   * Every RPC has a deadline; a dead peer can no longer pin threads.
//   * Fast log back-off: followers return a conflict index (match_index on
//     failure) so the leader skips back a whole term at once.
//   * Election timeout is randomized once per reset, not every 10 ms tick.
//   * A new leader appends a NOOP so earlier-term entries can commit.
class RaftNode {
public:
    RaftNode(const std::string& my_id, const std::map<std::string, std::string>& peer_addrs);
    ~RaftNode();

    // --- State Machine Hooks (Read Path) ---
    bool isAuthorized(const std::string& user_id, int32_t epoch) const;
    int32_t getCurrentEpoch() const;

    // --- Admin Commands (Write Path) ---
    // Returns true once the command is committed by a majority and applied.
    // Returns false if this node is not the leader (leader_hint is filled
    // with the best-known leader address) or the commit timed out.
    bool ProposeCommand(LogEntry::CommandType type, const std::string& target_user_id,
                        std::string* leader_hint = nullptr, int timeout_ms = 3000);

    // --- Raft RPC Handlers ---
    grpc::Status HandleRequestVote(const RequestVoteArgs* req, RequestVoteReply* resp);
    grpc::Status HandleAppendEntries(const AppendEntriesArgs* req, AppendEntriesReply* resp);
    grpc::Status HandleListUsers(const ListUsersRequest* req, ListUsersResponse* resp);
    grpc::Status HandleGetStatus(const StatusRequest* req, StatusResponse* resp);

    void Shutdown();

private:
    using Clock = std::chrono::steady_clock;

    std::string id_;
    LogStore store_;   // audit log of applied ACL changes
    std::map<std::string, std::string> peer_addrs_;
    std::unordered_map<std::string, std::unique_ptr<ControlPlaneRaft::Stub>> peers_;

    mutable std::mutex mtx_;
    std::condition_variable cv_;          // wakes replicator threads
    std::condition_variable cv_applied_;  // wakes proposers waiting for commit
    std::atomic<bool> dead_{false};
    std::thread ticker_thread_;
    std::vector<std::thread> peer_threads_;
    const Clock::time_point started_ = Clock::now();

    // --- Persistent Raft State ---
    int32_t current_term_{0};
    std::string voted_for_{""};
    std::vector<LogEntry> log_;   // index 0 is a dummy
    int log_fd_{-1};
    bool fsync_{false};

    // --- Volatile Raft State ---
    RaftRole role_{RaftRole::Follower};
    std::string leader_id_;
    Clock::time_point election_deadline_;
    int32_t commit_index_{0};
    int32_t last_applied_{0};
    int votes_{0};

    // --- Volatile Leader State ---
    std::unordered_map<std::string, int32_t> next_index_;
    std::unordered_map<std::string, int32_t> match_index_;
    std::unordered_map<std::string, Clock::time_point> last_sent_;
    std::unordered_map<std::string, int32_t> vote_sent_term_;

    // --- The ACL State Machine ---
    int32_t current_epoch_{1};
    std::set<std::string> authorized_users_;
    Clock::time_point last_snapshot_;
    bool snapshot_dirty_{false};

    static constexpr int kHeartbeatMs = 50;
    static constexpr int kMaxBatch = 1024;

    // --- Internal Helpers (all *Locked() expect mtx_ held) ---
    void connect_peers();
    void load_state();
    void persist_meta_locked() const;      // term / vote / ACL snapshot
    void maybe_snapshot_locked(bool force);
    void append_log_file_locked(const LogEntry& e);
    void rewrite_log_file_locked();
    void ticker_loop();
    void peer_loop(const std::string& peer_id);
    void apply_logs_locked();
    void advance_commit_index_locked();
    void start_election_locked();
    void become_leader_locked();
    void step_down_locked(int32_t new_term);
    void reset_election_timer_locked();
    int  get_quorum() const;
    int32_t last_index() const { return static_cast<int32_t>(log_.size()) - 1; }
};
