#include "raft_node.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>

#include <fcntl.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

namespace {
std::string state_path(const std::string& id) { return "raft_state_" + id + ".txt"; }
std::string log_path(const std::string& id)   { return "raft_log_" + id + ".bin"; }

std::chrono::system_clock::time_point deadline_in(int ms) {
    return std::chrono::system_clock::now() + std::chrono::milliseconds(ms);
}

bool write_all(int fd, const char* p, size_t n) {
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w <= 0) return false;
        p += w;
        n -= static_cast<size_t>(w);
    }
    return true;
}

const char* role_name(RaftRole r) {
    switch (r) {
        case RaftRole::Leader:    return "leader";
        case RaftRole::Candidate: return "candidate";
        default:                  return "follower";
    }
}
}  // namespace

// ============================================================================
// Construction / teardown
// ============================================================================
RaftNode::RaftNode(const std::string& my_id, const std::map<std::string, std::string>& peer_addrs)
    : id_(my_id), store_(my_id), peer_addrs_(peer_addrs)
{
    fsync_ = std::getenv("RAFT_FSYNC") != nullptr;

    LogEntry dummy;           // index 0: term 0, makes 1-based indexing safe
    dummy.set_term(0);
    log_.push_back(dummy);

    load_state();
    log_fd_ = ::open(log_path(id_).c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd_ < 0) std::cerr << "[Raft] cannot open " << log_path(id_) << "\n";

    connect_peers();

    {
        std::lock_guard<std::mutex> lk(mtx_);
        reset_election_timer_locked();
        last_snapshot_ = Clock::now();
    }

    ticker_thread_ = std::thread(&RaftNode::ticker_loop, this);
    for (const auto& kv : peer_addrs_)
        peer_threads_.emplace_back(&RaftNode::peer_loop, this, kv.first);
}

RaftNode::~RaftNode() { Shutdown(); }

void RaftNode::Shutdown() {
    if (dead_.exchange(true)) return;
    cv_.notify_all();
    cv_applied_.notify_all();
    if (ticker_thread_.joinable()) ticker_thread_.join();
    for (auto& t : peer_threads_) if (t.joinable()) t.join();
    std::lock_guard<std::mutex> lk(mtx_);
    maybe_snapshot_locked(true);
    if (log_fd_ >= 0) { ::close(log_fd_); log_fd_ = -1; }
}

void RaftNode::connect_peers() {
    // Cap gRPC's reconnect back-off (default grows to 120 s). Without this a
    // restarted peer can't hear heartbeats for seconds, times out, and forces
    // a needless election that bumps the term.
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 50);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 250);
    for (const auto& kv : peer_addrs_) {
        auto channel = grpc::CreateCustomChannel(kv.second, grpc::InsecureChannelCredentials(), args);
        peers_[kv.first] = ControlPlaneRaft::NewStub(channel);
    }
}

// ============================================================================
// Persistence
//   raft_state_<id>.txt : "v2", term, voted_for, epoch, last_applied, users...
//                         (written atomically via tmp + rename)
//   raft_log_<id>.bin   : [uint32 len][LogEntry protobuf] records
// The ACL snapshot is only an optimisation: entries after last_applied are
// in the log and get re-applied once they are (re)committed.
// ============================================================================
void RaftNode::load_state() {
    std::ifstream input(state_path(id_));
    if (!input.is_open()) {
        // fresh node: discard any stale log from a previous run
        std::remove(log_path(id_).c_str());
        return;
    }

    std::string first;
    if (!std::getline(input, first)) return;

    int32_t term = 0, epoch = 1, applied = 0;
    std::string voted;
    if (first == "v2") {
        input >> term >> std::ws;
        std::getline(input, voted);
        input >> epoch >> applied;
    } else {                       // legacy v1 format: term, voted, epoch, users
        term = std::atoi(first.c_str());
        input >> std::ws;
        std::getline(input, voted);
        input >> epoch;
    }
    std::set<std::string> users;
    std::string u;
    while (std::getline(input >> std::ws, u))
        if (!u.empty()) users.insert(u);

    current_term_   = term;
    voted_for_      = (voted == "-") ? "" : voted;
    current_epoch_  = epoch;
    authorized_users_ = std::move(users);

    // replay the persisted Raft log
    std::ifstream lf(log_path(id_), std::ios::binary);
    size_t good_bytes = 0;
    while (lf) {
        uint32_t len = 0;
        if (!lf.read(reinterpret_cast<char*>(&len), sizeof(len))) break;
        std::string buf(len, '\0');
        if (!lf.read(&buf[0], len)) break;
        LogEntry e;
        if (!e.ParseFromString(buf)) break;
        log_.push_back(std::move(e));
        good_bytes += sizeof(len) + len;
    }
    lf.close();
    if (::truncate(log_path(id_).c_str(), static_cast<off_t>(good_bytes)) != 0) {
        // file may not exist yet — fine
    }

    if (applied > last_index()) {
        // snapshot is ahead of the surviving log (log lost): keep the ACL,
        // but we can't claim any log prefix as committed.
        applied = 0;
        log_.resize(1);
        std::remove(log_path(id_).c_str());
    }
    last_applied_ = applied;
    commit_index_ = applied;

    std::cout << "[Raft] " << id_ << " restored term=" << current_term_
              << " epoch=" << current_epoch_ << " users=" << authorized_users_.size()
              << " log=" << last_index() << " applied=" << last_applied_ << "\n";
}

void RaftNode::persist_meta_locked() const {
    const std::string path = state_path(id_);
    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        if (!out.is_open()) {
            std::cerr << "[Raft] Failed to persist state for node " << id_ << "\n";
            return;
        }
        out << "v2\n"
            << current_term_ << '\n'
            << (voted_for_.empty() ? "-" : voted_for_) << '\n'
            << current_epoch_ << '\n'
            << last_applied_ << '\n';
        for (const auto& user : authorized_users_) out << user << '\n';
    }
    std::rename(tmp.c_str(), path.c_str());
}

void RaftNode::maybe_snapshot_locked(bool force) {
    if (!snapshot_dirty_) return;
    auto now = Clock::now();
    if (!force && now - last_snapshot_ < std::chrono::milliseconds(50)) return;
    persist_meta_locked();
    snapshot_dirty_ = false;
    last_snapshot_ = now;
}

void RaftNode::append_log_file_locked(const LogEntry& e) {
    if (log_fd_ < 0) return;
    std::string buf;
    e.SerializeToString(&buf);
    uint32_t len = static_cast<uint32_t>(buf.size());
    std::string rec(reinterpret_cast<const char*>(&len), sizeof(len));
    rec += buf;
    write_all(log_fd_, rec.data(), rec.size());
    if (fsync_) ::fdatasync(log_fd_);
}

void RaftNode::rewrite_log_file_locked() {
    const std::string path = log_path(id_);
    const std::string tmp = path + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    for (size_t i = 1; i < log_.size(); i++) {
        std::string buf;
        log_[i].SerializeToString(&buf);
        uint32_t len = static_cast<uint32_t>(buf.size());
        write_all(fd, reinterpret_cast<const char*>(&len), sizeof(len));
        write_all(fd, buf.data(), buf.size());
    }
    if (fsync_) ::fdatasync(fd);
    ::close(fd);
    std::rename(tmp.c_str(), path.c_str());
    if (log_fd_ >= 0) ::close(log_fd_);
    log_fd_ = ::open(path.c_str(), O_WRONLY | O_APPEND, 0644);
}

// ============================================================================
// Timers
// ============================================================================
void RaftNode::reset_election_timer_locked() {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(300, 600);
    election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng));
}

int RaftNode::get_quorum() const {
    return static_cast<int>((peers_.size() + 1) / 2) + 1;
}

void RaftNode::ticker_loop() {
    while (!dead_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::lock_guard<std::mutex> lk(mtx_);
        if (role_ != RaftRole::Leader && Clock::now() >= election_deadline_)
            start_election_locked();
        maybe_snapshot_locked(false);
    }
}

// ============================================================================
// Role transitions
// ============================================================================
void RaftNode::step_down_locked(int32_t new_term) {
    if (new_term > current_term_) {
        current_term_ = new_term;
        voted_for_.clear();
        persist_meta_locked();
    }
    if (role_ == RaftRole::Leader) leader_id_.clear();
    role_ = RaftRole::Follower;
    votes_ = 0;
    cv_applied_.notify_all();   // pending proposals on this node must give up
}

void RaftNode::start_election_locked() {
    role_ = RaftRole::Candidate;
    current_term_++;
    voted_for_ = id_;
    votes_ = 1;
    leader_id_.clear();
    persist_meta_locked();
    reset_election_timer_locked();
    if (votes_ >= get_quorum()) become_leader_locked();
    cv_.notify_all();           // peer threads send RequestVote
}

void RaftNode::become_leader_locked() {
    role_ = RaftRole::Leader;
    leader_id_ = id_;
    std::cout << "Won Election! Becoming Leader for Term " << current_term_ << std::endl;

    const int32_t next = last_index() + 1;
    for (const auto& p : peers_) {
        next_index_[p.first] = next;
        match_index_[p.first] = 0;
        last_sent_[p.first] = Clock::time_point{};   // heartbeat immediately
    }

    // NOOP lets entries from earlier terms commit (Raft §5.4.2)
    LogEntry noop;
    noop.set_term(current_term_);
    noop.set_type(LogEntry::NOOP);
    log_.push_back(noop);
    append_log_file_locked(noop);
    match_index_[id_] = last_index();
    advance_commit_index_locked();
    cv_.notify_all();
}

// ============================================================================
// Replication — one long-lived thread per peer
// ============================================================================
void RaftNode::peer_loop(const std::string& peer_id) {
    ControlPlaneRaft::Stub& stub = *peers_.at(peer_id);
    const auto hb = std::chrono::milliseconds(kHeartbeatMs);
    std::unique_lock<std::mutex> lk(mtx_);

    while (!dead_.load()) {
        // ---------------- candidate: ask for a vote once per term ----------
        if (role_ == RaftRole::Candidate && vote_sent_term_[peer_id] != current_term_) {
            const int32_t term = current_term_;
            vote_sent_term_[peer_id] = term;
            RequestVoteArgs args;
            args.set_term(term);
            args.set_candidate_id(id_);
            args.set_last_log_index(last_index());
            args.set_last_log_term(log_.back().term());

            lk.unlock();
            RequestVoteReply reply;
            grpc::ClientContext ctx;
            ctx.set_deadline(deadline_in(150));
            grpc::Status st = stub.RequestVote(&ctx, args, &reply);
            lk.lock();

            if (!st.ok()) {
                // retry later in the same term if the peer was unreachable
                cv_.wait_for(lk, std::chrono::milliseconds(30));
                if (current_term_ == term && role_ == RaftRole::Candidate)
                    vote_sent_term_[peer_id] = -1;
                continue;
            }
            if (reply.term() > current_term_) {
                step_down_locked(reply.term());
            } else if (role_ == RaftRole::Candidate && term == current_term_ &&
                       reply.vote_granted()) {
                if (++votes_ >= get_quorum()) become_leader_locked();
            }
            continue;
        }

        // ---------------- leader: replicate / heartbeat ---------------------
        if (role_ == RaftRole::Leader) {
            const auto now = Clock::now();
            const int32_t ni = next_index_[peer_id];
            const bool has_entries = ni <= last_index();

            if (has_entries || now - last_sent_[peer_id] >= hb) {
                const int32_t term = current_term_;
                const int32_t prev = ni - 1;
                AppendEntriesArgs args;
                args.set_term(term);
                args.set_leader_id(id_);
                args.set_prev_log_index(prev);
                args.set_prev_log_term(log_[prev].term());
                args.set_leader_commit(commit_index_);
                const int32_t end = std::min<int32_t>(last_index(), ni + kMaxBatch - 1);
                for (int32_t i = ni; i <= end; i++) *args.add_entries() = log_[i];
                last_sent_[peer_id] = now;

                lk.unlock();
                AppendEntriesReply reply;
                grpc::ClientContext ctx;
                ctx.set_deadline(deadline_in(args.entries_size() > 0 ? 1000 : 250));
                grpc::Status st = stub.AppendEntries(&ctx, args, &reply);
                lk.lock();

                if (!st.ok()) {
                    // peer down/slow: back off until the next heartbeat slot
                    last_sent_[peer_id] = Clock::now();
                    cv_.wait_until(lk, last_sent_[peer_id] + hb,
                                   [&] { return dead_.load() || role_ != RaftRole::Leader; });
                    continue;
                }
                if (role_ != RaftRole::Leader || current_term_ != term) continue;

                if (reply.term() > current_term_) {
                    step_down_locked(reply.term());
                } else if (reply.success()) {
                    const int32_t new_match = prev + args.entries_size();
                    if (new_match > match_index_[peer_id]) {
                        match_index_[peer_id] = new_match;
                        advance_commit_index_locked();
                    }
                    next_index_[peer_id] = std::max(next_index_[peer_id], new_match + 1);
                } else {
                    // fast back-off: follower's conflict hint in match_index
                    int32_t hint = reply.match_index();
                    if (hint <= 0 || hint >= ni) hint = ni - 1;
                    next_index_[peer_id] = std::max<int32_t>(1, hint);
                }
                continue;
            }

            cv_.wait_until(lk, last_sent_[peer_id] + hb, [&] {
                return dead_.load() || role_ != RaftRole::Leader ||
                       next_index_[peer_id] <= last_index();
            });
            continue;
        }

        // ---------------- follower / candidate that already asked ----------
        cv_.wait_for(lk, std::chrono::milliseconds(kHeartbeatMs));
    }
}

void RaftNode::advance_commit_index_locked() {
    for (int32_t n = last_index(); n > commit_index_; n--) {
        if (log_[n].term() < current_term_) break;   // terms are non-decreasing
        if (log_[n].term() != current_term_) continue;

        int replicas = 1;   // self
        for (const auto& peer : peers_)
            if (match_index_[peer.first] >= n) replicas++;

        if (replicas >= get_quorum()) {
            commit_index_ = n;
            apply_logs_locked();
            break;
        }
    }
}

// ============================================================================
// State machine
// ============================================================================
void RaftNode::apply_logs_locked() {
    bool changed = false;
    while (commit_index_ > last_applied_) {
        last_applied_++;
        const LogEntry& entry = log_[last_applied_];
        changed = true;

        if (entry.type() == LogEntry::NOOP) continue;

        if (entry.type() == LogEntry::ADD_USER) {
            authorized_users_.insert(entry.target_user_id());
            std::cout << "[ACL] Added user: " << entry.target_user_id() << "\n";
        } else if (entry.type() == LogEntry::REVOKE_USER) {
            authorized_users_.erase(entry.target_user_id());
            current_epoch_++;   // issue the new certificate epoch
            std::cout << "[ACL] Revoked user: " << entry.target_user_id()
                      << ". New Epoch: " << current_epoch_ << "\n";
        }

        ChatEntry ce;   // audit trail in the node's LogStore
        ce.sender_id    = id_;
        ce.payload      = entry.target_user_id();
        ce.epoch        = current_epoch_;
        ce.lamport_time = store_.get_lamport_time() + 1;
        ce.message_id   = ce.sender_id + "_" + std::to_string(ce.lamport_time);
        store_.apply(ce);
    }
    if (changed) {
        snapshot_dirty_ = true;
        maybe_snapshot_locked(false);
        cv_applied_.notify_all();
    }
}

bool RaftNode::isAuthorized(const std::string& user_id, int32_t epoch) const {
    std::lock_guard<std::mutex> lock(mtx_);
    return (authorized_users_.count(user_id) > 0) && (epoch <= current_epoch_);
}

int32_t RaftNode::getCurrentEpoch() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return current_epoch_;
}

// ============================================================================
// Write path
// ============================================================================
bool RaftNode::ProposeCommand(LogEntry::CommandType type, const std::string& target_user_id,
                              std::string* leader_hint, int timeout_ms) {
    std::unique_lock<std::mutex> lk(mtx_);
    if (role_ != RaftRole::Leader) {
        if (leader_hint && !leader_id_.empty()) {
            auto it = peer_addrs_.find(leader_id_);
            if (it != peer_addrs_.end()) *leader_hint = it->second;
        }
        return false;
    }

    LogEntry e;
    e.set_term(current_term_);
    e.set_type(type);
    e.set_target_user_id(target_user_id);
    log_.push_back(e);
    append_log_file_locked(e);

    const int32_t idx  = last_index();
    const int32_t term = current_term_;
    match_index_[id_] = idx;
    if (peers_.empty()) advance_commit_index_locked();
    cv_.notify_all();   // wake replicators: they batch whatever is pending

    cv_applied_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
        return dead_.load() || last_applied_ >= idx ||
               role_ != RaftRole::Leader || current_term_ != term;
    });
    return last_applied_ >= idx && idx <= last_index() && log_[idx].term() == term;
}

// ============================================================================
// RPC handlers
// ============================================================================
grpc::Status RaftNode::HandleRequestVote(const RequestVoteArgs* req, RequestVoteReply* resp) {
    std::lock_guard<std::mutex> lock(mtx_);

    if (req->term() > current_term_) step_down_locked(req->term());

    if (req->term() < current_term_) {
        resp->set_term(current_term_);
        resp->set_vote_granted(false);
        return grpc::Status::OK;
    }

    const int32_t my_last_idx  = last_index();
    const int32_t my_last_term = log_[my_last_idx].term();
    const bool up_to_date =
        req->last_log_term() > my_last_term ||
        (req->last_log_term() == my_last_term && req->last_log_index() >= my_last_idx);
    const bool can_vote = voted_for_.empty() || voted_for_ == req->candidate_id();

    if (can_vote && up_to_date) {
        voted_for_ = req->candidate_id();
        role_ = RaftRole::Follower;
        reset_election_timer_locked();
        persist_meta_locked();
        resp->set_vote_granted(true);
    } else {
        resp->set_vote_granted(false);
    }
    resp->set_term(current_term_);
    return grpc::Status::OK;
}

grpc::Status RaftNode::HandleAppendEntries(const AppendEntriesArgs* req, AppendEntriesReply* resp) {
    std::lock_guard<std::mutex> lock(mtx_);

    if (req->term() > current_term_) step_down_locked(req->term());

    resp->set_term(current_term_);
    if (req->term() < current_term_) {
        resp->set_success(false);
        resp->set_match_index(0);
        return grpc::Status::OK;
    }

    if (role_ != RaftRole::Follower) step_down_locked(req->term());
    leader_id_ = req->leader_id();
    reset_election_timer_locked();

    const int32_t prev = req->prev_log_index();
    if (prev > last_index()) {
        resp->set_success(false);
        resp->set_match_index(last_index() + 1);   // hint: send from here
        return grpc::Status::OK;
    }
    if (log_[prev].term() != req->prev_log_term()) {
        // hint: first index of the conflicting term
        const int32_t bad_term = log_[prev].term();
        int32_t i = prev;
        while (i > 1 && log_[i - 1].term() == bad_term) i--;
        resp->set_success(false);
        resp->set_match_index(i);
        return grpc::Status::OK;
    }

    bool truncated = false;
    int32_t appended = 0;
    int32_t insert_idx = prev + 1;
    for (int i = 0; i < req->entries_size(); i++, insert_idx++) {
        const LogEntry& incoming = req->entries(i);
        if (insert_idx <= last_index()) {
            if (log_[insert_idx].term() == incoming.term()) continue;   // already have it
            log_.erase(log_.begin() + insert_idx, log_.end());
            truncated = true;
        }
        log_.push_back(incoming);
        appended++;
    }
    if (truncated) {
        rewrite_log_file_locked();
    } else {
        for (int32_t i = last_index() - appended + 1; i <= last_index(); i++)
            append_log_file_locked(log_[i]);
    }

    if (req->leader_commit() > commit_index_) {
        commit_index_ = std::min<int32_t>(req->leader_commit(), last_index());
        apply_logs_locked();
    }

    resp->set_success(true);
    resp->set_match_index(prev + req->entries_size());
    return grpc::Status::OK;
}

grpc::Status RaftNode::HandleListUsers(const ListUsersRequest*, ListUsersResponse* resp) {
    std::lock_guard<std::mutex> lock(mtx_);
    for (const auto& user : authorized_users_) resp->add_authorized_users(user);
    resp->set_current_epoch(current_epoch_);
    return grpc::Status::OK;
}

grpc::Status RaftNode::HandleGetStatus(const StatusRequest*, StatusResponse* resp) {
    std::lock_guard<std::mutex> lock(mtx_);
    resp->set_node_id(id_);
    resp->set_role(role_name(role_));
    resp->set_term(current_term_);
    resp->set_leader_id(leader_id_);
    resp->set_commit_index(commit_index_);
    resp->set_last_applied(last_applied_);
    resp->set_log_size(last_index());
    resp->set_current_epoch(current_epoch_);
    resp->set_user_count(static_cast<int32_t>(authorized_users_.size()));
    for (const auto& kv : peer_addrs_) resp->add_peers(kv.first + "=" + kv.second);
    resp->set_uptime_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - started_).count());
    return grpc::Status::OK;
}
