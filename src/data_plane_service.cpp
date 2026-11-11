#include "data_plane_service.h"
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <cstdlib>
#include <iostream>

using namespace locality_messaging;

// ----------------------------
// Helpers
// ----------------------------
ChatEntry fromProto(const ChatMessage& m) {
    ChatEntry e;
    e.message_id   = m.message_id();
    e.sender_id    = m.sender_id();
    e.payload      = m.payload();
    e.lamport_time = m.lamport_time();
    e.epoch        = m.epoch();
    e.checksum     = 0;
    return e;
}

// O(1): no longer copies the whole log just to count it.
static LogSummary BuildSummary(const std::string& node_id, const LogStore& store) {
    LogSummary summary;
    summary.set_node_id(node_id);
    summary.set_entry_count(static_cast<int32_t>(store.size()));
    summary.set_log_hash(store.get_hash());
    summary.set_max_lamport_time(store.max_entry_lamport());
    return summary;
}

// ----------------------------
// Constructor
// ----------------------------
DataPlaneGossipImpl::DataPlaneGossipImpl(
    const std::string& node_id,
    const std::string& auth_address,
    int auth_cache_ms)
    : node_id_(node_id), store_(node_id), auth_ttl_(auth_cache_ms)
{
    if (const char* env = std::getenv("LBM_AUTH_CACHE_MS"))
        auth_ttl_ = std::chrono::milliseconds(std::atoi(env));
    verbose_ = std::getenv("LBM_VERBOSE") != nullptr;

    grpc::ChannelArguments args;   // reconnect quickly after the auth node restarts
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 100);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 500);
    auth_stub_ = IntegrationAuth::NewStub(
        grpc::CreateCustomChannel(auth_address, grpc::InsecureChannelCredentials(), args));

    std::cerr << "DataPlane node [" << node_id << "] auth backend at " << auth_address
              << " (auth cache " << auth_ttl_.count() << " ms)" << std::endl;
}

// ----------------------------
// SyncLogs implementation
// ----------------------------
grpc::Status DataPlaneGossipImpl::SyncLogs(
    grpc::ServerContext* ctx,
    const SyncLogsRequest* req,
    SyncLogsResponse* resp)
{
    (void)ctx;

    std::vector<ChatEntry> incoming;
    incoming.reserve(req->logs_size());

    // One authorization decision per distinct (sender, epoch) in this batch,
    // instead of one blocking RPC per message.
    std::unordered_map<std::string, bool> batch_decisions;
    int accepted = 0;
    int rejected = 0;
    int32_t max_ok_epoch = 0;

    for (const auto& msg : req->logs()) {
        std::string key = msg.sender_id();
        key.push_back('\0');
        key += std::to_string(msg.epoch());

        auto it = batch_decisions.find(key);
        bool ok;
        if (it != batch_decisions.end()) {
            ok = it->second;
        } else {
            ok = checkAuthCached(msg.sender_id(), msg.epoch());
            batch_decisions.emplace(std::move(key), ok);
        }

        if (!ok) {
            rejected++;
            if (verbose_)
                std::cerr << "Rejected message from " << msg.sender_id()
                          << " (epoch=" << msg.epoch() << ")" << std::endl;
            continue;
        }
        if (msg.epoch() > max_ok_epoch) max_ok_epoch = msg.epoch();
        incoming.push_back(fromProto(msg));
        accepted++;
    }

    // The control plane vouched for these epochs, so the store may advance to
    // them (otherwise the store's epoch would stay 0 forever and drop them).
    if (max_ok_epoch > 0) store_.raise_epoch(max_ok_epoch);

    store_.tick();
    if (!incoming.empty()) store_.merge(incoming);

    resp->set_success(true);
    resp->set_receiver_lamport_time(store_.get_lamport_time());
    *resp->mutable_summary() = BuildSummary(node_id_, store_);

    if (verbose_ || rejected > 0) {
        std::cerr << "SyncLogs from " << req->sender().node_id()
                  << " accepted=" << accepted << " rejected=" << rejected
                  << " local_lamport=" << resp->receiver_lamport_time() << std::endl;
    }
    return grpc::Status::OK;
}

// ----------------------------
// Auth helpers
// ----------------------------
bool DataPlaneGossipImpl::checkAuthCached(const std::string& user_id, int32_t epoch) {
    if (auth_ttl_.count() <= 0) return checkAuth(user_id, epoch);

    std::string key = user_id;
    key.push_back('\0');
    key += std::to_string(epoch);
    const auto now = Clock::now();
    {
        std::lock_guard<std::mutex> lk(cache_mu_);
        auto it = auth_cache_.find(key);
        if (it != auth_cache_.end() && it->second.expires > now) return it->second.allowed;
    }
    bool ok = checkAuth(user_id, epoch);
    {
        std::lock_guard<std::mutex> lk(cache_mu_);
        if (auth_cache_.size() > 100000) auth_cache_.clear();  // crude bound
        auth_cache_[key] = CachedDecision{ok, now + auth_ttl_};
    }
    return ok;
}

bool DataPlaneGossipImpl::checkAuth(const std::string& user_id, int32_t epoch) {
    AuthCheckRequest req;
    req.set_user_id(user_id);
    req.set_epoch(epoch);

    AuthCheckResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
    auth_rpcs_++;

    auto status = auth_stub_->CheckAuthorization(&ctx, req, &resp);
    if (!status.ok()) return false;   // fail closed
    return resp.is_authorized();
}
