#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "locality_messaging.grpc.pb.h"
#include "log_store.h"

class DataPlaneGossipImpl final : public locality_messaging::DataPlaneGossip::Service {
public:
    // auth_cache_ms: how long an authorization decision for (user, epoch) is
    // reused. 0 disables caching (one auth RPC per unique sender per batch).
    // Bounded staleness: a revoked user can still sync for at most this long.
    // Override at runtime with env LBM_AUTH_CACHE_MS.
    DataPlaneGossipImpl(const std::string& node_id,
                        const std::string& auth_address,
                        int auth_cache_ms = 200);

    grpc::Status SyncLogs(
        grpc::ServerContext* ctx,
        const locality_messaging::SyncLogsRequest* req,
        locality_messaging::SyncLogsResponse* resp
    ) override;

    uint64_t auth_rpcs() const { return auth_rpcs_; }

private:
    using Clock = std::chrono::steady_clock;
    struct CachedDecision { bool allowed; Clock::time_point expires; };

    std::string node_id_;
    LogStore store_;
    std::unique_ptr<locality_messaging::IntegrationAuth::Stub> auth_stub_;

    std::chrono::milliseconds auth_ttl_;
    std::mutex cache_mu_;
    std::unordered_map<std::string, CachedDecision> auth_cache_;  // key: user \0 epoch
    std::atomic<uint64_t> auth_rpcs_{0};
    bool verbose_ = false;

    bool checkAuth(const std::string& user_id, int32_t epoch);
    bool checkAuthCached(const std::string& user_id, int32_t epoch);
};
