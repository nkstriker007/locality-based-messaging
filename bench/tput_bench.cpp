// Throughput benchmark (control plane + data plane).
//
//   ./build/tput_bench raft [nodes=3] [clients=8] [ops_per_client=500]
//   ./build/tput_bench dp   [batch=100] [batches=200] [clients=4]
//
// raft mode: launches an N-node Raft cluster, finds the leader, then C client
// threads each issue K AddUser calls. Throughput is *committed* ops/sec: the
// clock stops only when the leader's applied ACL (ListUsers) contains every
// user, so it measures consensus, not just "leader accepted the request".
//
// dp mode: launches fake_auth_server + log_server, then C client threads push
// SyncLogs batches of B fresh messages. Reports merged messages/sec.
//
// Run from the repo root (it execs ./build/raft_server etc).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

#include <grpcpp/grpcpp.h>
#include "locality_messaging.grpc.pb.h"

using namespace locality_messaging;
using Clock = std::chrono::steady_clock;

static pid_t spawn(const std::vector<std::string>& args) {
    pid_t pid = fork();
    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        dup2(devnull, 1);
        dup2(devnull, 2);
        std::vector<char*> argv;
        for (auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
        argv.push_back(nullptr);
        execv(argv[0], argv.data());
        _exit(127);
    }
    return pid;
}

static void stop(const std::vector<pid_t>& pids) {
    for (auto p : pids) kill(p, SIGKILL);
    for (auto p : pids) waitpid(p, nullptr, 0);
}

static double pct(std::vector<double>& v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    size_t i = std::min(v.size() - 1, (size_t)(p / 100.0 * v.size()));
    return v[i];
}

// ---------------------------------------------------------------- raft mode
static int run_raft(int nodes, int clients, int ops) {
    if (system("rm -f raft_state_bn-*.txt raft_log_bn-*.bin")) {}
    std::vector<std::string> ids, addrs;
    for (int i = 0; i < nodes; i++) {
        ids.push_back("bn-" + std::to_string(i + 1));
        addrs.push_back("127.0.0.1:" + std::to_string(51053 + i));
    }
    std::vector<pid_t> pids;
    for (int i = 0; i < nodes; i++) {
        std::vector<std::string> args = {"./build/raft_server", ids[i], addrs[i]};
        for (int j = 0; j < nodes; j++)
            if (j != i) args.push_back(ids[j] + "=" + addrs[j]);
        pids.push_back(spawn(args));
    }

    std::vector<std::unique_ptr<ControlPlaneRaft::Stub>> stubs;
    for (auto& a : addrs)
        stubs.push_back(ControlPlaneRaft::NewStub(
            grpc::CreateChannel(a, grpc::InsecureChannelCredentials())));

    // find leader (retry up to ~10s)
    int leader = -1;
    auto t_boot = Clock::now();
    for (int attempt = 0; attempt < 100 && leader < 0; attempt++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for (int i = 0; i < nodes && leader < 0; i++) {
            AddUserRequest req; req.set_user_id("__probe__");
            AddUserResponse resp; grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
            if (stubs[i]->AddUser(&ctx, req, &resp).ok() && resp.success()) leader = i;
        }
    }
    if (leader < 0) { std::cerr << "no leader\n"; stop(pids); return 1; }
    double boot_ms = std::chrono::duration<double, std::milli>(Clock::now() - t_boot).count();

    std::vector<std::vector<double>> lat(clients);
    std::atomic<int> failures{0};
    auto t0 = Clock::now();
    std::vector<std::thread> th;
    for (int c = 0; c < clients; c++) {
        th.emplace_back([&, c] {
            // one channel per client so we measure the server, not one HTTP/2 conn
            grpc::ChannelArguments ca;
            ca.SetInt("grpc.channel_id", c);
            auto stub = ControlPlaneRaft::NewStub(grpc::CreateCustomChannel(
                addrs[leader], grpc::InsecureChannelCredentials(), ca));
            lat[c].reserve(ops);
            for (int k = 0; k < ops; k++) {
                AddUserRequest req;
                req.set_user_id("u" + std::to_string(c) + "_" + std::to_string(k));
                AddUserResponse resp; grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                auto a = Clock::now();
                auto st = stub->AddUser(&ctx, req, &resp);
                auto b = Clock::now();
                if (!st.ok() || !resp.success()) failures++;
                lat[c].push_back(std::chrono::duration<double, std::milli>(b - a).count());
            }
        });
    }
    for (auto& t : th) t.join();
    auto t_sent = Clock::now();

    // wait until leader's applied state has all users (committed + applied)
    const size_t want = (size_t)clients * ops;
    size_t have = 0;
    auto deadline = Clock::now() + std::chrono::seconds(60);
    while (Clock::now() < deadline) {
        ListUsersRequest req; ListUsersResponse resp; grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        if (stubs[leader]->ListUsers(&ctx, req, &resp).ok()) {
            have = 0;
            for (auto& u : resp.authorized_users()) if (u[0] == 'u') have++;
            if (have >= want) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    auto t_done = Clock::now();

    std::vector<double> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
    double send_s = std::chrono::duration<double>(t_sent - t0).count();
    double commit_s = std::chrono::duration<double>(t_done - t0).count();
    double avg = std::accumulate(all.begin(), all.end(), 0.0) / all.size();

    printf("RAFT nodes=%d clients=%d ops=%d\n", nodes, clients, clients * ops);
    printf("  leader elected in   %.0f ms\n", boot_ms);
    printf("  committed           %zu / %zu (rpc failures %d)\n", have, want, failures.load());
    printf("  committed ops/sec   %.0f\n", have / commit_s);
    printf("  rpc latency  avg %.2f ms  p50 %.2f  p99 %.2f  max %.2f\n",
           avg, pct(all, 50), pct(all, 99), pct(all, 100));
    printf("  (client send phase %.2fs, until all applied %.2fs)\n", send_s, commit_s);

    stop(pids);
    if (system("rm -f raft_state_bn-*.txt raft_log_bn-*.bin")) {}
    return have >= want ? 0 : 2;
}

// ------------------------------------------------------------------ dp mode
static int run_dp(int batch, int batches, int clients) {
    std::vector<pid_t> pids;
    pids.push_back(spawn({"./build/fake_auth_server", "127.0.0.1:51160"}));
    pids.push_back(spawn({"./build/log_server", "bench-dp", "127.0.0.1:51151", "127.0.0.1:51160"}));
    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    std::vector<std::vector<double>> lat(clients);
    std::atomic<long> merged{0};
    auto t0 = Clock::now();
    std::vector<std::thread> th;
    for (int c = 0; c < clients; c++) {
        th.emplace_back([&, c] {
            grpc::ChannelArguments ca; ca.SetInt("grpc.channel_id", c);
            auto stub = DataPlaneGossip::NewStub(grpc::CreateCustomChannel(
                "127.0.0.1:51151", grpc::InsecureChannelCredentials(), ca));
            long lamport = 1;
            for (int b = 0; b < batches; b++) {
                SyncLogsRequest req;
                req.mutable_sender()->set_node_id("peer-" + std::to_string(c));
                for (int i = 0; i < batch; i++) {
                    auto* m = req.add_logs();
                    m->set_message_id("p" + std::to_string(c) + "_" + std::to_string(lamport));
                    m->set_sender_id("user-" + std::to_string(i % 16));
                    m->set_payload("hello from peer " + std::to_string(c) + " #" + std::to_string(lamport));
                    m->set_lamport_time(lamport++);
                    m->set_epoch(0);
                }
                SyncLogsResponse resp; grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
                auto a = Clock::now();
                auto st = stub->SyncLogs(&ctx, req, &resp);
                auto e = Clock::now();
                if (st.ok() && resp.success()) merged += batch;
                lat[c].push_back(std::chrono::duration<double, std::milli>(e - a).count());
            }
        });
    }
    for (auto& t : th) t.join();
    double s = std::chrono::duration<double>(Clock::now() - t0).count();
    std::vector<double> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());

    printf("DATA PLANE batch=%d batches=%d clients=%d\n", batch, batches, clients);
    printf("  merged msgs/sec     %.0f  (%ld msgs in %.2fs)\n", merged / s, merged.load(), s);
    printf("  SyncLogs latency  p50 %.2f ms  p99 %.2f ms\n", pct(all, 50), pct(all, 99));
    stop(pids);
    return 0;
}

int main(int argc, char** argv) {
    std::string mode = argc > 1 ? argv[1] : "raft";
    auto arg = [&](int i, int d) { return argc > i ? atoi(argv[i]) : d; };
    if (mode == "raft") return run_raft(arg(2, 3), arg(3, 8), arg(4, 500));
    if (mode == "dp")   return run_dp(arg(2, 100), arg(3, 200), arg(4, 4));
    std::cerr << "usage: tput_bench raft [nodes clients ops] | dp [batch batches clients]\n";
    return 1;
}
