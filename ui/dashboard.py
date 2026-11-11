#!/usr/bin/env python3
"""
Live cluster dashboard for the Locality-Based Messaging system.

    pip install grpcio grpcio-tools fastapi uvicorn
    python3 ui/dashboard.py --nodes 3          # run from the repo root, after building
    open http://localhost:8080

It launches N Raft control-plane nodes (raft_server) and N data-plane nodes
(log_server, each authorising through one Raft node), then lets you:
  * watch role / term / commit index / epoch of every node live
  * kill, restart, pause (simulated partition) and resume any node
  * add / revoke users through the leader (follows leader_hint redirects)
  * send chat messages to one or all data-plane nodes and watch the CRDT
    logs diverge and converge (entry count + log hash per node)
  * run the throughput benchmark and see the numbers
"""
import argparse
import atexit
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UI_DIR = Path(__file__).resolve().parent
GEN_DIR = UI_DIR / "_gen"
RUN_DIR = UI_DIR / "_run"


# --------------------------------------------------------------------------
# protobuf stubs (generated on first run)
# --------------------------------------------------------------------------
def load_stubs():
    GEN_DIR.mkdir(exist_ok=True)
    proto = ROOT / "proto" / "locality_messaging.proto"
    out = GEN_DIR / "locality_messaging_pb2.py"
    if not out.exists() or out.stat().st_mtime < proto.stat().st_mtime:
        from grpc_tools import protoc
        rc = protoc.main(["protoc", f"-I{proto.parent}", f"--python_out={GEN_DIR}",
                          f"--grpc_python_out={GEN_DIR}", str(proto)])
        if rc != 0:
            sys.exit("protoc failed")
    sys.path.insert(0, str(GEN_DIR))
    import locality_messaging_pb2 as pb
    import locality_messaging_pb2_grpc as pbg
    return pb, pbg


pb, pbg = load_stubs()
import grpc  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.responses import FileResponse  # noqa: E402
from pydantic import BaseModel  # noqa: E402


# --------------------------------------------------------------------------
# cluster process management
# --------------------------------------------------------------------------
class Proc:
    def __init__(self, name, args, log):
        self.name, self.args, self.log = name, args, log
        self.p = None
        self.paused = False

    def start(self):
        if self.running():
            return
        self.p = subprocess.Popen(self.args, cwd=RUN_DIR, stdout=open(self.log, "ab"),
                                  stderr=subprocess.STDOUT, start_new_session=True)
        self.paused = False

    def running(self):
        return self.p is not None and self.p.poll() is None

    def kill(self):
        if self.running():
            if self.paused:
                self.p.send_signal(signal.SIGCONT)
            self.p.kill()
            self.p.wait()
        self.paused = False

    def pause(self):
        if self.running() and not self.paused:
            self.p.send_signal(signal.SIGSTOP)
            self.paused = True

    def resume(self):
        if self.running() and self.paused:
            self.p.send_signal(signal.SIGCONT)
            self.paused = False

    def state(self):
        if not self.running():
            return "stopped"
        return "paused" if self.paused else "running"


class Cluster:
    def __init__(self, n, raft_base=50053, dp_base=50151):
        RUN_DIR.mkdir(exist_ok=True)
        for f in RUN_DIR.glob("raft_*"):
            f.unlink()
        build = ROOT / "build"
        self.ids = [f"auth-{i + 1}" for i in range(n)]
        self.addrs = {nid: f"127.0.0.1:{raft_base + i}" for i, nid in enumerate(self.ids)}
        self.raft = {}
        for nid in self.ids:
            peers = [f"{p}={self.addrs[p]}" for p in self.ids if p != nid]
            self.raft[nid] = Proc(nid, [str(build / "raft_server"), nid, self.addrs[nid], *peers],
                                  RUN_DIR / f"{nid}.log")
        self.dp_ids = [f"dp-{i + 1}" for i in range(n)]
        self.dp_addrs = {d: f"127.0.0.1:{dp_base + i}" for i, d in enumerate(self.dp_ids)}
        self.dp = {}
        for i, d in enumerate(self.dp_ids):
            auth = self.addrs[self.ids[i]]
            self.dp[d] = Proc(d, [str(build / "log_server"), d, self.dp_addrs[d], auth],
                              RUN_DIR / f"{d}.log")

        self.raft_stub = {nid: pbg.ControlPlaneRaftStub(grpc.insecure_channel(a))
                          for nid, a in self.addrs.items()}
        self.auth_stub = {nid: pbg.IntegrationAuthStub(grpc.insecure_channel(a))
                          for nid, a in self.addrs.items()}
        self.dp_stub = {d: pbg.DataPlaneGossipStub(grpc.insecure_channel(a))
                        for d, a in self.dp_addrs.items()}

        self.lock = threading.Lock()
        self.status = {}          # nid -> dict
        self.dp_status = {}       # dp id -> dict
        self.users, self.epoch = [], 0
        self.events = []
        self.leader = None
        self.leader_lost_at = None
        self.messages = []        # every chat message ever sent (for anti-entropy)
        self.lamport = 0
        self.bench = {"running": False, "output": ""}

        for p in list(self.raft.values()) + list(self.dp.values()):
            p.start()
        self.event(f"cluster started: {n} Raft nodes + {n} data-plane nodes")
        threading.Thread(target=self.poll_loop, daemon=True).start()

    # ---------------------------------------------------------------- utils
    def event(self, text, kind="info"):
        with self.lock:
            self.events.append({"t": time.time(), "text": text, "kind": kind})
            self.events = self.events[-200:]

    def shutdown(self):
        for p in list(self.raft.values()) + list(self.dp.values()):
            try:
                p.kill()
            except Exception:
                pass

    # ---------------------------------------------------------------- polling
    def poll_loop(self):
        while True:
            statuses = {}
            for nid in self.ids:
                proc = self.raft[nid]
                if proc.state() != "running":
                    statuses[nid] = None
                    continue
                try:
                    r = self.raft_stub[nid].GetStatus(pb.StatusRequest(), timeout=0.25)
                    statuses[nid] = {
                        "role": r.role, "term": r.term, "leader_id": r.leader_id,
                        "commit_index": r.commit_index, "last_applied": r.last_applied,
                        "log_size": r.log_size, "epoch": r.current_epoch,
                        "user_count": r.user_count, "uptime_ms": r.uptime_ms,
                    }
                except grpc.RpcError:
                    statuses[nid] = None

            leaders = [n for n, s in statuses.items() if s and s["role"] == "leader"]
            # highest term wins if a stale leader is still around (e.g. after resume)
            leader = max(leaders, key=lambda n: statuses[n]["term"]) if leaders else None

            users, epoch = self.users, self.epoch
            src = leader or next((n for n, s in statuses.items() if s), None)
            if src:
                try:
                    lu = self.raft_stub[src].ListUsers(pb.ListUsersRequest(), timeout=0.5)
                    users, epoch = sorted(lu.authorized_users), lu.current_epoch
                except grpc.RpcError:
                    pass

            dps = {}
            for d in self.dp_ids:
                if self.dp[d].state() != "running":
                    dps[d] = None
                    continue
                try:
                    r = self.dp_stub[d].SyncLogs(
                        pb.SyncLogsRequest(sender=pb.NodeInfo(node_id="dashboard")), timeout=0.25)
                    dps[d] = {"entries": r.summary.entry_count, "hash": r.summary.log_hash,
                              "max_lamport": r.summary.max_lamport_time,
                              "clock": r.receiver_lamport_time}
                except grpc.RpcError:
                    dps[d] = None

            prev = self.leader
            with self.lock:
                self.status, self.dp_status = statuses, dps
                self.users, self.epoch = users, epoch
                self.leader = leader
            if leader != prev:
                if leader and self.leader_lost_at:
                    ms = (time.time() - self.leader_lost_at) * 1000
                    self.event(f"{leader} elected leader (term {statuses[leader]['term']}) — "
                               f"failover took {ms:.0f} ms", "good")
                    self.leader_lost_at = None
                elif leader:
                    self.event(f"{leader} is leader (term {statuses[leader]['term']})", "good")
                else:
                    self.leader_lost_at = self.leader_lost_at or time.time()
                    self.event("no leader — election in progress", "warn")
            time.sleep(0.2)

    # ---------------------------------------------------------------- actions
    def node_action(self, nid, action):
        proc = self.raft.get(nid) or self.dp.get(nid)
        if not proc:
            raise HTTPException(404, "unknown node")
        was_leader = nid == self.leader
        getattr(proc, action)()
        if action in ("kill", "pause") and was_leader:
            self.leader_lost_at = time.time()
        verb = {"kill": "crashed (SIGKILL)", "start": "restarted",
                "pause": "paused (simulated partition)", "resume": "resumed"}[action]
        self.event(f"{nid} {verb}{' — it was the leader' if was_leader else ''}",
                   "bad" if action in ("kill", "pause") else "info")

    def acl(self, action, user):
        order = ([self.leader] if self.leader else []) + [n for n in self.ids if n != self.leader]
        tried = set()
        for nid in order:
            if nid in tried or self.raft[nid].state() != "running":
                continue
            tried.add(nid)
            try:
                t0 = time.time()
                if action == "add":
                    r = self.raft_stub[nid].AddUser(pb.AddUserRequest(user_id=user), timeout=4)
                else:
                    r = self.raft_stub[nid].RevokeUser(pb.RevokeUserRequest(user_id=user), timeout=4)
                ms = (time.time() - t0) * 1000
                if r.success:
                    ep = r.current_epoch if action == "add" else r.new_epoch
                    self.event(f"{action} '{user}' committed via {nid} in {ms:.1f} ms (epoch {ep})",
                               "good")
                    return {"ok": True, "via": nid, "ms": ms, "epoch": ep}
            except grpc.RpcError:
                continue
        self.event(f"{action} '{user}' failed — no reachable leader / no quorum", "bad")
        return {"ok": False}

    def chat(self, user, text, target):
        with self.lock:
            self.lamport += 1
            lam = self.lamport
        epoch = self.epoch or 1
        msg = pb.ChatMessage(message_id=f"{user}_{lam}", sender_id=user, payload=text,
                             lamport_time=lam, epoch=epoch)
        self.messages.append(msg)
        targets = self.dp_ids if target == "all" else [target]
        results = {}
        for d in targets:
            results[d] = self._push(d, [msg])
        accepted = [d for d, r in results.items() if r == 1]
        if accepted:
            self.event(f"'{user}': \"{text}\" → accepted by {', '.join(accepted)}", "info")
        else:
            self.event(f"'{user}': \"{text}\" → REJECTED (not authorised / node down)", "bad")
        return results

    def _push(self, d, msgs):
        if self.dp[d].state() != "running":
            return None
        try:
            before = self.dp_stub[d].SyncLogs(
                pb.SyncLogsRequest(sender=pb.NodeInfo(node_id="dashboard")), timeout=0.5)
            after = self.dp_stub[d].SyncLogs(
                pb.SyncLogsRequest(sender=pb.NodeInfo(node_id="dashboard"), logs=msgs), timeout=2)
            return after.summary.entry_count - before.summary.entry_count
        except grpc.RpcError:
            return None

    def anti_entropy(self):
        total = 0
        for d in self.dp_ids:
            n = self._push(d, self.messages) or 0
            total += n
        self.event(f"anti-entropy round: re-sent {len(self.messages)} msgs to every data node, "
                   f"{total} were new", "info")
        return {"new": total}

    def run_bench(self, nodes, clients, ops):
        if self.bench["running"]:
            return

        def go():
            self.bench = {"running": True, "output": f"running raft bench: {nodes} nodes, "
                                                       f"{clients} clients x {ops} ops...\n"}
            try:
                out = subprocess.run([str(ROOT / "build" / "tput_bench"), "raft", str(nodes),
                                      str(clients), str(ops)], cwd=ROOT, capture_output=True,
                                     text=True, timeout=180)
                self.bench["output"] += out.stdout + out.stderr
            except Exception as e:  # noqa: BLE001
                self.bench["output"] += f"error: {e}"
            self.bench["running"] = False
            self.event(f"benchmark finished ({nodes} nodes, {clients} clients)", "info")

        threading.Thread(target=go, daemon=True).start()

    def snapshot(self):
        with self.lock:
            nodes = []
            for nid in self.ids:
                nodes.append({"id": nid, "addr": self.addrs[nid], "proc": self.raft[nid].state(),
                              "status": self.status.get(nid)})
            dps = []
            for i, d in enumerate(self.dp_ids):
                dps.append({"id": d, "addr": self.dp_addrs[d], "auth": self.ids[i],
                            "proc": self.dp[d].state(), "status": self.dp_status.get(d)})
            hashes = {s["status"]["hash"] for s in dps if s["status"]}
            return {"nodes": nodes, "leader": self.leader, "users": self.users,
                    "epoch": self.epoch, "data": dps,
                    "converged": len(hashes) == 1 and all(s["status"] for s in dps),
                    "events": self.events[-60:], "bench": self.bench,
                    "quorum": len(self.ids) // 2 + 1, "messages": len(self.messages)}


# --------------------------------------------------------------------------
# HTTP API
# --------------------------------------------------------------------------
app = FastAPI(title="LBM dashboard")
cluster: Cluster = None  # set in main


class AclReq(BaseModel):
    action: str
    user: str


class ChatReq(BaseModel):
    user: str
    text: str
    target: str = "all"


class BenchReq(BaseModel):
    nodes: int = 3
    clients: int = 8
    ops: int = 300


@app.get("/")
def index():
    return FileResponse(UI_DIR / "index.html")


@app.get("/api/state")
def state():
    return cluster.snapshot()


@app.post("/api/node/{nid}/{action}")
def node(nid: str, action: str):
    if action not in ("kill", "start", "pause", "resume"):
        raise HTTPException(400, "bad action")
    cluster.node_action(nid, action)
    return {"ok": True}


@app.post("/api/acl")
def acl(req: AclReq):
    if req.action not in ("add", "revoke") or not req.user.strip():
        raise HTTPException(400, "bad request")
    return cluster.acl(req.action, req.user.strip())


@app.post("/api/chat")
def chat(req: ChatReq):
    return cluster.chat(req.user.strip(), req.text, req.target)


@app.post("/api/sync")
def sync():
    return cluster.anti_entropy()


@app.post("/api/bench")
def bench(req: BenchReq):
    cluster.run_bench(max(3, min(req.nodes, 9)), max(1, min(req.clients, 64)),
                      max(10, min(req.ops, 2000)))
    return {"ok": True}


def main():
    global cluster
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1",
                    help="use 0.0.0.0 to open the dashboard from outside the VM")
    a = ap.parse_args()
    if not (ROOT / "build" / "raft_server").exists():
        sys.exit("build first: cmake -S . -B build && cmake --build build -j")
    cluster = Cluster(a.nodes)
    atexit.register(cluster.shutdown)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    import uvicorn
    print(f"dashboard: http://{a.host}:{a.port}")
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
