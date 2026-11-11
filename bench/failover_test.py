#!/usr/bin/env python3
"""
Failover + durability test (run from repo root after building):

    python3 bench/failover_test.py [nodes=3] [seconds=6]

1. starts an N-node Raft cluster
2. client threads keep issuing AddUser, following leader_hint redirects
3. the leader is SIGKILLed mid-run, then restarted
4. checks that every write acknowledged as committed is present on EVERY node
5. kills the whole cluster, restarts it from disk, checks again (log persistence)
"""
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ui"))
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
from dashboard import pb, pbg  # noqa: E402  (generates stubs if needed)
import grpc  # noqa: E402

N = int(sys.argv[1]) if len(sys.argv) > 1 else 3
SECS = float(sys.argv[2]) if len(sys.argv) > 2 else 6
RUN = ROOT / "ui" / "_run_failover"
RUN.mkdir(exist_ok=True)
for f in RUN.glob("raft_*"):
    f.unlink()

ids = [f"ft-{i + 1}" for i in range(N)]
addrs = {n: f"127.0.0.1:{52053 + i}" for i, n in enumerate(ids)}
by_addr = {a: n for n, a in addrs.items()}
procs = {}


def start(n):
    peers = [f"{p}={addrs[p]}" for p in ids if p != n]
    procs[n] = subprocess.Popen([str(ROOT / "build" / "raft_server"), n, addrs[n], *peers],
                                cwd=RUN, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def kill(n):
    procs[n].send_signal(signal.SIGKILL)
    procs[n].wait()


stubs = {n: pbg.ControlPlaneRaftStub(grpc.insecure_channel(a)) for n, a in addrs.items()}


def leader():
    for n in ids:
        try:
            s = stubs[n].GetStatus(pb.StatusRequest(), timeout=0.3)
            if s.role == "leader":
                return n
        except grpc.RpcError:
            pass
    return None


def users_on(n):
    return set(stubs[n].ListUsers(pb.ListUsersRequest(), timeout=2).authorized_users)


for n in ids:
    start(n)
while not leader():
    time.sleep(0.05)

acked, lock, stop = set(), threading.Lock(), threading.Event()
stats = {"ok": 0, "redirect": 0, "fail": 0}


def client(c):
    target, k = ids[0], 0
    while not stop.is_set():
        u = f"c{c}-{k}"
        try:
            r = stubs[target].AddUser(pb.AddUserRequest(user_id=u), timeout=2)
            if r.success:
                with lock:
                    acked.add(u)
                    stats["ok"] += 1
                k += 1
                continue
            with lock:
                stats["redirect"] += 1
            target = by_addr.get(r.leader_hint) or leader() or target
        except grpc.RpcError:
            with lock:
                stats["fail"] += 1
            target = leader() or ids[(ids.index(target) + 1) % N]
            time.sleep(0.02)


threads = [threading.Thread(target=client, args=(c,)) for c in range(4)]
for t in threads:
    t.start()

time.sleep(SECS / 3)
old = leader()
t_kill = time.time()
kill(old)
print(f"killed leader {old}")
while True:
    new = leader()
    if new and new != old:
        break
    time.sleep(0.01)
print(f"new leader {new} after {(time.time() - t_kill) * 1000:.0f} ms")
time.sleep(SECS / 3)
start(old)
print(f"restarted {old}")
time.sleep(SECS / 3)
stop.set()
for t in threads:
    t.join()
time.sleep(1.0)   # let followers apply the final commit index

ok = True
print(f"client stats: {stats}, acknowledged writes: {len(acked)}")
for n in ids:
    missing = acked - users_on(n)
    print(f"  {n}: {len(users_on(n))} users, missing acknowledged: {len(missing)}")
    ok &= not missing

# full-cluster crash + restart from disk
for n in ids:
    kill(n)
print("killed ALL nodes; restarting from persisted state + log")
for n in ids:
    start(n)
while not leader():
    time.sleep(0.05)
time.sleep(1.0)
for n in ids:
    missing = acked - users_on(n)
    print(f"  {n}: after full restart missing acknowledged: {len(missing)}")
    ok &= not missing

for n in ids:
    kill(n)
print("PASS: no acknowledged write was lost" if ok else "FAIL: acknowledged writes lost")
sys.exit(0 if ok else 1)
