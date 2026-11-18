#!/usr/bin/env python3
"""
Black-box check of an already-running cluster (Docker Compose now, Kubernetes later).

  1. finds the Raft leader with GetStatus
  2. N clients write users through the leader (a write counts only if AddUser says success,
     i.e. it was committed by a majority)
  3. kills the leader mid-run, times how long until a new leader is elected
  4. restarts the old leader and checks every node has every acknowledged write
  5. kills ALL Raft nodes, restarts them, and checks again (persistence)

Usage (from deploy/, with the project venv):
  ../.venv/bin/python cluster_check.py
  ../.venv/bin/python cluster_check.py --clients 8 --duration 10

Kubernetes later (after `kubectl port-forward`), e.g.:
  ... --kill-cmd "kubectl delete pod {node} --wait=false" --start-cmd "" \
      --kill-all-cmd "kubectl delete pod auth-0 auth-1 auth-2 --wait=false" --start-all-cmd ""
"""
import argparse
import os
import subprocess
import sys
import threading
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "ui"))
from dashboard import pb, pbg  # noqa: E402  (generates the gRPC stubs if needed)
import grpc  # noqa: E402

DEFAULT_NODES = "auth-1=localhost:50053,auth-2=localhost:50054,auth-3=localhost:50055"


class Cluster:
    def __init__(self, nodes):
        self.addr = nodes
        self.chan = {n: grpc.insecure_channel(a) for n, a in nodes.items()}
        self.stub = {n: pbg.ControlPlaneRaftStub(c) for n, c in self.chan.items()}
        for c in self.chan.values():  # connect up front so the first calls don't time out
            try:
                grpc.channel_ready_future(c).result(timeout=3)
            except grpc.FutureTimeoutError:
                pass

    def status(self, node, timeout=1.0):
        try:
            return self.stub[node].GetStatus(pb.StatusRequest(), timeout=timeout)
        except grpc.RpcError:
            return None

    def leader(self):
        """(node, term) of the leader with the highest term, or None."""
        best = None
        for n in self.addr:
            s = self.status(n)
            if s is not None and s.role == "leader" and (best is None or s.term > best[1]):
                best = (n, s.term)
        return best

    def wait_leader(self, timeout, exclude=None, min_term=0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            l = self.leader()
            if l and l[0] != exclude and l[1] > min_term:
                return l, time.time() - t0
            time.sleep(0.02)
        return None, None

    def users(self, node):
        try:
            r = self.stub[node].ListUsers(pb.ListUsersRequest(), timeout=3)
            return set(r.authorized_users)
        except grpc.RpcError:
            return None

    def print_status(self):
        for n in self.addr:
            s = self.status(n)
            if s is None:
                print(f"  {n:8s} DOWN")
            else:
                print(f"  {n:8s} {s.role:9s} term={s.term:<3d} commit={s.commit_index:<6d} users={s.user_count}")


def run(cmd):
    if not cmd:
        return
    print(f"  $ {cmd}")
    subprocess.run(cmd, shell=True, cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def verify(cl, acked, label, timeout=20):
    """Wait until every node holds every acknowledged write; report what's missing."""
    t0 = time.time()
    while True:
        missing = {}
        for n in cl.addr:
            u = cl.users(n)
            missing[n] = len(acked) if u is None else len(acked - u)
        if all(m == 0 for m in missing.values()) or time.time() - t0 > timeout:
            break
        time.sleep(0.5)
    ok = all(m == 0 for m in missing.values())
    for n, m in missing.items():
        print(f"  {n}: missing acknowledged writes: {m}")
    print(f"  {label}: {'OK' if ok else 'FAILED'}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", default=DEFAULT_NODES, help="id=host:port,...")
    ap.add_argument("--clients", type=int, default=4)
    ap.add_argument("--duration", type=float, default=6.0, help="seconds of writes")
    ap.add_argument("--kill-cmd", default="docker compose kill {node}")
    ap.add_argument("--start-cmd", default="docker compose start {node}")
    ap.add_argument("--kill-all-cmd", default="docker compose kill {all}")
    ap.add_argument("--start-all-cmd", default="docker compose start {all}")
    ap.add_argument("--no-full-restart", action="store_true")
    a = ap.parse_args()

    nodes = dict(x.split("=", 1) for x in a.nodes.split(","))
    allnodes = " ".join(nodes)
    cl = Cluster(nodes)

    print("== cluster status")
    cl.print_status()
    l, _ = cl.wait_leader(15)
    if not l:
        print("FAIL: no leader elected within 15 s")
        return 1
    print(f"leader: {l[0]} (term {l[1]})")

    # ---- writes + leader kill ----
    prefix = "chk-" + uuid.uuid4().hex[:6]
    acked, lock = set(), threading.Lock()
    stats = {"ok": 0, "fail": 0}
    leader_ref = [l[0]]
    stop = threading.Event()

    def refresh():
        nl = cl.leader()
        if nl:
            leader_ref[0] = nl[0]
        else:
            time.sleep(0.05)

    def writer(wid):
        i = 0
        while not stop.is_set():
            user = f"{prefix}-{wid}-{i}"
            i += 1
            try:
                r = cl.stub[leader_ref[0]].AddUser(pb.AddUserRequest(user_id=user), timeout=1.0)
                if r.success:
                    with lock:
                        acked.add(user)
                        stats["ok"] += 1
                    continue
            except grpc.RpcError:
                pass
            with lock:
                stats["fail"] += 1
            refresh()

    print(f"\n== {a.clients} clients writing for {a.duration:.0f}s; leader is killed after {a.duration/3:.1f}s")
    threads = [threading.Thread(target=writer, args=(w,), daemon=True) for w in range(a.clients)]
    t_start = time.time()
    for t in threads:
        t.start()

    time.sleep(a.duration / 3)
    old, old_term = l
    run(a.kill_cmd.format(node=old))
    new, failover_s = cl.wait_leader(15, exclude=old, min_term=old_term)
    if new:
        print(f"  new leader {new[0]} (term {new[1]}) after {failover_s*1000:.0f} ms")
    else:
        print("  no new leader within 15 s")

    time.sleep(max(0.0, a.duration - (time.time() - t_start)))
    stop.set()
    for t in threads:
        t.join()
    elapsed = time.time() - t_start
    run(a.start_cmd.format(node=old))

    print(f"\n  acknowledged writes: {stats['ok']}  (failed/redirected attempts: {stats['fail']})")
    print(f"  throughput incl. failover: {stats['ok']/elapsed:.0f} committed writes/s")

    print("\n== after leader kill + restart")
    ok = verify(cl, acked, "no acknowledged write lost")

    # ---- full-cluster restart from disk ----
    if not a.no_full_restart:
        print("\n== killing ALL Raft nodes and restarting from persisted state")
        run(a.kill_all_cmd.format(all=allnodes))
        time.sleep(1)
        run(a.start_all_cmd.format(all=allnodes))
        nl, t = cl.wait_leader(30)
        print(f"  leader after full restart: {nl[0] if nl else 'none'}"
              + (f" ({t*1000:.0f} ms)" if nl else ""))
        ok = verify(cl, acked, "persistence after full restart") and ok

    print("\n== final status")
    cl.print_status()
    print("\nPASS" if ok and new else "\nFAIL")
    return 0 if ok and new else 1


if __name__ == "__main__":
    sys.exit(main())
