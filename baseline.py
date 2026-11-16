import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLIENT_TIMEOUT_S = 60


def server_outbound_kbps(server_csv):
    # average outbound rate computed from the server packet log
    rows = []
    with open(server_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["event"] == "PACKET" and r["direction"] == "SEND":
                rows.append((int(r["epoch_ms"]), int(r["size"])))
    if len(rows) < 2:
        return 0.0
    span_s = max((rows[-1][0] - rows[0][0]) / 1000.0, 0.001)
    return sum(size for _, size in rows[1:]) * 8 / 1000.0 / span_s


def run_once(args, run_no, stamp):
    run_dir = HERE / args.out / f"baseline_{stamp}_run{run_no}"
    run_dir.mkdir(parents=True, exist_ok=True)
    py = [sys.executable, "-u"]

    server_out = open(run_dir / "server.out", "w", encoding="utf-8")
    server = subprocess.Popen(
        py + [str(HERE / "server.py"), "--port", str(args.port),
              "--throttle-kbps", str(args.throttle_kbps), "--log-dir", str(run_dir)],
        stdout=server_out, stderr=subprocess.STDOUT, cwd=HERE)
    time.sleep(1.0)   # let the server bind

    procs, outs = [], []
    t0 = time.perf_counter()
    for cid in range(1, args.clients + 1):
        out = open(run_dir / f"client_{cid}.out", "w", encoding="utf-8")
        outs.append(out)
        procs.append(subprocess.Popen(
            py + [str(HERE / "client.py"), str(cid), "--server-port", str(args.port),
                  "--log-dir", str(run_dir)],
            stdout=out, stderr=subprocess.STDOUT, cwd=HERE))

    exit_codes = []
    for p in procs:
        try:
            exit_codes.append(p.wait(timeout=CLIENT_TIMEOUT_S))
        except subprocess.TimeoutExpired:
            p.kill()
            exit_codes.append(-1)
    wall_s = time.perf_counter() - t0

    server.terminate()
    try:
        server.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server.kill()
    for f in outs + [server_out]:
        f.close()

    summaries = []
    for cid in range(1, args.clients + 1):
        path = run_dir / f"client_{cid}_summary.json"
        if path.exists():
            summaries.append(json.loads(path.read_text(encoding="utf-8")))
        else:
            summaries.append({"peer_id": cid, "complete": False})

    kbps = server_outbound_kbps(run_dir / "server_packets.csv")
    cols = ["run", "peer_id", "complete", "map_verified", "completion_time_ms", "mean_rtt_ms",
            "requests_sent", "retransmits", "timeouts", "duplicates", "hash_failures"]
    with open(run_dir / "summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for s in summaries:
            w.writerow({"run": run_no, **s})

    print(f"\n=== run {run_no}  ({wall_s:.1f}s wall clock)  -> {run_dir}")
    print(f"{'peer':>4} {'complete':>8} {'verified':>8} {'time_ms':>9} {'rtt_ms':>7} {'retx':>5} {'dups':>5}")
    for s in summaries:
        print(f"{s['peer_id']:>4} {str(s.get('complete')):>8} {str(s.get('map_verified')):>8} "
              f"{str(s.get('completion_time_ms')):>9} {str(s.get('mean_rtt_ms')):>7} "
              f"{str(s.get('retransmits')):>5} {str(s.get('duplicates')):>5}")
    print(f"server outbound average: {kbps:.1f} kbps "
          f"({'throttle ON' if args.throttle_kbps else 'throttle OFF in Phase 1 baseline'})")

    ok = all(s.get("complete") and s.get("map_verified") for s in summaries) and all(c == 0 for c in exit_codes)
    times = [s["completion_time_ms"] for s in summaries if s.get("completion_time_ms") is not None]
    return ok, times


def main():
    parser = argparse.ArgumentParser(description="Run the local baseline scenario")
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--throttle-kbps", type=float, default=0)
    parser.add_argument("--out", default="logs", help="output folder (relative to this script)")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_ok, all_times = True, []
    for run_no in range(1, args.runs + 1):
        ok, times = run_once(args, run_no, stamp)
        all_ok &= ok
        all_times += times

    if all_times:
        print(f"\ncompletion_time_ms over {len(all_times)} client runs: "
              f"min={min(all_times):.0f}  median={statistics.median(all_times):.0f}  max={max(all_times):.0f}")
    print("BASELINE RESULT:", "PASS" if all_ok else "FAIL")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()