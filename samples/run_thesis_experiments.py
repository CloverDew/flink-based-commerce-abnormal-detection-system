import csv
import json
import math
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
EXP_DIR = ROOT / ".data" / "experiment"
EXP_DIR.mkdir(parents=True, exist_ok=True)

ALERT_TOPIC = "alerts-exp"
JAR_ARTIFACT = "target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar"


def run(cmd: str, timeout: int = 1200) -> str:
    p = subprocess.run(
        cmd,
        shell=True,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {cmd}\nstdout={p.stdout}\nstderr={p.stderr}")
    return p.stdout + p.stderr


def run_argv(argv: list[str], timeout: int = 1200) -> str:
    p = subprocess.run(
        argv,
        shell=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {' '.join(argv)}\nstdout={p.stdout}\nstderr={p.stderr}")
    return p.stdout + p.stderr


def run_argv_input(argv: list[str], input_text: str, timeout: int = 1200) -> str:
    p = subprocess.run(
        argv,
        shell=False,
        cwd=ROOT,
        input=input_text,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {' '.join(argv)}\nstdout={p.stdout}\nstderr={p.stderr}")
    return p.stdout + p.stderr


def update_flink_conf(parallelism: int, backend: str):
    conf = ROOT / "docker" / "conf" / "flink-job.conf"
    lines = conf.read_text(encoding="utf-8").splitlines()
    out = []
    for line in lines:
        if line.startswith("PARALLELISM="):
            out.append(f"PARALLELISM={parallelism}")
        elif line.startswith("STATE_BACKEND="):
            out.append(f"STATE_BACKEND={backend}")
        elif line.startswith("ALERT_TOPIC="):
            out.append(f"ALERT_TOPIC={ALERT_TOPIC}")
        else:
            out.append(line)
    with conf.open("w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out) + "\n")


def current_job_id() -> str:
    out = run("docker exec flink-based-commerce-abnormal-detection-system-jobmanager-1 flink list -m jobmanager:8081")
    m = re.search(r"\b([0-9a-fA-F]{32})\b", out)
    if m:
        return m.group(1)
    return ""


def restart_job(parallelism: int, backend: str):
    update_flink_conf(parallelism, backend)
    jid = current_job_id()
    if jid:
        run(f"docker exec flink-based-commerce-abnormal-detection-system-jobmanager-1 flink cancel -m jobmanager:8081 {jid}")
    run('docker compose run --rm --entrypoint /bin/bash jobmanager -lc "/workspace/docker/scripts/submit-flink-job.sh"')
    time.sleep(6)


def generate_rules_and_events(window_ms: int, event_count: int):
    # IMPORTANT: Flink side keeps the latest rule version in broadcast state.
    # If we publish a lower/same version, it will be ignored and experiments produce 0 alerts.
    ver = int(time.time() * 1000)
    rules = [
        {
            "ruleId": "thesis-abnormal-login",
            "ruleName": "论文-异常登录后敏感操作",
            "ruleType": "ABNORMAL_LOGIN",
            "status": "ENABLED",
            "targetActionType": "LOGIN",
            "windowSizeMs": window_ms,
            "threshold": 2,
            "groupKeyType": "BY_USER_ID",
            "priority": 100,
            "version": ver,
            "updateTimestamp": int(time.time() * 1000),
            "valid": True,
            "enabled": True,
        },
        {
            "ruleId": "thesis-order-brush",
            "ruleName": "论文-高频下单",
            "ruleType": "ORDER_BRUSH",
            "status": "ENABLED",
            "targetActionType": "ORDER",
            "windowSizeMs": window_ms,
            "threshold": 5,
            "groupKeyType": "BY_USER_ID",
            "priority": 90,
            "version": ver,
            "updateTimestamp": int(time.time() * 1000),
            "valid": True,
            "enabled": True,
        },
        {
            "ruleId": "thesis-high-freq-view",
            "ruleName": "论文-高频访问",
            "ruleType": "HIGH_FREQ_ACCESS",
            "status": "ENABLED",
            "targetActionType": "VIEW",
            "windowSizeMs": window_ms,
            "threshold": 100,
            "groupKeyType": "BY_USER_ID",
            "priority": 80,
            "version": ver,
            "updateTimestamp": int(time.time() * 1000),
            "valid": True,
            "enabled": True,
        },
    ]
    (EXP_DIR / "thesis-risk-rules.json").write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")

    now = int(time.time() * 1000)
    events = []
    # Each abnormal cohort user contributes 2 + 5 + 100 events (login chain, orders, views).
    per_user = 107
    group_n = max(1, min(event_count // per_user, 8000))
    intra_login_gap = min(8000, max(800, window_ms // 8))
    order_spacing = min(3500, max(120, window_ms // 25))
    view_spacing = min(800, max(15, window_ms // 200))
    for i in range(group_n):
        uid = f"al_u_{window_ms}_{i}"
        ts = now + i * 800
        events.append({"userId": uid, "actionType": "LOGIN", "ip": f"10.0.0.{i%250+1}", "timestamp": ts, "sessionId": f"als-{i}"})
        events.append(
            {"userId": uid, "actionType": "CHANGE_PASSWORD", "ip": f"10.0.1.{i%250+1}", "timestamp": ts + intra_login_gap, "sessionId": f"als-{i}"}
        )
    for i in range(group_n):
        uid = f"ob_u_{window_ms}_{i}"
        base = now + 120_000 + i * 900
        for k in range(5):
            events.append(
                {
                    "userId": uid,
                    "actionType": "ORDER",
                    "ip": f"10.1.0.{i%250+1}",
                    "timestamp": base + k * order_spacing,
                    "sessionId": f"obs-{i}",
                    "productId": f"p{k}",
                    "amount": 99.9,
                }
            )
    for i in range(group_n):
        uid = f"hf_u_{window_ms}_{i}"
        base = now + 240_000 + i * 1200
        for k in range(100):
            events.append(
                {
                    "userId": uid,
                    "actionType": "VIEW",
                    "ip": f"10.2.0.{i%250+1}",
                    "timestamp": base + k * view_spacing,
                    "sessionId": f"hfs-{i}",
                    "productId": f"pv{k%20}",
                }
            )

    events.sort(key=lambda e: int(e["timestamp"]))
    events = events[:event_count]
    with (EXP_DIR / "thesis-events.jsonl").open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    return len(events)


def publish_rules():
    # Avoid Maven during experiments (network dependency downloads are flaky).
    # Use Kafka console producer with key support.
    rules = json.loads((EXP_DIR / "thesis-risk-rules.json").read_text(encoding="utf-8"))
    payload = "".join([f"{r.get('ruleId','')}|{json.dumps(r, ensure_ascii=False)}\n" for r in rules if r.get("ruleId")])
    run_argv_input(
        [
            "docker",
            "exec",
            "-i",
            "flink-based-commerce-abnormal-detection-system-kafka-1",
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            "risk-rules",
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=|",
        ],
        payload,
        timeout=120,
    )


def sample_stats(stop_flag, cpu_samples, mem_samples):
    while not stop_flag["stop"]:
        try:
            out = run('docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}"', timeout=60)
            for line in out.splitlines():
                if "taskmanager" in line:
                    parts = line.split(",")
                    cpu = float(parts[1].replace("%", "").strip())
                    mem_raw = parts[2].split("/")[0].strip().upper()
                    if "GIB" in mem_raw:
                        mem = float(mem_raw.replace("GIB", "").strip()) * 1024
                    elif "MIB" in mem_raw:
                        mem = float(mem_raw.replace("MIB", "").strip())
                    else:
                        mem = 0.0
                    cpu_samples.append(cpu)
                    mem_samples.append(mem)
        except Exception:
            pass
        time.sleep(2)


def publish_events_and_measure() -> float:
    stop = {"stop": False}
    cpus, mems = [], []
    t = threading.Thread(target=sample_stats, args=(stop, cpus, mems), daemon=True)
    t.start()
    start = time.time()
    lines = (EXP_DIR / "thesis-events.jsonl").read_text(encoding="utf-8", errors="ignore").splitlines()
    payload_parts = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        key = obj.get("userId") or obj.get("user_id") or ""
        if not key:
            continue
        payload_parts.append(f"{key}|{json.dumps(obj, ensure_ascii=False)}\n")
    payload = "".join(payload_parts)
    run_argv_input(
        [
            "docker",
            "exec",
            "-i",
            "flink-based-commerce-abnormal-detection-system-kafka-1",
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            "user-behavior",
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=|",
        ],
        payload,
        timeout=600,
    )
    duration = time.time() - start
    time.sleep(8)
    stop["stop"] = True
    t.join(timeout=5)
    return duration, (sum(cpus) / len(cpus) if cpus else 0.0), (sum(mems) / len(mems) if mems else 0.0)


def capture_alerts(tag: str):
    out_file = EXP_DIR / f"alerts-{tag}.jsonl"
    group_id = f"exp-capture-{tag}-{int(time.time() * 1000)}"
    # Avoid shell redirection ("> file") which is fragile across Windows + docker exec.
    p = subprocess.run(
        [
            "docker",
            "exec",
            "flink-based-commerce-abnormal-detection-system-kafka-1",
            "kafka-console-consumer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            ALERT_TOPIC,
            "--from-beginning",
            "--group",
            group_id,
            "--consumer-property",
            "auto.offset.reset=earliest",
            "--timeout-ms",
            "25000",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    text = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    out_file.write_text(text, encoding="utf-8", newline="\n")
    rows = []
    text = out_file.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            rows.append(json.loads(line))
    return rows


def latency_metrics(alerts):
    lats = [abs(a["alertTimestamp"] - a.get("lastEventTimestamp", a["alertTimestamp"])) for a in alerts if "alertTimestamp" in a]
    if not lats:
        return 0.0, 0.0
    lats.sort()
    p99 = lats[max(0, math.ceil(len(lats) * 0.99) - 1)]
    return sum(lats) / len(lats), p99


def run_case(name: str, parallelism: int, window_ms: int, backend: str, event_count: int):
    restart_job(parallelism, backend)
    actual = generate_rules_and_events(window_ms, event_count)
    publish_rules()
    duration, cpu_avg, mem_avg = publish_events_and_measure()
    alerts = capture_alerts(name)
    avg_lat, p99_lat = latency_metrics(alerts)
    throughput = actual / duration if duration > 0 else 0
    return {
        "case": name,
        "parallelism": parallelism,
        "window": window_ms,
        "backend": backend,
        "events": actual,
        "alerts": len(alerts),
        "throughput": throughput,
        "avg_latency_ms": avg_lat,
        "p99_latency_ms": p99_lat,
        "cpu_pct": cpu_avg,
        "mem_mb": mem_avg,
    }


def main():
    results = []

    # parallelism experiments
    for p in [1, 2, 4, 8]:
        results.append(run_case(f"p{p}", p, 60000, "hashmap", 30000))
    # window experiments
    for w in [30000, 60000, 300000, 600000]:
        results.append(run_case(f"w{w}", 4, w, "hashmap", 30000))
    # backend experiments
    for b in ["hashmap", "rocksdb"]:
        results.append(run_case(f"b{b}", 4, 60000, b, 30000))
    # scalability experiments
    for n in [10000, 50000, 100000, 200000]:
        results.append(run_case(f"s{n}", 4, 60000, "rocksdb", n))

    with (EXP_DIR / "all_results.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["case", "parallelism", "window", "backend", "events", "alerts", "throughput", "avg_latency_ms", "p99_latency_ms", "cpu_pct", "mem_mb"],
        )
        w.writeheader()
        w.writerows(results)

    # split tables
    def write_filtered(name, pred):
        rows = [r for r in results if pred(r)]
        with (EXP_DIR / name).open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["case", "parallelism", "window", "backend", "events", "alerts", "throughput", "avg_latency_ms", "p99_latency_ms", "cpu_pct", "mem_mb"],
            )
            w.writeheader()
            w.writerows(rows)
        return rows

    p_rows = write_filtered("perf_parallelism.csv", lambda r: r["case"].startswith("p"))
    w_rows = write_filtered("perf_windows.csv", lambda r: r["case"].startswith("w"))
    b_rows = write_filtered("perf_backend.csv", lambda r: r["case"].startswith("b"))
    s_rows = write_filtered("scalability.csv", lambda r: r["case"].startswith("s"))

    # charts (matplotlib fallback)
    plt.figure(figsize=(7, 4))
    x = [r["parallelism"] for r in p_rows]
    y1 = [r["throughput"] for r in p_rows]
    y2 = [r["avg_latency_ms"] for r in p_rows]
    plt.plot(x, y1, marker="o", label="throughput(events/s)")
    plt.plot(x, y2, marker="s", label="avg latency(ms)")
    plt.xlabel("parallelism")
    plt.legend()
    plt.tight_layout()
    plt.savefig(EXP_DIR / "parallelism_vs_perf.png", dpi=160)

    plt.figure(figsize=(7, 4))
    x = [r["events"] for r in s_rows]
    y = [r["throughput"] for r in s_rows]
    plt.plot(x, y, marker="o")
    plt.xlabel("data scale(events)")
    plt.ylabel("throughput(events/s)")
    plt.tight_layout()
    plt.savefig(EXP_DIR / "scalability_throughput.png", dpi=160)

    fig_dir = EXP_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "samples" / "render_thesis_figures.py"),
                "--input-dir",
                str(EXP_DIR),
                "--out-dir",
                str(fig_dir),
            ],
            cwd=str(ROOT),
            check=False,
            timeout=120,
        )
    except Exception:
        pass

    print("done", len(results), "cases")


if __name__ == "__main__":
    main()
