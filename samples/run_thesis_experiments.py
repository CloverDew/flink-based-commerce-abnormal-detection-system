import argparse
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

from experiment_artifacts import (
    CLASSIFICATION_DISCLAIMER,
    CLASSIFICATION_LABEL,
    ensure_run_layout,
    file_record,
    mirror_latest_run,
    now_iso_utc,
    root_relative,
    summarize_manifest,
    update_manifest,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
JAR_ARTIFACT = "target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar"
KAFKA_CONTAINER = "flink-based-commerce-abnormal-detection-system-kafka-1"
JOBMANAGER_CONTAINER = "flink-based-commerce-abnormal-detection-system-jobmanager-1"
FIELDNAMES = [
    "case",
    "parallelism",
    "window",
    "backend",
    "events",
    "alerts",
    "alerts_raw_total",
    "unexpected_alerts",
    "scope_valid",
    "throughput",
    "avg_latency_ms",
    "p99_latency_ms",
    "cpu_pct",
    "mem_mb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run reproducible thesis experiments with isolated per-run artifacts.")
    parser.add_argument("--run-id", default="", help="Optional fixed run id. If omitted, a timestamped run id is created.")
    parser.add_argument("--cases", default="", help="Optional comma-separated case names to run, for example: p1,w60000,brocksdb")
    parser.add_argument("--skip-build", action="store_true", help="Skip rebuilding the shaded Flink job jar before experiments.")
    parser.add_argument("--skip-figures", action="store_true", help="Skip Plotly HTML rendering.")
    return parser.parse_args()


def run(cmd: str, timeout: int = 1200) -> str:
    p = subprocess.run(
        cmd,
        shell=True,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
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
        encoding="utf-8",
        errors="ignore",
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
        encoding="utf-8",
        errors="ignore",
        timeout=timeout,
    )
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {' '.join(argv)}\nstdout={p.stdout}\nstderr={p.stderr}")
    return p.stdout + p.stderr


def topic_token(text: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9._-]+", "-", text.strip())
    token = token.strip("-._").lower()
    return token or "default"


def ensure_topics(*topic_names: str) -> None:
    for topic in topic_names:
        run_argv(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "kafka-topics",
                "--bootstrap-server",
                "kafka:9092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ],
            timeout=120,
        )


def update_flink_conf(parallelism: int, backend: str, *, behavior_topic: str, rule_topic: str, alert_topic: str, kafka_group_id: str) -> None:
    conf = ROOT / "docker" / "conf" / "flink-job.conf"
    lines = conf.read_text(encoding="utf-8").splitlines()
    replacements = {
        "PARALLELISM": str(parallelism),
        "STATE_BACKEND": backend,
        "BEHAVIOR_TOPIC": behavior_topic,
        "RULE_TOPIC": rule_topic,
        "ALERT_TOPIC": alert_topic,
        "KAFKA_GROUP_ID": kafka_group_id,
    }
    seen = set()
    out = []
    for line in lines:
        key, sep, _ = line.partition("=")
        if sep and key in replacements:
            out.append(f"{key}={replacements[key]}")
            seen.add(key)
        else:
            out.append(line)
    for key, value in replacements.items():
        if key not in seen:
            out.append(f"{key}={value}")
    conf.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")


def current_job_id() -> str:
    out = run(f"docker exec {JOBMANAGER_CONTAINER} flink list -m jobmanager:8081")
    m = re.search(r"\b([0-9a-fA-F]{32})\b", out)
    return m.group(1) if m else ""


def ensure_packaged_jar(skip_build: bool) -> None:
    jar_path = ROOT / JAR_ARTIFACT
    if skip_build and jar_path.exists():
        return
    run("mvn -DskipTests package", timeout=600)


def restart_job(parallelism: int, backend: str, *, behavior_topic: str, rule_topic: str, alert_topic: str, kafka_group_id: str) -> None:
    ensure_topics(behavior_topic, rule_topic, alert_topic)
    update_flink_conf(
        parallelism,
        backend,
        behavior_topic=behavior_topic,
        rule_topic=rule_topic,
        alert_topic=alert_topic,
        kafka_group_id=kafka_group_id,
    )
    jid = current_job_id()
    if jid:
        run(f"docker exec {JOBMANAGER_CONTAINER} flink cancel -m jobmanager:8081 {jid}")
    run(
        'docker compose run --rm --entrypoint /bin/bash tools '
        '-lc "/opt/app/scripts/submit-flink-job.sh"'
    )
    time.sleep(25)


def case_topics(run_id: str, case_name: str) -> dict[str, str]:
    prefix = topic_token(f"{run_id}-{case_name}")
    return {
        "behavior_topic": f"exp-behavior-{prefix}",
        "rule_topic": f"exp-rules-{prefix}",
        "alert_topic": f"exp-alerts-{prefix}",
        "kafka_group_id": f"exp-group-{prefix}",
    }


def case_rule_payloads(case_name: str, window_ms: int, version: int) -> list[dict[str, object]]:
    prefix = topic_token(case_name)
    now_ms = int(time.time() * 1000)
    return [
        {
            "ruleId": f"{prefix}-abnormal-login",
            "ruleName": f"{case_name}-异常登录后敏感操作",
            "ruleType": "ABNORMAL_LOGIN",
            "status": "ENABLED",
            "targetActionType": "LOGIN",
            "windowSizeMs": window_ms,
            "threshold": 2,
            "groupKeyType": "BY_USER_ID",
            "priority": 100,
            "severityWeight": 1.6,
            "scoreThreshold": 1.5,
            "version": version,
            "updateTimestamp": now_ms,
            "description": "Synthetic thesis sample: login followed by sensitive action.",
            "valid": True,
            "enabled": True,
        },
        {
            "ruleId": f"{prefix}-order-brush",
            "ruleName": f"{case_name}-高频下单",
            "ruleType": "ORDER_BRUSH",
            "status": "ENABLED",
            "targetActionType": "ORDER",
            "windowSizeMs": window_ms,
            "threshold": 5,
            "groupKeyType": "BY_USER_ID",
            "priority": 90,
            "severityWeight": 1.8,
            "scoreThreshold": 1.8,
            "version": version,
            "updateTimestamp": now_ms,
            "description": "Synthetic thesis sample: repeated order burst.",
            "valid": True,
            "enabled": True,
        },
        {
            "ruleId": f"{prefix}-high-freq-view",
            "ruleName": f"{case_name}-高频访问",
            "ruleType": "HIGH_FREQ_ACCESS",
            "status": "ENABLED",
            "targetActionType": "VIEW",
            "windowSizeMs": window_ms,
            "threshold": 100,
            "groupKeyType": "BY_USER_ID",
            "priority": 80,
            "severityWeight": 1.3,
            "scoreThreshold": 1.5,
            "version": version,
            "updateTimestamp": now_ms,
            "description": "Synthetic thesis sample: dense page-view burst.",
            "valid": True,
            "enabled": True,
        },
    ]


def generate_rules_and_events(case_dir: Path, *, case_name: str, window_ms: int, event_count: int) -> dict[str, object]:
    version = int(time.time() * 1000)
    rules = case_rule_payloads(case_name, window_ms, version)
    rules_path = case_dir / "rules.json"
    rules_path.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
    events = []
    per_user = 107
    group_n = max(1, min(event_count // per_user, 8000))
    intra_login_gap = min(8000, max(800, window_ms // 8))
    order_spacing = min(3500, max(120, window_ms // 25))
    view_spacing = min(800, max(15, window_ms // 200))
    phase_ob = 8_000
    phase_hf = 16_000
    stagger_al = min(80, max(10, window_ms // 2000))
    stagger_ob = min(100, max(15, window_ms // 2500))
    stagger_hf = min(120, max(20, window_ms // 3000))
    schedule_horizon = max(
        group_n * stagger_al + intra_login_gap,
        phase_ob + group_n * stagger_ob + 5 * order_spacing,
        phase_hf + group_n * stagger_hf + 100 * view_spacing,
    )
    now = int(time.time() * 1000) - schedule_horizon - 30_000
    token = topic_token(case_name)
    for i in range(group_n):
        uid = f"al_u_{token}_{window_ms}_{i}"
        ts = now + i * stagger_al
        events.append({"userId": uid, "actionType": "LOGIN", "ip": f"10.0.0.{i%250+1}", "timestamp": ts, "sessionId": f"als-{i}"})
        events.append(
            {
                "userId": uid,
                "actionType": "CHANGE_PASSWORD",
                "ip": f"10.0.1.{i%250+1}",
                "timestamp": ts + intra_login_gap,
                "sessionId": f"als-{i}",
            }
        )
    for i in range(group_n):
        uid = f"ob_u_{token}_{window_ms}_{i}"
        base = now + phase_ob + i * stagger_ob
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
        uid = f"hf_u_{token}_{window_ms}_{i}"
        base = now + phase_hf + i * stagger_hf
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
    events_path = case_dir / "events.jsonl"
    with events_path.open("w", encoding="utf-8", newline="\n") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    return {
        "rules_path": rules_path,
        "events_path": events_path,
        "event_count": len(events),
        "rule_ids": [str(rule["ruleId"]) for rule in rules],
        "generator": {
            "classificationLabel": CLASSIFICATION_LABEL,
            "disclaimer": CLASSIFICATION_DISCLAIMER,
            "windowMs": window_ms,
            "requestedEvents": event_count,
            "actualEvents": len(events),
            "cohortUsersPerPattern": group_n,
            "patterns": ["ABNORMAL_LOGIN", "ORDER_BRUSH", "HIGH_FREQ_ACCESS"],
        },
    }


def publish_rules(rules_path: Path, rule_topic: str) -> None:
    rules = json.loads(rules_path.read_text(encoding="utf-8"))
    payload = "".join(f"{r.get('ruleId','')}|{json.dumps(r, ensure_ascii=False)}\n" for r in rules if r.get("ruleId"))
    run_argv_input(
        [
            "docker",
            "exec",
            "-i",
            KAFKA_CONTAINER,
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            rule_topic,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=|",
        ],
        payload,
        timeout=120,
    )


def sample_stats(stop_flag: dict[str, bool], cpu_samples: list[float], mem_samples: list[float]) -> None:
    while not stop_flag["stop"]:
        try:
            out = run('docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}"', timeout=60)
            for line in out.splitlines():
                if "taskmanager" not in line:
                    continue
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


def publish_events_and_measure(events_path: Path, behavior_topic: str) -> tuple[float, float, float]:
    stop = {"stop": False}
    cpus: list[float] = []
    mems: list[float] = []
    t = threading.Thread(target=sample_stats, args=(stop, cpus, mems), daemon=True)
    t.start()
    start = time.time()
    lines = events_path.read_text(encoding="utf-8", errors="ignore").splitlines()
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
            KAFKA_CONTAINER,
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            behavior_topic,
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
    cpu_avg = sum(cpus) / len(cpus) if cpus else 0.0
    mem_avg = sum(mems) / len(mems) if mems else 0.0
    return duration, cpu_avg, mem_avg


def capture_alerts(case_dir: Path, *, case_name: str, alert_topic: str) -> tuple[Path, list[dict[str, object]]]:
    out_file = case_dir / f"alerts-{case_name}.jsonl"
    group_id = f"exp-capture-{topic_token(case_name)}-{int(time.time() * 1000)}"
    p = subprocess.run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "kafka-console-consumer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            alert_topic,
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
        encoding="utf-8",
        errors="ignore",
        timeout=180,
    )
    text = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    out_file.write_text(text, encoding="utf-8", newline="\n")
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out_file, rows


def filter_alert_scope(alerts: list[dict[str, object]], expected_rule_ids: set[str]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    scoped = []
    unexpected = []
    for alert in alerts:
        rule_id = str(alert.get("ruleId", ""))
        if rule_id in expected_rule_ids:
            scoped.append(alert)
        else:
            unexpected.append(alert)
    return scoped, unexpected


def latency_metrics(alerts: list[dict[str, object]], *, window_ms: int = 60_000) -> tuple[float, float]:
    cap = max(120_000, int(window_ms) * 5)
    lats = []
    for alert in alerts:
        if "alertTimestamp" not in alert:
            continue
        value = None
        processing_lag = alert.get("processingLagMs")
        if processing_lag is not None and processing_lag != "":
            try:
                value = float(processing_lag)
            except (TypeError, ValueError):
                value = None
        if value is None:
            last_ts = alert.get("lastEventTimestamp")
            if last_ts is None:
                continue
            try:
                value = max(0.0, float(alert["alertTimestamp"]) - float(last_ts))
            except (TypeError, ValueError):
                continue
        lats.append(max(0.0, min(float(value), float(cap))))
    if not lats:
        return 0.0, 0.0
    lats.sort()
    p99 = lats[max(0, math.ceil(len(lats) * 0.99) - 1)]
    return sum(lats) / len(lats), p99


def write_rows(path: Path, rows: list[dict[str, object]]) -> list[dict[str, object]]:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def run_case(layout, *, name: str, parallelism: int, window_ms: int, backend: str, event_count: int) -> dict[str, object]:
    case_dir = layout.cases_dir / name
    case_dir.mkdir(parents=True, exist_ok=True)
    topics = case_topics(layout.run_id, name)
    restart_job(
        parallelism,
        backend,
        behavior_topic=topics["behavior_topic"],
        rule_topic=topics["rule_topic"],
        alert_topic=topics["alert_topic"],
        kafka_group_id=topics["kafka_group_id"],
    )
    generated = generate_rules_and_events(case_dir, case_name=name, window_ms=window_ms, event_count=event_count)
    rules_path = generated["rules_path"]
    events_path = generated["events_path"]
    publish_rules(rules_path, topics["rule_topic"])
    duration, cpu_avg, mem_avg = publish_events_and_measure(events_path, topics["behavior_topic"])
    alerts_path, raw_alerts = capture_alerts(case_dir, case_name=name, alert_topic=topics["alert_topic"])
    scoped_alerts, unexpected_alerts = filter_alert_scope(raw_alerts, set(generated["rule_ids"]))
    avg_lat, p99_lat = latency_metrics(scoped_alerts, window_ms=window_ms)
    throughput = generated["event_count"] / duration if duration > 0 else 0.0
    result = {
        "case": name,
        "parallelism": parallelism,
        "window": window_ms,
        "backend": backend,
        "events": generated["event_count"],
        "alerts": len(scoped_alerts),
        "alerts_raw_total": len(raw_alerts),
        "unexpected_alerts": len(unexpected_alerts),
        "scope_valid": len(unexpected_alerts) == 0,
        "throughput": round(throughput, 6),
        "avg_latency_ms": round(avg_lat, 3),
        "p99_latency_ms": round(p99_lat, 3),
        "cpu_pct": round(cpu_avg, 6),
        "mem_mb": round(mem_avg, 6),
    }
    case_result_path = case_dir / "result.json"
    write_json(
        case_result_path,
        {
            "case": result,
            "topics": topics,
            "classification": {
                "label": CLASSIFICATION_LABEL,
                "disclaimer": CLASSIFICATION_DISCLAIMER,
            },
            "generator": generated["generator"],
            "files": {
                "rules": file_record(rules_path),
                "events": file_record(events_path),
                "alerts": file_record(alerts_path),
            },
            "unexpectedRuleIds": sorted({str(a.get("ruleId", "")) for a in unexpected_alerts if a.get("ruleId")}),
            "generatedAt": now_iso_utc(),
        },
    )
    return result


def main() -> None:
    args = parse_args()
    ensure_packaged_jar(args.skip_build)
    layout = ensure_run_layout(args.run_id or None)
    planned_cases = [
        {"name": "p1", "parallelism": 1, "window_ms": 60000, "backend": "hashmap", "event_count": 30000},
        {"name": "p2", "parallelism": 2, "window_ms": 60000, "backend": "hashmap", "event_count": 30000},
        {"name": "p4", "parallelism": 4, "window_ms": 60000, "backend": "hashmap", "event_count": 30000},
        {"name": "p8", "parallelism": 8, "window_ms": 60000, "backend": "hashmap", "event_count": 30000},
        {"name": "w30000", "parallelism": 4, "window_ms": 30000, "backend": "hashmap", "event_count": 30000},
        {"name": "w60000", "parallelism": 4, "window_ms": 60000, "backend": "hashmap", "event_count": 30000},
        {"name": "w300000", "parallelism": 4, "window_ms": 300000, "backend": "hashmap", "event_count": 30000},
        {"name": "w600000", "parallelism": 4, "window_ms": 600000, "backend": "hashmap", "event_count": 30000},
        {"name": "bhashmap", "parallelism": 4, "window_ms": 60000, "backend": "hashmap", "event_count": 30000},
        {"name": "brocksdb", "parallelism": 4, "window_ms": 60000, "backend": "rocksdb", "event_count": 30000},
        {"name": "s10000", "parallelism": 4, "window_ms": 60000, "backend": "rocksdb", "event_count": 10000},
        {"name": "s50000", "parallelism": 4, "window_ms": 60000, "backend": "rocksdb", "event_count": 50000},
        {"name": "s100000", "parallelism": 4, "window_ms": 60000, "backend": "rocksdb", "event_count": 100000},
        {"name": "s200000", "parallelism": 4, "window_ms": 60000, "backend": "rocksdb", "event_count": 200000},
    ]
    if args.cases.strip():
        allowed = {token.strip() for token in args.cases.split(",") if token.strip()}
        planned_cases = [case for case in planned_cases if case["name"] in allowed]
        if not planned_cases:
            raise ValueError(f"No cases matched --cases={args.cases}")
    reproduce = f"{sys.executable} {root_relative(Path(__file__))} --run-id {layout.run_id}"
    if args.cases.strip():
        reproduce += f" --cases {args.cases}"
    if args.skip_figures:
        reproduce += " --skip-figures"
    (layout.root / "reproduce-command.txt").write_text(reproduce + "\n", encoding="utf-8")
    update_manifest(
        layout,
        {
            "generation": {
                "script": root_relative(Path(__file__)),
                "command": sys.argv,
                "reproduceCommand": reproduce,
                "classificationLabel": CLASSIFICATION_LABEL,
                "disclaimer": CLASSIFICATION_DISCLAIMER,
                "plannedCases": planned_cases,
            }
        },
    )

    results = []
    for case in planned_cases:
        results.append(
            run_case(
                layout,
                name=case["name"],
                parallelism=case["parallelism"],
                window_ms=case["window_ms"],
                backend=case["backend"],
                event_count=case["event_count"],
            )
        )

    all_results_path = layout.root / "all_results.csv"
    write_rows(all_results_path, results)
    p_rows = write_rows(layout.root / "perf_parallelism.csv", [r for r in results if str(r["case"]).startswith("p")])
    w_rows = write_rows(layout.root / "perf_windows.csv", [r for r in results if str(r["case"]).startswith("w")])
    b_rows = write_rows(layout.root / "perf_backend.csv", [r for r in results if str(r["case"]).startswith("b")])
    s_rows = write_rows(layout.root / "scalability.csv", [r for r in results if str(r["case"]).startswith("s")])

    plt.figure(figsize=(7, 4))
    plt.plot([r["parallelism"] for r in p_rows], [r["throughput"] for r in p_rows], marker="o", label="throughput(events/s)")
    plt.plot([r["parallelism"] for r in p_rows], [r["avg_latency_ms"] for r in p_rows], marker="s", label="avg latency(ms)")
    plt.xlabel("parallelism")
    plt.legend()
    plt.tight_layout()
    plt.savefig(layout.root / "parallelism_vs_perf.png", dpi=160)

    plt.figure(figsize=(7, 4))
    plt.plot([r["events"] for r in s_rows], [r["throughput"] for r in s_rows], marker="o")
    plt.xlabel("data scale(events)")
    plt.ylabel("throughput(events/s)")
    plt.tight_layout()
    plt.savefig(layout.root / "scalability_throughput.png", dpi=160)

    if not args.skip_figures:
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "samples" / "render_thesis_figures.py"),
                "--input-dir",
                str(layout.root),
                "--out-dir",
                str(layout.figures_dir),
                "--manifest",
                str(layout.manifest_path),
            ],
            cwd=str(ROOT),
            check=False,
            timeout=120,
        )

    update_manifest(
        layout,
        {
            "metrics": {
                "resultTables": {
                    "allResults": file_record(all_results_path),
                    "parallelism": file_record(layout.root / "perf_parallelism.csv"),
                    "windows": file_record(layout.root / "perf_windows.csv"),
                    "backend": file_record(layout.root / "perf_backend.csv"),
                    "scalability": file_record(layout.root / "scalability.csv"),
                },
                "caseCount": len(results),
                "scopeValidCases": sum(1 for row in results if row["scope_valid"]),
            },
            "outputs": {
                "reproduceCommand": {"path": root_relative(layout.root / "reproduce-command.txt")},
                "figures": file_record(layout.figures_dir / "index.html") if (layout.figures_dir / "index.html").exists() else {},
            },
        },
    )
    summarize_manifest(layout)
    mirror_latest_run(
        layout,
        top_level_files=[
            all_results_path,
            layout.root / "perf_parallelism.csv",
            layout.root / "perf_windows.csv",
            layout.root / "perf_backend.csv",
            layout.root / "scalability.csv",
        ],
        include_figures=(layout.figures_dir / "index.html").exists(),
    )
    print("done", len(results), "cases", layout.run_id)


if __name__ == "__main__":
    main()
