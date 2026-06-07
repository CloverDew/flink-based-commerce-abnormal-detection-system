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
    "expected_positive_users",
    "detected_users",
    "tp_users",
    "fp_users",
    "fn_users",
    "precision",
    "recall",
    "f1",
    "macro_precision",
    "macro_recall",
    "macro_f1",
]
RULE_LABELS = {
    "ABNORMAL_LOGIN": "Abnormal Login",
    "ORDER_BRUSH": "Order Brush",
    "HIGH_FREQ_ACCESS": "High Frequency Access",
}
RULE_METRIC_FIELDNAMES = [
    "case",
    "rule_type",
    "rule_label",
    "expected_users",
    "detected_users",
    "tp_users",
    "fp_users",
    "fn_users",
    "precision",
    "recall",
    "f1",
]
LOGIN_FIXED_BUCKETS = (
    {"bucket": "fast_pos", "ratio": 0.35, "gap_ms": 18_000, "expected_positive": True, "bucket_kind": "positive"},
    {"bucket": "mid_pos", "ratio": 0.25, "gap_ms": 45_000, "expected_positive": True, "bucket_kind": "positive"},
    {"bucket": "slow_pos", "ratio": 0.20, "gap_ms": 180_000, "expected_positive": True, "bucket_kind": "positive"},
    {"bucket": "hard_neg", "ratio": 0.12, "gap_ms": 50_000, "expected_positive": False, "bucket_kind": "hard_negative"},
    {
        "bucket": "late_neg",
        "ratio": 0.08,
        "gap_ms": 330_000,
        "expected_positive": False,
        "bucket_kind": "window_edge_negative",
    },
)
REPEATED_FIXED_BUCKETS = {
    "ORDER_BRUSH": {
        "action_type": "ORDER",
        "events_per_user": 5,
        "buckets": (
            {
                "bucket": "fast_pos",
                "ratio": 0.35,
                "spacing_ms": 4_000,
                "expected_positive": True,
                "bucket_kind": "positive",
            },
            {
                "bucket": "mid_pos",
                "ratio": 0.25,
                "spacing_ms": 12_000,
                "expected_positive": True,
                "bucket_kind": "positive",
            },
            {
                "bucket": "slow_pos",
                "ratio": 0.20,
                "spacing_ms": 70_000,
                "expected_positive": True,
                "bucket_kind": "positive",
            },
            {
                "bucket": "hard_neg",
                "ratio": 0.12,
                "spacing_ms": 14_000,
                "expected_positive": False,
                "bucket_kind": "hard_negative",
            },
            {
                "bucket": "late_neg",
                "ratio": 0.08,
                "spacing_ms": 90_000,
                "expected_positive": False,
                "bucket_kind": "window_edge_negative",
            },
        ),
    },
    "HIGH_FREQ_ACCESS": {
        "action_type": "VIEW",
        "events_per_user": 100,
        "buckets": (
            {
                "bucket": "fast_pos",
                "ratio": 0.35,
                "spacing_ms": 250,
                "expected_positive": True,
                "bucket_kind": "positive",
            },
            {
                "bucket": "mid_pos",
                "ratio": 0.25,
                "spacing_ms": 450,
                "expected_positive": True,
                "bucket_kind": "positive",
            },
            {
                "bucket": "slow_pos",
                "ratio": 0.20,
                "spacing_ms": 2_500,
                "expected_positive": True,
                "bucket_kind": "positive",
            },
            {
                "bucket": "hard_neg",
                "ratio": 0.12,
                "spacing_ms": 500,
                "expected_positive": False,
                "bucket_kind": "hard_negative",
            },
            {
                "bucket": "late_neg",
                "ratio": 0.08,
                "spacing_ms": 3_800,
                "expected_positive": False,
                "bucket_kind": "window_edge_negative",
            },
        ),
    },
}


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


def precision_recall_f1(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    if precision + recall <= 0:
        return precision, recall, 0.0
    return precision, recall, 2 * precision * recall / (precision + recall)


def alert_lag_ms(alert: dict[str, object], *, window_ms: int) -> float | None:
    cap = float(max(120_000, int(window_ms) * 5))
    processing_lag = alert.get("processingLagMs")
    if processing_lag is not None and processing_lag != "":
        try:
            return max(0.0, min(float(processing_lag), cap))
        except (TypeError, ValueError):
            pass
    alert_ts = alert.get("alertTimestamp")
    last_event_ts = alert.get("lastEventTimestamp")
    if alert_ts is None or last_event_ts is None:
        return None
    try:
        return max(0.0, min(float(alert_ts) - float(last_event_ts), cap))
    except (TypeError, ValueError):
        return None


def dedupe_alerts_by_rule_and_user(alerts: list[dict[str, object]], *, window_ms: int) -> list[dict[str, object]]:
    best: dict[tuple[str, str], dict[str, object]] = {}
    best_rank: dict[tuple[str, str], float] = {}
    for alert in alerts:
        rule_type = str(alert.get("ruleType", "")).strip()
        user_id = str(alert.get("userId", "")).strip()
        if not rule_type or not user_id:
            continue
        key = (rule_type, user_id)
        lag = alert_lag_ms(alert, window_ms=window_ms)
        rank = lag if lag is not None else float("inf")
        if key not in best or rank < best_rank.get(key, float("inf")):
            best[key] = alert
            best_rank[key] = rank
    return list(best.values())


def allocate_counts(total: int, ratios: list[float]) -> list[int]:
    if total <= 0 or not ratios:
        return [0 for _ in ratios]
    scaled = [max(0.0, ratio) * total for ratio in ratios]
    counts = [int(value) for value in scaled]
    remainder = total - sum(counts)
    order = sorted(range(len(ratios)), key=lambda idx: scaled[idx] - counts[idx], reverse=True)
    for idx in order[:remainder]:
        counts[idx] += 1
    return counts


def background_event(action_type: str, user_id: str, timestamp: int, *, idx: int) -> dict[str, object]:
    event: dict[str, object] = {
        "userId": user_id,
        "actionType": action_type,
        "ip": f"172.16.{idx % 16}.{idx % 250 + 1}",
        "timestamp": timestamp,
        "sessionId": f"bg-{idx}",
    }
    if action_type in {"VIEW", "ORDER", "CART"}:
        event["productId"] = f"bgp-{idx % 1000}"
    if action_type == "ORDER":
        event["amount"] = round(15 + (idx % 80) * 2.25, 2)
    return event


def compute_case_quality_metrics(
    case_name: str,
    scoped_alerts: list[dict[str, object]],
    *,
    expected_users_by_rule_type: dict[str, set[str]],
    window_ms: int,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    deduped = dedupe_alerts_by_rule_and_user(scoped_alerts, window_ms=window_ms)
    detected_by_rule_type: dict[str, set[str]] = {rule_type: set() for rule_type in RULE_LABELS}
    for alert in deduped:
        rule_type = str(alert.get("ruleType", "")).strip()
        user_id = str(alert.get("userId", "")).strip()
        if rule_type in detected_by_rule_type and user_id:
            detected_by_rule_type[rule_type].add(user_id)

    rule_rows: list[dict[str, object]] = []
    tp_total = 0
    fp_total = 0
    fn_total = 0
    macro_precision_values: list[float] = []
    macro_recall_values: list[float] = []
    macro_f1_values: list[float] = []
    detected_total = 0

    for rule_type, label in RULE_LABELS.items():
        expected = expected_users_by_rule_type.get(rule_type, set())
        detected = detected_by_rule_type.get(rule_type, set())
        tp = len(expected & detected)
        fp = len(detected - expected)
        fn = len(expected - detected)
        precision, recall, f1 = precision_recall_f1(tp, fp, fn)
        tp_total += tp
        fp_total += fp
        fn_total += fn
        detected_total += len(detected)
        macro_precision_values.append(precision)
        macro_recall_values.append(recall)
        macro_f1_values.append(f1)
        rule_rows.append(
            {
                "case": case_name,
                "rule_type": rule_type,
                "rule_label": label,
                "expected_users": len(expected),
                "detected_users": len(detected),
                "tp_users": tp,
                "fp_users": fp,
                "fn_users": fn,
                "precision": round(precision, 6),
                "recall": round(recall, 6),
                "f1": round(f1, 6),
            }
        )

    micro_precision, micro_recall, micro_f1 = precision_recall_f1(tp_total, fp_total, fn_total)
    macro_precision = sum(macro_precision_values) / len(macro_precision_values) if macro_precision_values else 0.0
    macro_recall = sum(macro_recall_values) / len(macro_recall_values) if macro_recall_values else 0.0
    macro_f1 = sum(macro_f1_values) / len(macro_f1_values) if macro_f1_values else 0.0
    return (
        {
            "expected_positive_users": sum(len(users) for users in expected_users_by_rule_type.values()),
            "detected_users": detected_total,
            "tp_users": tp_total,
            "fp_users": fp_total,
            "fn_users": fn_total,
            "precision": round(micro_precision, 6),
            "recall": round(micro_recall, 6),
            "f1": round(micro_f1, 6),
            "macro_precision": round(macro_precision, 6),
            "macro_recall": round(macro_recall, 6),
            "macro_f1": round(macro_f1, 6),
        },
        rule_rows,
    )


def generate_rules_and_events(case_dir: Path, *, case_name: str, window_ms: int, event_count: int) -> dict[str, object]:
    version = int(time.time() * 1000)
    rules = case_rule_payloads(case_name, window_ms, version)
    rules_path = case_dir / "rules.json"
    rules_path.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
    events: list[dict[str, object]] = []
    expected_users_by_rule_type: dict[str, set[str]] = {rule_type: set() for rule_type in RULE_LABELS}
    negative_users_by_rule_type: dict[str, set[str]] = {rule_type: set() for rule_type in RULE_LABELS}
    per_pattern_user_budget = 107
    group_n = max(1, min(event_count // per_pattern_user_budget, 8000))
    background_count = max(0, event_count - group_n * per_pattern_user_budget)
    token = topic_token(case_name)
    user_seq = {rule_type: 0 for rule_type in RULE_LABELS}
    bucket_summary: dict[str, list[dict[str, object]]] = {rule_type: [] for rule_type in RULE_LABELS}
    max_offset = 0
    phases = {
        "ABNORMAL_LOGIN": 0,
        "ORDER_BRUSH": 120_000,
        "HIGH_FREQ_ACCESS": 360_000,
        "BACKGROUND": 780_000,
    }
    staggers = {
        "ABNORMAL_LOGIN": 45,
        "ORDER_BRUSH": 65,
        "HIGH_FREQ_ACCESS": 85,
        "BACKGROUND": 30,
    }

    def append_login_bucket(
        bucket_name: str,
        count: int,
        gap_ms: int,
        *,
        expected_positive: bool,
        bucket_kind: str,
    ) -> None:
        nonlocal max_offset
        bucket_summary["ABNORMAL_LOGIN"].append(
            {
                "bucket": bucket_name,
                "bucket_kind": bucket_kind,
                "count": count,
                "gap_ms": gap_ms,
                "expected_positive": expected_positive,
            }
        )
        for _ in range(count):
            idx = user_seq["ABNORMAL_LOGIN"]
            user_seq["ABNORMAL_LOGIN"] += 1
            user_id = f"al_{'pos' if expected_positive else 'neg'}_{token}_{bucket_name}_{idx}"
            base = phases["ABNORMAL_LOGIN"] + idx * staggers["ABNORMAL_LOGIN"]
            events.append(
                {
                    "userId": user_id,
                    "actionType": "LOGIN",
                    "ip": f"10.0.0.{idx % 250 + 1}",
                    "timestamp": base,
                    "sessionId": f"als-{idx}",
                }
            )
            events.append(
                {
                    "userId": user_id,
                    "actionType": "CHANGE_PASSWORD",
                    "ip": f"10.0.1.{idx % 250 + 1}",
                    "timestamp": base + gap_ms,
                    "sessionId": f"als-{idx}",
                }
            )
            if expected_positive:
                expected_users_by_rule_type["ABNORMAL_LOGIN"].add(user_id)
            else:
                negative_users_by_rule_type["ABNORMAL_LOGIN"].add(user_id)
            max_offset = max(max_offset, base + gap_ms)

    def append_repeated_bucket(
        rule_type: str,
        bucket_name: str,
        count: int,
        action_type: str,
        per_user_events: int,
        spacing_ms: int,
        expected_positive: bool,
        bucket_kind: str,
    ) -> None:
        nonlocal max_offset
        prefix = "ob" if rule_type == "ORDER_BRUSH" else "hf"
        ip_prefix = "10.1.0" if rule_type == "ORDER_BRUSH" else "10.2.0"
        session_prefix = "obs" if rule_type == "ORDER_BRUSH" else "hfs"
        product_prefix = "p" if rule_type == "ORDER_BRUSH" else "pv"
        bucket_summary[rule_type].append(
            {
                "bucket": bucket_name,
                "bucket_kind": bucket_kind,
                "count": count,
                "spacing_ms": spacing_ms,
                "events_per_user": per_user_events,
                "expected_positive": expected_positive,
            }
        )
        for _ in range(count):
            idx = user_seq[rule_type]
            user_seq[rule_type] += 1
            user_id = f"{prefix}_{'pos' if expected_positive else 'neg'}_{token}_{bucket_name}_{idx}"
            base = phases[rule_type] + idx * staggers[rule_type]
            for event_idx in range(per_user_events):
                event = {
                    "userId": user_id,
                    "actionType": action_type,
                    "ip": f"{ip_prefix}.{idx % 250 + 1}",
                    "timestamp": base + event_idx * spacing_ms,
                    "sessionId": f"{session_prefix}-{idx}",
                    "productId": f"{product_prefix}{event_idx % 20}",
                }
                if action_type == "ORDER":
                    event["amount"] = 99.9 + event_idx
                events.append(event)
            if expected_positive:
                expected_users_by_rule_type[rule_type].add(user_id)
            else:
                negative_users_by_rule_type[rule_type].add(user_id)
            max_offset = max(max_offset, base + (per_user_events - 1) * spacing_ms)

    login_counts = allocate_counts(group_n, [bucket["ratio"] for bucket in LOGIN_FIXED_BUCKETS])
    for bucket_cfg, count in zip(LOGIN_FIXED_BUCKETS, login_counts):
        append_login_bucket(
            str(bucket_cfg["bucket"]),
            count,
            gap_ms=int(bucket_cfg["gap_ms"]),
            expected_positive=bool(bucket_cfg["expected_positive"]),
            bucket_kind=str(bucket_cfg["bucket_kind"]),
        )

    for rule_type, config in REPEATED_FIXED_BUCKETS.items():
        bucket_counts = allocate_counts(group_n, [bucket["ratio"] for bucket in config["buckets"]])
        for bucket_cfg, count in zip(config["buckets"], bucket_counts):
            append_repeated_bucket(
                rule_type,
                str(bucket_cfg["bucket"]),
                count,
                str(config["action_type"]),
                int(config["events_per_user"]),
                int(bucket_cfg["spacing_ms"]),
                bool(bucket_cfg["expected_positive"]),
                str(bucket_cfg["bucket_kind"]),
            )

    background_actions = ["SEARCH", "CLICK", "VIEW", "ORDER", "CART"]
    for idx in range(background_count):
        action_type = background_actions[idx % len(background_actions)]
        user_id = f"bg_{token}_{idx}"
        timestamp = phases["BACKGROUND"] + idx * staggers["BACKGROUND"]
        events.append(background_event(action_type, user_id, timestamp, idx=idx))
        max_offset = max(max_offset, timestamp)

    start_ts = int(time.time() * 1000) - max_offset - 30_000
    for event in events:
        event["timestamp"] = int(event["timestamp"]) + start_ts

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
            "fixedTimingTiers": True,
            "requestedEvents": event_count,
            "actualEvents": len(events),
            "cohortUsersPerPattern": group_n,
            "backgroundEvents": background_count,
            "positiveUsersByRuleType": {rule_type: len(users) for rule_type, users in expected_users_by_rule_type.items()},
            "negativeUsersByRuleType": {rule_type: len(users) for rule_type, users in negative_users_by_rule_type.items()},
            "patterns": bucket_summary,
        },
        "expected_users_by_rule_type": expected_users_by_rule_type,
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


def capture_alerts(
    case_dir: Path,
    *,
    case_name: str,
    alert_topic: str,
    parallelism: int = 1,
) -> tuple[Path, list[dict[str, object]]]:
    out_file = case_dir / f"alerts-{case_name}.jsonl"
    group_id = f"exp-capture-{topic_token(case_name)}-{int(time.time() * 1000)}"
    # Higher parallelism runs need longer drain time before the consumer times out.
    consumer_timeout_ms = min(90_000, 28_000 + max(1, parallelism) * 8_000)
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
            str(consumer_timeout_ms),
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


def write_rows(path: Path, rows: list[dict[str, object]], *, fieldnames: list[str] = FIELDNAMES) -> list[dict[str, object]]:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def run_case(layout, *, name: str, parallelism: int, window_ms: int, backend: str, event_count: int) -> tuple[dict[str, object], list[dict[str, object]]]:
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
    alerts_path, raw_alerts = capture_alerts(
        case_dir,
        case_name=name,
        alert_topic=topics["alert_topic"],
        parallelism=parallelism,
    )
    scoped_alerts, unexpected_alerts = filter_alert_scope(raw_alerts, set(generated["rule_ids"]))
    avg_lat, p99_lat = latency_metrics(scoped_alerts, window_ms=window_ms)
    quality_summary, rule_metrics = compute_case_quality_metrics(
        name,
        scoped_alerts,
        expected_users_by_rule_type=generated["expected_users_by_rule_type"],
        window_ms=window_ms,
    )
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
        **quality_summary,
    }
    rule_metrics_path = case_dir / "rule_metrics.csv"
    write_rows(rule_metrics_path, rule_metrics, fieldnames=RULE_METRIC_FIELDNAMES)
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
            "quality": {
                "summary": quality_summary,
                "ruleMetrics": rule_metrics,
            },
            "files": {
                "rules": file_record(rules_path),
                "events": file_record(events_path),
                "alerts": file_record(alerts_path),
                "ruleMetrics": file_record(rule_metrics_path),
            },
            "unexpectedRuleIds": sorted({str(a.get("ruleId", "")) for a in unexpected_alerts if a.get("ruleId")}),
            "generatedAt": now_iso_utc(),
        },
    )
    return result, rule_metrics


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
    all_rule_metrics: list[dict[str, object]] = []
    for case in planned_cases:
        result, rule_rows = run_case(
            layout,
            name=case["name"],
            parallelism=case["parallelism"],
            window_ms=case["window_ms"],
            backend=case["backend"],
            event_count=case["event_count"],
        )
        results.append(result)
        all_rule_metrics.extend(rule_rows)

    all_results_path = layout.root / "all_results.csv"
    write_rows(all_results_path, results)
    rule_metrics_path = layout.root / "rule_metrics.csv"
    write_rows(rule_metrics_path, all_rule_metrics, fieldnames=RULE_METRIC_FIELDNAMES)
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
                    "ruleMetrics": file_record(rule_metrics_path),
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
    top_level_files = [
        all_results_path,
        rule_metrics_path,
        layout.root / "perf_parallelism.csv",
        layout.root / "perf_windows.csv",
        layout.root / "perf_backend.csv",
        layout.root / "scalability.csv",
    ]
    functional_metrics_path = layout.root / "functional_metrics.csv"
    if functional_metrics_path.exists():
        top_level_files.append(functional_metrics_path)
    mirror_latest_run(
        layout,
        top_level_files=top_level_files,
        include_figures=(layout.figures_dir / "index.html").exists(),
    )
    print("done", len(results), "cases", layout.run_id)


if __name__ == "__main__":
    main()
