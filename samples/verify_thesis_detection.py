import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
EXP_DIR = ROOT / ".data" / "experiment"
EXP_DIR.mkdir(parents=True, exist_ok=True)


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


def parse_args():
    parser = argparse.ArgumentParser(description="Publish rules/events and verify abnormal detection hits.")
    parser.add_argument("--rules", default="samples/thesis-risk-rules.json", help="Input rules json file.")
    parser.add_argument("--events", default="samples/thesis-behavior-events.jsonl", help="Input events jsonl file.")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap for Maven publishers.")
    parser.add_argument("--rule-topic", default="risk-rules", help="Kafka topic for risk rules.")
    parser.add_argument("--event-topic", default="user-behavior", help="Kafka topic for behavior events.")
    parser.add_argument("--alert-topic", default="alerts", help="Kafka topic for alerts.")
    parser.add_argument(
        "--kafka-container",
        default="flink-based-commerce-abnormal-detection-system-kafka-1",
        help="Kafka container name for consuming alerts via docker exec.",
    )
    parser.add_argument("--consume-timeout-ms", type=int, default=25000, help="Kafka console consumer timeout.")
    parser.add_argument("--publish-settle-seconds", type=int, default=8, help="Wait after event publish before consume.")
    parser.add_argument("--summary-out", default="", help="Optional output json path for summary report.")
    return parser.parse_args()


def create_run_scoped_rules(src_rules: Path) -> Tuple[Path, Dict[str, str], str]:
    now_ms = int(time.time() * 1000)
    run_tag = f"run{now_ms}"
    rules = json.loads(src_rules.read_text(encoding="utf-8"))
    rule_id_map: Dict[str, str] = {}

    for i, rule in enumerate(rules):
        old_id = str(rule.get("ruleId", f"rule-{i}"))
        new_id = f"{old_id}-{run_tag}"
        rule_id_map[old_id] = new_id
        rule["ruleId"] = new_id
        rule["ruleName"] = f"{rule.get('ruleName', old_id)}-{run_tag}"
        rule["version"] = int(rule.get("version", 1)) + (now_ms % 100000)
        rule["updateTimestamp"] = now_ms

    out_path = EXP_DIR / f"thesis-risk-rules-{run_tag}.json"
    out_path.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path, rule_id_map, run_tag


def publish_rules(rules_path: Path, kafka_bootstrap: str, rule_topic: str):
    run(
        'mvn "exec:java" "-Dexec.mainClass=cn.edu.ustb.detection.tools.JsonFileKafkaPublisher" '
        f'"-Dexec.args=--input {rules_path.as_posix()} --kafka-bootstrap {kafka_bootstrap} --kafka-topic {rule_topic} --key-field ruleId"',
        timeout=900,
    )


def publish_events(events_path: Path, kafka_bootstrap: str, event_topic: str):
    run(
        'mvn "exec:java" "-Dexec.mainClass=cn.edu.ustb.detection.tools.JsonFileKafkaPublisher" '
        f'"-Dexec.args=--input {events_path.as_posix()} --kafka-bootstrap {kafka_bootstrap} --kafka-topic {event_topic} --key-field userId"',
        timeout=1800,
    )


def consume_alerts(kafka_container: str, alert_topic: str, timeout_ms: int, run_tag: str) -> List[dict]:
    out_file = EXP_DIR / f"alerts-verify-{run_tag}.jsonl"
    run(
        f"docker exec {kafka_container} kafka-console-consumer --bootstrap-server kafka:9092 "
        f"--topic {alert_topic} --from-beginning --timeout-ms {timeout_ms} > {out_file.as_posix()}",
        timeout=max(120, timeout_ms // 1000 + 30),
    )

    rows = []
    text = out_file.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def summarize_alerts(alerts: List[dict], scoped_rule_ids: List[str]) -> dict:
    scoped = [a for a in alerts if str(a.get("ruleId", "")) in scoped_rule_ids]
    by_rule: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    users = set()

    for row in scoped:
        rule_id = str(row.get("ruleId", "unknown"))
        rule_type = str(row.get("ruleType", "unknown"))
        by_rule[rule_id] = by_rule.get(rule_id, 0) + 1
        by_type[rule_type] = by_type.get(rule_type, 0) + 1
        if row.get("userId"):
            users.add(str(row["userId"]))

    return {
        "alerts_scoped_total": len(scoped),
        "alerts_by_rule_id": by_rule,
        "alerts_by_rule_type": by_type,
        "affected_users": len(users),
        "recognized_types": sorted(by_type.keys()),
    }


def main():
    args = parse_args()
    rules_path = Path(args.rules)
    events_path = Path(args.events)
    if not rules_path.exists():
        raise FileNotFoundError(f"rules file not found: {rules_path}")
    if not events_path.exists():
        raise FileNotFoundError(f"events file not found: {events_path}")

    scoped_rules_path, _, run_tag = create_run_scoped_rules(rules_path)
    scoped_rule_ids = [r["ruleId"] for r in json.loads(scoped_rules_path.read_text(encoding="utf-8"))]

    t0 = time.time()
    publish_rules(scoped_rules_path, args.kafka_bootstrap, args.rule_topic)
    publish_events(events_path, args.kafka_bootstrap, args.event_topic)
    time.sleep(args.publish_settle_seconds)
    alerts = consume_alerts(args.kafka_container, args.alert_topic, args.consume_timeout_ms, run_tag)
    summary = summarize_alerts(alerts, scoped_rule_ids)
    summary["run_tag"] = run_tag
    summary["rules_file"] = str(scoped_rules_path.as_posix())
    summary["events_file"] = str(events_path.as_posix())
    summary["elapsed_seconds"] = round(time.time() - t0, 3)

    out_path = Path(args.summary_out) if args.summary_out else EXP_DIR / f"verify-summary-{run_tag}.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"summary saved: {out_path.as_posix()}")


if __name__ == "__main__":
    main()
