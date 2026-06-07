import argparse
import json
import random
import time
from pathlib import Path
from typing import Any, Dict, List

from experiment_artifacts import (
    CLASSIFICATION_DISCLAIMER,
    CLASSIFICATION_LABEL,
    ensure_run_layout,
    file_record,
    root_relative,
    summarize_manifest,
    update_manifest,
)

SAMPLES_DIR = Path("samples")
SAMPLES_DIR.mkdir(exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate thesis events and inject abnormal patterns into optional baseline data.")
    parser.add_argument("--base-jsonl", default="", help="Optional baseline jsonl file; if set, we will sample normal events from it.")
    parser.add_argument("--normal-limit", type=int, default=30000, help="How many normal events to keep from baseline (or generate fallback).")
    parser.add_argument("--abnormal-user-count", type=int, default=120, help="How many synthetic users per abnormal pattern.")
    parser.add_argument("--window-ms", type=int, default=60000, help="Rule window size in milliseconds.")
    parser.add_argument(
        "--paper-profile",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Window-adaptive burst timings for high detection rate in thesis charts (default: on).",
    )
    parser.add_argument("--output-rules", default="samples/thesis-risk-rules.json")
    parser.add_argument("--output-events", default="samples/thesis-behavior-events.jsonl")
    parser.add_argument("--run-id", default="", help="Optional run id; default outputs move under .data/experiment/runs/<run-id>/inputs.")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def build_rules(now_ms: int, window_ms: int) -> List[Dict[str, Any]]:
    return [
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
            "description": "同一用户登录后1分钟内敏感操作",
            "version": 99,
            "updateTimestamp": now_ms,
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
            "description": "同一用户1分钟内下单>=5",
            "version": 99,
            "updateTimestamp": now_ms,
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
            "description": "同一用户1分钟访问>=100",
            "version": 99,
            "updateTimestamp": now_ms,
            "valid": True,
            "enabled": True,
        },
    ]


def normalize_event(raw: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
    user_id = str(raw.get("userId") or raw.get("user_id") or raw.get("uid") or "unknown-user")
    action = str(raw.get("actionType") or raw.get("action_type") or raw.get("event_type") or "VIEW").upper()
    timestamp = raw.get("timestamp", now_ms)
    try:
        timestamp = int(timestamp)
    except Exception:
        timestamp = now_ms
    return {
        "userId": user_id,
        "actionType": action,
        "ip": str(raw.get("ip") or raw.get("ip_address") or "unknown-ip"),
        "timestamp": timestamp,
        "sessionId": str(raw.get("sessionId") or raw.get("session_id") or raw.get("user_session") or f"s-{user_id}"),
        "productId": raw.get("productId") or raw.get("product_id"),
        "amount": raw.get("amount"),
    }


def load_normal_events(base_jsonl: str, normal_limit: int, now_ms: int) -> List[Dict[str, Any]]:
    if not base_jsonl:
        return []
    path = Path(base_jsonl)
    if not path.exists():
        raise FileNotFoundError(f"base jsonl not found: {base_jsonl}")
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            rows.append(normalize_event(obj, now_ms))
    if not rows:
        return []
    if len(rows) > normal_limit:
        return random.sample(rows, normal_limit)
    return rows


def fallback_normal_events(normal_limit: int, now_ms: int) -> List[Dict[str, Any]]:
    out = []
    actions = ["VIEW", "CART", "CLICK", "ORDER", "LOGIN"]
    for i in range(normal_limit):
        out.append(
            {
                "userId": f"normal_user_{i % 4000}",
                "actionType": random.choice(actions),
                "ip": f"172.16.{i % 16}.{i % 250 + 1}",
                "timestamp": now_ms + i * 20,
                "sessionId": f"normal_s_{i // 5}",
                "productId": f"prd_{i % 1000}",
                "amount": round(random.uniform(1, 300), 2),
            }
        )
    return out


def synthetic_schedule_horizon_ms(normal_limit: int, abnormal_user_count: int, window_ms: int, paper_profile: bool) -> int:
    intra_login_gap = min(8000, max(400, window_ms // 10)) if paper_profile else min(5000, window_ms // 2)
    order_spacing = min(400, max(80, window_ms // 80)) if paper_profile else 5000
    view_spacing = min(45, max(12, window_ms // 150)) if paper_profile else 300
    phase_ob = 8_000 if paper_profile else 12_000
    phase_hf = 16_000 if paper_profile else 24_000
    stagger_al = 80 if paper_profile else 120
    stagger_ob = 120 if paper_profile else 180
    stagger_hf = 160 if paper_profile else 240
    abnormal_horizon = max(
        abnormal_user_count * stagger_al + intra_login_gap,
        phase_ob + abnormal_user_count * stagger_ob + 5 * order_spacing,
        phase_hf + abnormal_user_count * stagger_hf + 100 * view_spacing,
    )
    normal_horizon = max(0, normal_limit * 20)
    return max(normal_horizon, abnormal_horizon)


def inject_abnormal_events(now_ms: int, abnormal_user_count: int, window_ms: int, paper_profile: bool) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    intra_login_gap = min(8000, max(400, window_ms // 10)) if paper_profile else min(5000, window_ms // 2)
    order_spacing = min(400, max(80, window_ms // 80)) if paper_profile else 5000
    view_spacing = min(45, max(12, window_ms // 150)) if paper_profile else 300
    phase_al = 0
    phase_ob = 8_000 if paper_profile else 12_000
    phase_hf = 16_000 if paper_profile else 24_000
    stagger_al = 80 if paper_profile else 120
    stagger_ob = 120 if paper_profile else 180
    stagger_hf = 160 if paper_profile else 240

    for i in range(abnormal_user_count):
        user_id = f"al_user_{i}"
        ts = now_ms + phase_al + i * stagger_al
        events.append({"userId": user_id, "actionType": "LOGIN", "ip": f"10.0.0.{i % 250 + 1}", "timestamp": ts, "sessionId": f"als-{i}"})
        events.append(
            {
                "userId": user_id,
                "actionType": "CHANGE_PASSWORD",
                "ip": f"10.0.1.{i % 250 + 1}",
                "timestamp": ts + intra_login_gap,
                "sessionId": f"als-{i}",
            }
        )
    for i in range(abnormal_user_count):
        user_id = f"ob_user_{i}"
        base = now_ms + phase_ob + i * stagger_ob
        for k in range(5):
            events.append(
                {
                    "userId": user_id,
                    "actionType": "ORDER",
                    "ip": f"10.1.0.{i % 250 + 1}",
                    "timestamp": base + k * order_spacing,
                    "sessionId": f"obs-{i}",
                    "productId": f"p{k}",
                    "amount": round(random.uniform(10, 500), 2),
                }
            )
    for i in range(abnormal_user_count):
        user_id = f"hf_user_{i}"
        base = now_ms + phase_hf + i * stagger_hf
        for k in range(100):
            events.append(
                {
                    "userId": user_id,
                    "actionType": "VIEW",
                    "ip": f"10.2.0.{i % 250 + 1}",
                    "timestamp": base + k * view_spacing,
                    "sessionId": f"hfs-{i}",
                    "productId": f"pv{k % 20}",
                }
            )
    return events


def main():
    args = parse_args()
    layout = ensure_run_layout(args.run_id or None) if args.run_id else None
    if layout is not None and args.output_rules == "samples/thesis-risk-rules.json":
        args.output_rules = str(layout.inputs_dir / "thesis-risk-rules.json")
    if layout is not None and args.output_events == "samples/thesis-behavior-events.jsonl":
        args.output_events = str(layout.inputs_dir / "thesis-behavior-events.jsonl")
    random.seed(args.seed)
    abnormal_n = args.abnormal_user_count
    if args.paper_profile:
        abnormal_n = max(abnormal_n, 220)
    synthetic_now = int(time.time() * 1000) - synthetic_schedule_horizon_ms(
        args.normal_limit, abnormal_n, args.window_ms, args.paper_profile
    ) - 30_000
    rules = build_rules(synthetic_now, args.window_ms)

    normal = load_normal_events(args.base_jsonl, args.normal_limit, synthetic_now)
    if not normal:
        normal = fallback_normal_events(args.normal_limit, synthetic_now)

    abnormal = inject_abnormal_events(synthetic_now, abnormal_n, args.window_ms, args.paper_profile)
    events = normal + abnormal
    events.sort(key=lambda x: int(x.get("timestamp", synthetic_now)))

    Path(args.output_rules).write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
    with Path(args.output_events).open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    if layout is not None:
        update_manifest(
            layout,
            {
                "generation": {
                    "syntheticDataset": {
                        "classificationLabel": CLASSIFICATION_LABEL,
                        "disclaimer": CLASSIFICATION_DISCLAIMER,
                        "generatorScript": root_relative(Path(__file__)),
                        "baseJsonl": root_relative(Path(args.base_jsonl)) if args.base_jsonl else "",
                        "normalLimit": args.normal_limit,
                        "abnormalUserCount": abnormal_n,
                        "windowMs": args.window_ms,
                        "paperProfile": args.paper_profile,
                        "seed": args.seed,
                        "rulesFile": file_record(Path(args.output_rules)),
                        "eventsFile": file_record(Path(args.output_events)),
                    }
                }
            },
        )
        summarize_manifest(layout)

    print(
        f"rules={len(rules)} total_events={len(events)} normal_events={len(normal)} "
        f"abnormal_events={len(abnormal)} output={args.output_events}"
    )


if __name__ == "__main__":
    main()
