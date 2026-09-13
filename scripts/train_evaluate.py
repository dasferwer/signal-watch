"""Обучить демонстрационную модель и честно сохранить результат на позднем периоде."""

import argparse
import hashlib
import json
import platform
import time
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import joblib
import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    auc,
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    precision_score,
    recall_score,
)

from signalwatch.detector import (
    RULE_LIMITS,
    RULE_VERSION,
    Detector,
    drift_reference,
    manifest_identity,
    rule_score,
    select_policy,
    threshold_at_fpr,
    transform,
)
from signalwatch.features import (
    ALLOWED_LATENESS_SECONDS,
    FEATURE_NAMES,
    FEATURE_VERSION,
    WINDOW_SECONDS,
    extract_features,
)

ROOT = Path(__file__).resolve().parents[1]
SEED = 20260913
START = 1767225600.0
GENERATOR_VERSION = "synthetic-event-stream-v1"
FPR_BUDGET = 0.01


def save_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def save_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
            for row in rows
        )
    )


def checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generate_period(
    split: str, start: float, duration: int, normal_count: int, seed: int, attacks: bool
) -> list[dict]:
    rng = np.random.default_rng(seed)
    weights = rng.lognormal(0, 0.55, 90)
    weights /= weights.sum()
    countries = ["US", "GB", "DE", "FR", "CA"]
    rows = []

    def append(moment, actor, ip, country, kind, success, bytes_sent, label, scenario):
        event = {
            "event_id": str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL, f"signalwatch/{GENERATOR_VERSION}/{split}/{len(rows)}"
                )
            ),
            "occurred_at": round(float(moment), 6),
            "actor_id": actor,
            "source_ip": ip,
            "country": country,
            "kind": kind,
            "success": bool(success),
            "bytes_sent": int(bytes_sent),
        }
        rows.append({"event": event, "label": label, "scenario": scenario})

    for moment in np.sort(rng.uniform(start, start + duration, normal_count)):
        actor = int(rng.choice(90, p=weights))
        kind = str(rng.choice(["api", "login", "signup"], p=[0.925, 0.065, 0.01]))
        success = bool(rng.random() > (0.05 if kind == "login" else 0.018))
        country = countries[actor % len(countries)]
        if rng.random() < 0.003:
            country = str(rng.choice(countries))
        # Несколько обычных клиентов сидят за общим NAT, а некоторые часто скачивают файлы.
        address = 100 if actor < 12 else actor // 3 + 1
        volume = min(300_000, int(rng.lognormal(7.4, 1.0))) if success and kind == "api" else 0
        if actor % 19 == 0:
            volume *= 7
        append(
            moment,
            f"customer-{actor:03}",
            f"198.51.100.{address}",
            country,
            kind,
            success,
            volume,
            0,
            "normal",
        )

    if attacks:
        base = start + duration * 0.18
        for index in range(32):
            append(
                base + index * 2.5,
                f"guessing-{split}",
                "203.0.113.20",
                countries[index % 3],
                "login",
                False,
                0,
                1,
                "password_guessing",
            )
        base = start + duration * 0.43
        for index in range(48):
            append(
                base + index * 0.3,
                f"scraper-{split}",
                "203.0.113.30",
                "US",
                "api",
                True,
                int(rng.integers(8_000, 30_000)),
                1,
                "api_burst",
            )
        base = start + duration * 0.67
        for index in range(28):
            append(
                base + index * 0.6,
                f"signup-{split}-{index:02}",
                "203.0.113.40",
                "GB",
                "signup",
                True,
                0,
                1,
                "signup_farm",
            )
        for index in range(12):
            # Намерение вредоносное по сценарию, но эти запросы выглядят как обычные: модель может их пропустить.
            append(
                start + duration * (0.08 + index * 0.075),
                "customer-075",
                "198.51.100.26",
                "US",
                "api",
                True,
                int(rng.integers(500, 4_000)),
                1,
                "low_and_slow",
            )
    return sorted(rows, key=lambda row: (row["event"]["occurred_at"], row["event"]["event_id"]))


def materialize_features(rows: list[dict]) -> list[dict]:
    history, result = [], []
    for row in rows:
        event = row["event"]
        history = [
            item for item in history if item["occurred_at"] > event["occurred_at"] - WINDOW_SECONDS
        ]
        result.append(extract_features(history, event))
        history.append(event)
    return result


def score_metrics(labels: np.ndarray, scores: np.ndarray, threshold: float) -> dict:
    predictions = scores > threshold
    tn, fp, fn, tp = confusion_matrix(labels, predictions, labels=[0, 1]).ravel()
    precision, recall, _ = precision_recall_curve(labels, scores)
    return {
        "threshold": float(threshold),
        "precision": float(precision_score(labels, predictions, zero_division=0)),
        "recall": float(recall_score(labels, predictions, zero_division=0)),
        "false_positive_rate": float(fp / (tn + fp)),
        "pr_auc": float(auc(recall, precision)),
        "average_precision": float(average_precision_score(labels, scores)),
        "true_positive": int(tp),
        "false_positive": int(fp),
        "true_negative": int(tn),
        "false_negative": int(fn),
        "event_count": len(labels),
        "positive_count": int(labels.sum()),
        "positive_prevalence": float(labels.mean()),
    }


def scoring_latency(detector: Detector, rows: list[dict]) -> dict:
    selected = rows[:256]
    for row in selected[:10]:
        detector.evaluate(row)
    methods = {
        "rules": lambda row: rule_score(row),
        "model": lambda row: detector.model.score_samples(transform([row])),
        "combined_detector": lambda row: detector.evaluate(row),
    }
    result = {}
    for name, function in methods.items():
        times = []
        for row in selected:
            start = time.perf_counter_ns()
            function(row)
            times.append((time.perf_counter_ns() - start) / 1_000_000)
        result[name] = {
            "p50_ms": float(np.percentile(times, 50)),
            "p95_ms": float(np.percentile(times, 95)),
            "samples": len(times),
        }
    return result


def write_demo(directory: Path) -> None:
    rows = generate_period("demo", START + 3 * 86400, 1200, 880, SEED + 3, attacks=True)
    events = [row["event"] for row in rows]
    last = events[-1]["occurred_at"]
    accepted_late = {
        **events[-1],
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, "signalwatch/demo/accepted-late")),
        "occurred_at": last - 10,
    }
    rejected_late = {
        **events[-1],
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, "signalwatch/demo/rejected-late")),
        "occurred_at": last - 80,
    }
    events.extend([accepted_late, rejected_late, events[0]])
    save_jsonl(directory / "demo_events.jsonl", events)
    save_json(
        directory / "demo_manifest.json",
        {
            "generator_version": GENERATOR_VERSION,
            "event_count": len(events),
            "unique_event_count": len({event["event_id"] for event in events}),
            "normal_and_attack_stream_events": len(rows),
            "scenario_counts": dict(Counter(row["scenario"] for row in rows)),
            "accepted_late_event_id": accepted_late["event_id"],
            "rejected_late_event_id": rejected_late["event_id"],
            "duplicate_event_id": events[0]["event_id"],
            "scenarios_by_event_id": {
                row["event"]["event_id"]: row["scenario"] for row in rows if row["label"]
            },
            "note": "This short stream intentionally contains more attacks than the evaluation splits and includes arrival-order examples. It is for API replay, not quality measurement.",
            "sha256": checksum(directory / "demo_events.jsonl"),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    args = parser.parse_args()
    started = datetime.now(UTC)
    periods = {
        "train": generate_period("train", START, 3600, 10_000, SEED, attacks=False),
        "dev": generate_period("dev", START + 86400, 2400, 6_000, SEED + 1, attacks=True),
        "test": generate_period("test", START + 2 * 86400, 2200, 6_000, SEED + 2, attacks=True),
    }
    assert max(row["event"]["occurred_at"] for row in periods["train"]) < min(
        row["event"]["occurred_at"] for row in periods["dev"]
    )
    assert max(row["event"]["occurred_at"] for row in periods["dev"]) < min(
        row["event"]["occurred_at"] for row in periods["test"]
    )
    assert all(row["label"] == 0 for row in periods["train"])
    features, metadata = {}, {}
    for name, rows in periods.items():
        path = args.data_dir / f"{name}_events.jsonl"
        save_jsonl(path, rows)
        features[name] = materialize_features(rows)
        metadata[name] = {
            "event_count": len(rows),
            "positive_count": sum(row["label"] for row in rows),
            "start": min(row["event"]["occurred_at"] for row in rows),
            "end": max(row["event"]["occurred_at"] for row in rows),
            "sha256": checksum(path),
            "scenarios": dict(Counter(row["scenario"] for row in rows)),
        }
        print(f"Features ready: {name}, {len(rows)} events", flush=True)
    model = IsolationForest(
        n_estimators=160, max_samples=256, contamination="auto", random_state=SEED, n_jobs=1
    )
    model.fit(transform(features["train"]))
    dev_labels = np.asarray([row["label"] for row in periods["dev"]], dtype=np.int8)
    dev_model_scores = -model.score_samples(transform(features["dev"]))
    dev_rule_scores = np.asarray([rule_score(row) for row in features["dev"]])
    model_threshold = threshold_at_fpr(dev_model_scores[dev_labels == 0], FPR_BUDGET)
    rule_threshold = threshold_at_fpr(dev_rule_scores[dev_labels == 0], FPR_BUDGET)
    dev_metrics = {
        "rules": score_metrics(dev_labels, dev_rule_scores, rule_threshold),
        "model": score_metrics(dev_labels, dev_model_scores, model_threshold),
    }
    selected_policy = select_policy(dev_metrics, FPR_BUDGET)
    # Только теперь считаем итоговые метрики позднего периода. Его метки не меняют пороги.
    test_labels = np.asarray([row["label"] for row in periods["test"]], dtype=np.int8)
    test_model_scores = -model.score_samples(transform(features["test"]))
    test_rule_scores = np.asarray([rule_score(row) for row in features["test"]])
    args.output.mkdir(parents=True, exist_ok=True)
    model_path = args.output / "isolation_forest.joblib"
    joblib.dump(model, model_path, compress=3)
    manifest = {
        "schema_version": 1,
        "selected_policy": selected_policy,
        "policy_selection": {
            "split": "dev",
            "criteria": ["FPR <= budget", "maximum recall", "maximum precision", "rules on a tie"],
        },
        "model_type": "sklearn.ensemble.IsolationForest",
        "model_sha256": checksum(model_path),
        "feature_contract": {
            "names": FEATURE_NAMES,
            "version": FEATURE_VERSION,
            "window_seconds": WINDOW_SECONDS,
            "allowed_lateness_seconds": ALLOWED_LATENESS_SECONDS,
        },
        "rule_policy": {"version": RULE_VERSION, "limits": RULE_LIMITS},
        "preprocessing": "log1p-float32",
        "score_definition": "negative IsolationForest.score_samples; higher means more unusual, not attack probability",
        "comparison": ">",
        "thresholds": {"model": model_threshold, "rules": rule_threshold},
        "threshold_selection": {
            "split": "dev",
            "labels_used": "negatives only",
            "maximum_empirical_fpr": FPR_BUDGET,
            "positive_labels_used_for_thresholds": False,
        },
        "training": {
            "generator_version": GENERATOR_VERSION,
            "seed": SEED,
            "split": "train",
            "normal_only": True,
            "n_estimators": 160,
            "max_samples": 256,
            "contamination": "auto",
            "dataset": metadata,
        },
        "dependencies": {
            "scikit_learn": sklearn.__version__,
            "numpy": np.__version__,
            "joblib": joblib.__version__,
        },
        "drift_reference": drift_reference(features["train"]),
        "trust": "Load only artifacts from the trusted local repository. SHA detects corruption, not a malicious replacement of model and manifest together.",
    }
    manifest["identity"] = manifest_identity(manifest)
    save_json(args.output / "manifest.json", manifest)
    detector = Detector(args.output)
    for index in range(10):
        loaded = detector.evaluate(features["test"][index])
        assert np.isclose(loaded["model_score"], test_model_scores[index], rtol=0, atol=1e-10)
    test_metrics = {
        "rules": score_metrics(test_labels, test_rule_scores, rule_threshold),
        "model": score_metrics(test_labels, test_model_scores, model_threshold),
    }
    assert all(item["false_positive_rate"] <= FPR_BUDGET for item in dev_metrics.values())
    predictions = []
    for name, labels, model_scores, rule_scores in (
        ("dev", dev_labels, dev_model_scores, dev_rule_scores),
        ("test", test_labels, test_model_scores, test_rule_scores),
    ):
        for index, row in enumerate(periods[name]):
            predictions.append(
                {
                    "split": name,
                    "event_id": row["event"]["event_id"],
                    "occurred_at": row["event"]["occurred_at"],
                    "label": int(labels[index]),
                    "scenario": row["scenario"],
                    "model_score": float(model_scores[index]),
                    "model_alert": bool(model_scores[index] > model_threshold),
                    "rule_score": float(rule_scores[index]),
                    "rule_alert": bool(rule_scores[index] > rule_threshold),
                }
            )
    save_jsonl(args.output / "predictions.jsonl", predictions)
    shifted = [
        {
            **row,
            "actor_requests_60s": row["actor_requests_60s"] * 5 + 30,
            "actor_bytes_60s": row["actor_bytes_60s"] * 8 + 1_000_000,
        }
        for row in features["test"][:1000]
    ]
    scenario_recall = {}
    for scenario in sorted({row["scenario"] for row in periods["test"] if row["label"]}):
        mask = np.asarray([row["scenario"] == scenario for row in periods["test"]])
        scenario_recall[scenario] = {
            "positive_count": int(mask.sum()),
            "rules": float((test_rule_scores[mask] > rule_threshold).mean()),
            "model": float((test_model_scores[mask] > model_threshold).mean()),
        }
    report = {
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "model_version": detector.identity,
        "data_provenance": "Seeded synthetic events with scenario-authored labels; no real user or production security data",
        "selection": manifest["threshold_selection"],
        "selected_policy": selected_policy,
        "policy_selection": manifest["policy_selection"],
        "periods": metadata,
        "dev": dev_metrics,
        "test": test_metrics,
        "test_recall_by_scenario": scenario_recall,
        "scoring_latency": scoring_latency(detector, features["test"]),
        "latency_environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "concurrency": 1,
            "warmup": 10,
            "includes": "Single-row scoring; model path includes log1p transform; no feature extraction, database, network, queue or HTTP",
        },
        "drift": {
            "train": detector.drift(features["train"]),
            "dev": detector.drift(features["dev"]),
            "test": detector.drift(features["test"]),
            "deliberately_shifted_example": detector.drift(shifted),
        },
        "checksums": {
            "model_sha256": checksum(model_path),
            "manifest_sha256": checksum(args.output / "manifest.json"),
            "predictions_sha256": checksum(args.output / "predictions.jsonl"),
            "training_script_sha256": checksum(Path(__file__)),
            "feature_code_sha256": checksum(ROOT / "src/signalwatch/features.py"),
            "detector_code_sha256": checksum(ROOT / "src/signalwatch/detector.py"),
            "uv_lock_sha256": checksum(ROOT / "uv.lock"),
        },
        "limitations": [
            "Temporal windows are correlated; individual events are not independent trials.",
            "Evaluation arrival order equals event-time order; late arrival is tested separately through API replay.",
            "The 1% empirical false-positive budget is calibrated on dev negatives, not guaranteed on test or unseen traffic.",
            "Labels and attack patterns come from this generator; a later synthetic period does not establish real-world detection quality.",
            "PSI measures marginal distribution change, not attack probability or model quality.",
        ],
    }
    save_json(args.output / "evaluation.json", report)
    write_demo(args.data_dir)
    print(
        json.dumps(
            {
                "model_version": detector.identity,
                "dev": dev_metrics,
                "test": test_metrics,
                "scoring_latency": report["scoring_latency"],
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
