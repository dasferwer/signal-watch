import copy
import json
import shutil
import uuid
from pathlib import Path

import numpy as np
import pytest

from signalwatch.detector import (
    Detector,
    drift_reference,
    manifest_identity,
    threshold_at_fpr,
    transform,
)
from signalwatch.features import FEATURE_NAMES, extract_features

ROOT = Path(__file__).resolve().parents[1]


def event(moment=1000, **changes):
    return {
        "event_id": str(uuid.uuid4()),
        "occurred_at": moment,
        "actor_id": "alice",
        "source_ip": "198.51.100.1",
        "country": "US",
        "kind": "api",
        "success": True,
        "bytes_sent": 100,
        **changes,
    }


def test_windows_are_open_left_closed_right_and_never_use_future():
    current = event()
    history = [
        event(700, success=False),
        event(700.001, success=False),
        event(940),
        event(940.001),
        event(1000),
        event(1000.001, success=False),
    ]
    result = extract_features(history, current)
    assert list(result) == FEATURE_NAMES
    assert result["actor_requests_60s"] == 3
    assert result["actor_failures_300s"] == 1
    assert result["actor_failure_ratio_300s"] == 1 / 5
    assert result["actor_bytes_60s"] == 300


def test_duplicates_and_current_event_are_counted_once_without_mutating_inputs():
    current = event()
    previous = event(999, success=False)
    history = [current, previous, copy.deepcopy(previous), copy.deepcopy(current)]
    original = copy.deepcopy(history)
    result = extract_features(history, current)
    assert result["actor_requests_60s"] == 2
    assert result["actor_failure_ratio_300s"] == 0.5
    assert history == original


def test_actor_and_ip_features_have_different_scopes():
    current = event()
    history = [
        event(999, actor_id="bob", kind="signup"),
        event(998, actor_id="bob", kind="signup"),
        event(997, actor_id="carol", source_ip="198.51.100.2", kind="signup"),
        event(996, source_ip="198.51.100.3", country="GB", success=False),
    ]
    result = extract_features(history, current)
    assert result["actor_requests_60s"] == 2
    assert result["actor_countries_300s"] == 2
    assert result["actor_failures_300s"] == 1
    assert result["source_signups_300s"] == 2
    assert result["source_actors_300s"] == 2


def test_feature_extraction_is_invariant_to_history_order():
    current = event()
    history = [event(999), event(955, success=False), event(800, country="FR")]
    assert extract_features(history, current) == extract_features(list(reversed(history)), current)


def test_threshold_respects_budget_with_and_without_ties():
    unique = np.arange(1000)
    threshold = threshold_at_fpr(unique, 0.01)
    assert (unique > threshold).mean() == 0.01
    tied = np.asarray([0] * 975 + [1] * 25)
    assert (tied > threshold_at_fpr(tied, 0.01)).mean() == 0
    assert threshold_at_fpr(np.asarray([1, 2, 3]), 0.01) == 3


@pytest.mark.parametrize(
    "scores,budget", [(np.array([]), 0.01), (np.array([np.nan]), 0.01), (np.array([1]), -0.01)]
)
def test_invalid_calibration_data_is_rejected(scores, budget):
    with pytest.raises(ValueError):
        threshold_at_fpr(scores, budget)


@pytest.mark.parametrize("value", [-1, np.inf, np.nan])
def test_model_rejects_invalid_feature_values(value):
    features = extract_features([], event())
    features["actor_bytes_60s"] = value
    with pytest.raises(ValueError, match="finite, nonnegative"):
        transform([features])


def test_manifest_and_model_load_match_recorded_predictions():
    detector = Detector(ROOT / "artifacts")
    first_event = json.loads((ROOT / "data/test_events.jsonl").read_text().splitlines()[0])["event"]
    expected = next(
        json.loads(line)
        for line in (ROOT / "artifacts/predictions.jsonl").read_text().splitlines()
        if json.loads(line)["split"] == "test"
    )
    result = detector.evaluate(extract_features([], first_event))
    assert result["model_score"] == pytest.approx(expected["model_score"], abs=1e-10)
    assert result["model_alert"] == expected["model_alert"]
    assert result["rule_alert"] == expected["rule_alert"]
    assert result["model_version"] == detector.manifest["identity"]
    json.dumps(result, allow_nan=False)


def test_corrupted_model_is_rejected_before_deserialization(tmp_path, monkeypatch):
    shutil.copy(ROOT / "artifacts/manifest.json", tmp_path / "manifest.json")
    (tmp_path / "isolation_forest.joblib").write_bytes(b"untrusted content")

    def must_not_load(*args, **kwargs):
        pytest.fail("Checksum must be verified before joblib.load")

    monkeypatch.setattr("signalwatch.detector.joblib.load", must_not_load)
    with pytest.raises(ValueError, match="checksum"):
        Detector(tmp_path)


def test_changed_threshold_also_changes_model_identity(tmp_path):
    manifest = json.loads((ROOT / "artifacts/manifest.json").read_text())
    previous = manifest_identity(manifest)
    manifest["thresholds"]["model"] += 0.01
    assert manifest_identity(manifest) != previous
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="manifest checksum"):
        Detector(tmp_path)


def test_incompatible_feature_window_is_rejected(tmp_path):
    manifest = json.loads((ROOT / "artifacts/manifest.json").read_text())
    manifest["feature_contract"]["window_seconds"] = 600
    manifest["identity"] = manifest_identity(manifest)
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="feature contract"):
        Detector(tmp_path)


def test_psi_detects_a_change_in_a_constant_feature():
    normal = extract_features([], event())
    rows = [normal] * 200
    detector = Detector(ROOT / "artifacts")
    detector.manifest = {**detector.manifest, "drift_reference": drift_reference(rows)}
    assert detector.drift(rows)["max_psi"] == 0
    shifted = [{**normal, "actor_failures_300s": 1, "actor_failure_ratio_300s": 0.5}] * 200
    result = detector.drift(shifted)
    assert result["features"]["actor_failures_300s"]["psi"] > 0.2
    assert result["status"] == "shift"
    assert detector.drift([])["status"] == "insufficient_data"
    assert detector.drift(shifted[:2])["status"] == "insufficient_data"
    json.dumps(result, allow_nan=False)


def test_temporal_splits_and_negative_only_training_are_saved():
    report = json.loads((ROOT / "artifacts/evaluation.json").read_text())
    periods = report["periods"]
    assert periods["train"]["positive_count"] == 0
    assert periods["train"]["end"] < periods["dev"]["start"]
    assert periods["dev"]["end"] < periods["test"]["start"]
    for name in ("dev", "test"):
        assert 0.01 <= periods[name]["positive_count"] / periods[name]["event_count"] <= 0.03
    assert all(row["false_positive_rate"] <= 0.01 for row in report["dev"].values())
    assert report["selection"]["labels_used"] == "negatives only"
