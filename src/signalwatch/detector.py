"""Загрузка проверенного локального артефакта и оценка без дообучения в API."""

import hashlib
import io
import json
import math
from pathlib import Path

import joblib
import numpy as np
import sklearn

from .features import (
    ALLOWED_LATENESS_SECONDS,
    FEATURE_NAMES,
    FEATURE_VERSION,
    WINDOW_SECONDS,
)

RULE_VERSION = "fixed-ratios-v1"
RULE_LIMITS = {
    "requests_60s": 30,
    "failed_requests_300s": 6,
    "additional_countries_300s": 2,
    "source_signups_300s": 12,
    "additional_source_actors_300s": 20,
    "source_actor_baseline": 4,
    "bytes_60s": 1_000_000,
}


def canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def manifest_identity(manifest: dict) -> str:
    return hashlib.sha256(
        canonical({key: value for key, value in manifest.items() if key != "identity"})
    ).hexdigest()


def feature_matrix(rows: list[dict]) -> np.ndarray:
    if not rows:
        return np.empty((0, len(FEATURE_NAMES)), dtype=np.float64)
    values = np.asarray([[row[name] for name in FEATURE_NAMES] for row in rows], dtype=np.float64)
    if not np.isfinite(values).all() or (values < 0).any():
        raise ValueError("Features must contain finite, nonnegative numbers")
    if (values[:, FEATURE_NAMES.index("actor_failure_ratio_300s")] > 1).any():
        raise ValueError("Failure ratio must not exceed one")
    return values


def transform(rows: list[dict]) -> np.ndarray:
    return np.log1p(feature_matrix(rows)).astype(np.float32)


def rule_components(features: dict) -> dict[str, float]:
    limits = RULE_LIMITS
    return {
        "request_rate": features["actor_requests_60s"] / limits["requests_60s"],
        "failed_requests": features["actor_failures_300s"]
        / limits["failed_requests_300s"]
        * features["actor_failure_ratio_300s"],
        "country_changes": max(0, features["actor_countries_300s"] - 1)
        / limits["additional_countries_300s"],
        "registrations": features["source_signups_300s"] / limits["source_signups_300s"],
        "source_actor_count": max(
            0, features["source_actors_300s"] - limits["source_actor_baseline"]
        )
        / limits["additional_source_actors_300s"],
        "transferred_bytes": features["actor_bytes_60s"] / limits["bytes_60s"],
    }


def rule_score(features: dict) -> float:
    return float(max(rule_components(features).values()))


def threshold_at_fpr(negative_scores: np.ndarray, budget: float = 0.01) -> float:
    scores = np.sort(np.asarray(negative_scores, dtype=np.float64))
    if not len(scores) or not np.isfinite(scores).all() or not 0 <= budget < 1:
        raise ValueError(
            "Threshold calibration requires finite negative scores and a budget in [0, 1)"
        )
    # Сравнение строгое: score > threshold. Одинаковые значения на границе не увеличивают FPR.
    allowed = math.floor(len(scores) * budget)
    return float(scores[len(scores) - allowed - 1])


def select_policy(dev_metrics: dict, budget: float = 0.01) -> str:
    eligible = [
        name for name in ("rules", "model") if dev_metrics[name]["false_positive_rate"] <= budget
    ]
    if not eligible:
        raise ValueError("No policy satisfies the development false-positive budget")
    return max(
        eligible,
        key=lambda name: (
            dev_metrics[name]["recall"],
            dev_metrics[name]["precision"],
            name == "rules",
        ),
    )


def drift_reference(rows: list[dict]) -> dict:
    values = feature_matrix(rows)
    if not len(values):
        raise ValueError("Drift reference must not be empty")
    result = {
        "sample_count": len(rows),
        "smoothing_count": 0.5,
        "minimum_samples": 100,
        "warning_psi": 0.2,
        "features": {},
    }
    for index, name in enumerate(FEATURE_NAMES):
        cuts = np.unique(np.quantile(values[:, index], np.arange(0.1, 1, 0.1)))
        if np.ptp(values[:, index]) == 0:
            # Постоянное значение оставляем в отдельном интервале, иначе рост из нуля потеряется.
            cuts = np.array([values[0, index], np.nextafter(values[0, index], np.inf)])
        counts = np.bincount(
            np.searchsorted(cuts, values[:, index], side="right"), minlength=len(cuts) + 1
        )
        result["features"][name] = {"cuts": cuts.tolist(), "counts": counts.tolist()}
    return result


class Detector:
    def __init__(self, path: str | Path = "artifacts"):
        directory = Path(path)
        self.manifest = json.loads((directory / "manifest.json").read_text())
        self.identity = manifest_identity(self.manifest)
        if self.manifest.get("identity") != self.identity:
            raise ValueError("Artifact manifest checksum does not match")
        if self.manifest["selected_policy"] not in {"rules", "model"}:
            raise ValueError("Unsupported decision policy")
        contract = self.manifest["feature_contract"]
        if contract != {
            "names": FEATURE_NAMES,
            "version": FEATURE_VERSION,
            "window_seconds": WINDOW_SECONDS,
            "allowed_lateness_seconds": ALLOWED_LATENESS_SECONDS,
        }:
            raise ValueError("Artifact feature contract does not match this application")
        if self.manifest["rule_policy"] != {"version": RULE_VERSION, "limits": RULE_LIMITS}:
            raise ValueError("Artifact rule policy does not match this application")
        if self.manifest["preprocessing"] != "log1p-float32" or self.manifest["comparison"] != ">":
            raise ValueError("Unsupported score preprocessing or threshold comparison")
        for library, actual in (
            ("scikit_learn", sklearn.__version__),
            ("numpy", np.__version__),
            ("joblib", joblib.__version__),
        ):
            if self.manifest["dependencies"][library] != actual:
                raise ValueError(
                    f"Artifact requires a different {library} version; install the frozen project environment"
                )
        payload = (directory / "isolation_forest.joblib").read_bytes()
        if hashlib.sha256(payload).hexdigest() != self.manifest["model_sha256"]:
            raise ValueError("Model checksum does not match the trusted local manifest")
        # Проверка хеша ловит порчу файла. Она не делает чужой pickle безопасным для загрузки.
        self.model = joblib.load(io.BytesIO(payload))
        if self.model.n_features_in_ != len(FEATURE_NAMES):
            raise ValueError("Model input width does not match the feature contract")

    def evaluate(self, features: dict) -> dict:
        score = float(-self.model.score_samples(transform([features]))[0])
        rules = rule_components(features)
        baseline = float(max(rules.values()))
        model_threshold = float(self.manifest["thresholds"]["model"])
        rule_threshold = float(self.manifest["thresholds"]["rules"])
        model_alert, rule_alert = score > model_threshold, baseline > rule_threshold
        observations = {
            "request_rate": f"За минуту от одного пользователя поступило запросов: {features['actor_requests_60s']:.0f}.",
            "failed_requests": f"За пять минут неудачных запросов: {features['actor_failures_300s']:.0f}; доля ошибок: {features['actor_failure_ratio_300s']:.0%}.",
            "country_changes": f"За пять минут запросы пользователя пришли из нескольких стран: {features['actor_countries_300s']:.0f}.",
            "registrations": f"За пять минут с одного IP поступило регистраций: {features['source_signups_300s']:.0f}.",
            "source_actor_count": f"За пять минут один IP использовали разные пользователи: {features['source_actors_300s']:.0f}.",
            "transferred_bytes": f"За минуту пользователь передал {features['actor_bytes_60s']:.0f} байт.",
        }
        reasons = [observations[name] for name, value in rules.items() if value > rule_threshold]
        if model_alert:
            reasons.append(
                "Сочетание признаков вышло за порог аномальности модели; это повод проверить событие."
            )
        return {
            "model_score": score,
            "model_threshold": model_threshold,
            "model_alert": bool(model_alert),
            "rule_score": baseline,
            "rule_threshold": rule_threshold,
            "rule_alert": bool(rule_alert),
            "reasons": reasons,
            "model_version": self.identity,
            "policy": self.manifest["selected_policy"],
            "alert": bool(
                rule_alert if self.manifest["selected_policy"] == "rules" else model_alert
            ),
        }

    def drift(self, rows: list[dict]) -> dict:
        reference = self.manifest["drift_reference"]
        result = {
            "sample_count": len(rows),
            "reference_count": reference["sample_count"],
            "model_version": self.identity,
            "features": {},
            "max_psi": None,
            "minimum_samples": reference["minimum_samples"],
            "warning_psi": reference["warning_psi"],
            "status": "insufficient_data",
        }
        if not rows:
            return result
        values = feature_matrix(rows)
        smoothing = reference["smoothing_count"]
        for index, name in enumerate(FEATURE_NAMES):
            bins = reference["features"][name]
            counts = np.bincount(
                np.searchsorted(bins["cuts"], values[:, index], side="right"),
                minlength=len(bins["cuts"]) + 1,
            )
            expected = np.asarray(bins["counts"], dtype=np.float64) + smoothing
            observed = counts.astype(np.float64) + smoothing
            expected /= expected.sum()
            observed /= observed.sum()
            psi = float(np.sum((observed - expected) * np.log(observed / expected)))
            result["features"][name] = {
                "psi": psi,
                "cuts": bins["cuts"],
                "reference": expected.tolist(),
                "observed": observed.tolist(),
            }
        result["max_psi"] = max(row["psi"] for row in result["features"].values())
        if len(rows) >= reference["minimum_samples"]:
            result["status"] = "shift" if result["max_psi"] > reference["warning_psi"] else "stable"
        return result
