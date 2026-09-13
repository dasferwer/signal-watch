from signalwatch.detector import select_policy


def test_policy_is_selected_from_development_metrics_within_budget():
    dev = {
        "rules": {"false_positive_rate": 0.005, "recall": 0.7, "precision": 0.8},
        "model": {"false_positive_rate": 0.02, "recall": 0.99, "precision": 0.9},
    }
    assert select_policy(dev) == "rules"
    dev["model"]["false_positive_rate"] = 0.01
    assert select_policy(dev) == "model"


def test_exact_development_tie_prefers_rules():
    score = {"false_positive_rate": 0, "recall": 0.5, "precision": 1}
    assert select_policy({"rules": score, "model": score}) == "rules"
