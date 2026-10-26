"""
Unit tests for the three anomaly detection methods.
"""

import pytest
from streaming.anomaly_detector import (
    ThresholdDetector,
    RollingZScoreDetector,
    IsolationForestDetector,
    UnifiedAnomalyEngine,
)


def test_threshold_detector_normal():
    detector = ThresholdDetector()
    normal_event = {"temperature": 45.0, "vibration": 0.35, "pressure": 101.3}
    res = detector.detect(normal_event)
    assert not res["is_anomaly"]
    assert res["severity"] == "none"


def test_threshold_detector_violations():
    detector = ThresholdDetector()
    # High temperature
    res_t = detector.detect({"temperature": 95.0, "vibration": 0.35, "pressure": 101.3})
    assert res_t["is_anomaly"]
    assert "Temperature" in res_t["reason"]
    assert res_t["severity"] == "high"

    # Extreme vibration
    res_v = detector.detect({"temperature": 45.0, "vibration": 0.95, "pressure": 101.3})
    assert res_v["is_anomaly"]
    assert "Vibration" in res_v["reason"]

    # Low pressure
    res_p = detector.detect({"temperature": 45.0, "vibration": 0.35, "pressure": 60.0})
    assert res_p["is_anomaly"]
    assert "Pressure" in res_p["reason"]


def test_rolling_zscore_warmup_and_detection():
    detector = RollingZScoreDetector()
    sensor_id = "sensor_test_01"

    # Feed normal warm-up events
    for _ in range(12):
        evt = {"sensor_id": sensor_id, "temperature": 45.0, "vibration": 0.35, "pressure": 101.3}
        res = detector.detect(evt)

    # After warm-up, normal event should not trigger
    res_normal = detector.detect({"sensor_id": sensor_id, "temperature": 45.2, "vibration": 0.36, "pressure": 101.2})
    assert not res_normal["is_anomaly"]

    # Sudden extreme jump (e.g. 80.0°C when historical mean was ~45°C with near 0 std)
    res_spike = detector.detect({"sensor_id": sensor_id, "temperature": 85.0, "vibration": 0.35, "pressure": 101.3})
    assert res_spike["is_anomaly"]
    assert "Temperature" in res_spike["reason"]


def test_rolling_zscore_zero_variance_guard():
    detector = RollingZScoreDetector()
    sensor_id = "sensor_constant"

    # Feed perfectly identical numbers (variance = 0)
    for _ in range(15):
        detector.detect({"sensor_id": sensor_id, "temperature": 50.0, "vibration": 0.4, "pressure": 100.0})

    # Slight variance should not throw ZeroDivisionError
    res = detector.detect({"sensor_id": sensor_id, "temperature": 50.1, "vibration": 0.4, "pressure": 100.0})
    assert "z_scores" in res


def test_isolation_forest_detector():
    detector = IsolationForestDetector()
    assert detector.pipeline is not None, "Isolation Forest model pipeline failed to load"

    normal_evt = {"temperature": 45.0, "vibration": 0.35, "pressure": 101.3}
    res_norm = detector.detect(normal_evt)
    assert not res_norm["is_anomaly"]

    extreme_evt = {"temperature": 110.0, "vibration": 1.25, "pressure": 145.0}
    res_ext = detector.detect(extreme_evt)
    assert res_ext["is_anomaly"]
    assert "Multivariate outlier" in res_ext["reason"]


def test_unified_engine_coordination():
    engine = UnifiedAnomalyEngine()
    event = {
        "event_id": "evt_test_01",
        "timestamp": "2026-10-26T12:00:00Z",
        "sensor_id": "sensor_01",
        "temperature": 98.5,
        "vibration": 0.85,
        "pressure": 101.3,
        "ground_truth_label": "anomaly",
        "anomaly_type": "point"
    }
    result = engine.process_event(event)
    assert result["is_anomaly"]
    assert result["flag_threshold"]
    assert result["severity"] == "high"
    assert "threshold" in result["detected_by"]
