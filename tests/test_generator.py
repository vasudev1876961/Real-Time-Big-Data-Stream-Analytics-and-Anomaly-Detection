"""
Unit tests for the synthetic sensor event generator.
"""

import pytest
from datetime import datetime
from producer.event_generator import EventGenerator
from config.config import GeneratorSettings, SensorBaseline


def test_event_structure_and_schema():
    """Verify all required fields exist and have valid types."""
    gen = EventGenerator(seed=101)
    event = gen.generate_single_event()

    required_fields = [
        "event_id", "timestamp", "sensor_id",
        "temperature", "vibration", "pressure",
        "ground_truth_label", "anomaly_type", "anomaly_severity"
    ]
    for field in required_fields:
        assert field in event, f"Missing field: {field}"

    assert isinstance(event["event_id"], str)
    assert event["event_id"].startswith("evt_")
    assert isinstance(event["temperature"], float)
    assert isinstance(event["vibration"], float)
    assert isinstance(event["pressure"], float)
    assert event["ground_truth_label"] in ["normal", "anomaly"]
    assert event["anomaly_type"] in ["none", "point", "collective", "multivariate"]
    assert event["anomaly_severity"] in ["none", "low", "medium", "high"]

    # ISO 8601 timestamp validation
    dt = datetime.fromisoformat(event["timestamp"])
    assert dt is not None


def test_seed_reproducibility():
    """Verify same seed produces identical numerical and categorical sequences."""
    gen1 = EventGenerator(seed=12345)
    gen2 = EventGenerator(seed=12345)

    batch1 = gen1.generate_batch(50)
    batch2 = gen2.generate_batch(50)

    for e1, e2 in zip(batch1, batch2):
        # All measurements, IDs, and labels must be identical
        assert e1["sensor_id"] == e2["sensor_id"]
        assert e1["temperature"] == e2["temperature"]
        assert e1["vibration"] == e2["vibration"]
        assert e1["pressure"] == e2["pressure"]
        assert e1["ground_truth_label"] == e2["ground_truth_label"]
        assert e1["anomaly_type"] == e2["anomaly_type"]


def test_anomaly_generation_and_types():
    """Verify all anomaly types (point, collective, multivariate) occur in a large sample."""
    settings = GeneratorSettings(
        anomaly_probability=0.25,
        random_seed=777
    )
    gen = EventGenerator(settings=settings, seed=777)
    events = gen.generate_batch(1000)

    anomalies = [e for e in events if e["ground_truth_label"] == "anomaly"]
    assert len(anomalies) > 100, "Too few anomalies generated"

    types_generated = set(e["anomaly_type"] for e in anomalies)
    assert "point" in types_generated, "Point anomalies missing"
    assert "collective" in types_generated, "Collective anomalies missing"
    assert "multivariate" in types_generated, "Multivariate anomalies missing"


def test_physical_constraints():
    """Verify values do not violate physical lower bounds (vibration > 0, pressure > 0)."""
    gen = EventGenerator(seed=42)
    events = gen.generate_batch(500)

    for e in events:
        assert e["vibration"] > 0, f"Negative vibration detected: {e['vibration']}"
        assert e["pressure"] > 0, f"Negative pressure detected: {e['pressure']}"
