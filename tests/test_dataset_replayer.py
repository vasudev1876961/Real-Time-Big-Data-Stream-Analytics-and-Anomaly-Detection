"""
Unit tests for the industrial dataset streaming replayer.
"""

import pytest
import pandas as pd
from pathlib import Path
from producer.dataset_replayer import DatasetStreamReplayer
from config.config import DATA_DIR


def test_industrial_dataset_structure():
    """Verify that real_sensor_stream.csv exists, is non-empty, and conforms to required schema."""
    csv_path = DATA_DIR / "real_sensor_stream.csv"
    assert csv_path.exists(), f"Dataset file missing at {csv_path}"

    df = pd.read_csv(csv_path)
    assert len(df) >= 1000, "Dataset has fewer than 1,000 records"

    required_cols = ["event_id", "timestamp", "sensor_id", "temperature", "vibration", "pressure", "ground_truth_label"]
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"

    # Verify both normal and anomaly labels exist
    labels = set(df["ground_truth_label"].unique())
    assert "normal" in labels
    assert "anomaly" in labels


def test_replayer_initialization():
    """Verify replayer initializes and loads dataset without errors."""
    csv_path = str(DATA_DIR / "real_sensor_stream.csv")
    replayer = DatasetStreamReplayer(dataset_path=csv_path, rate_hz=20.0, loop=False)

    assert replayer.rate_hz == 20.0
    assert not replayer.loop
    assert replayer.dataset_path.exists()
