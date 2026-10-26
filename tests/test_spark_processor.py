"""
Unit and integration tests for Spark processing logic and schema validation.
"""

import json
import pytest
from streaming.spark_processor import SENSOR_SCHEMA, SparkStreamProcessor
from streaming.storage import AnalyticsStorage
from producer.event_generator import EventGenerator
import pandas as pd


def test_sensor_schema_fields():
    """Verify Spark schema matches expected fields and types."""
    field_names = [f.name for f in SENSOR_SCHEMA.fields]
    assert "event_id" in field_names
    assert "timestamp" in field_names
    assert "sensor_id" in field_names
    assert "temperature" in field_names
    assert "vibration" in field_names
    assert "pressure" in field_names
    assert "ground_truth_label" in field_names


def test_micro_batch_processing_logic(tmp_path):
    """Test the micro-batch processing logic and persistence using mock DataFrame."""
    db_file = str(tmp_path / "test_analytics.db")
    test_storage = AnalyticsStorage(db_path=db_file)

    gen = EventGenerator(seed=123)
    sample_events = gen.generate_batch(20)

    # Convert to pandas DataFrame
    pdf = pd.DataFrame(sample_events)

    class MockSparkDataFrame:
        def __init__(self, data):
            self.data = data
        def count(self):
            return len(self.data)
        def toPandas(self):
            return self.data

    processor = SparkStreamProcessor()
    # Temporarily point storage to test_storage
    from streaming import spark_processor
    original_storage = spark_processor.storage
    spark_processor.storage = test_storage

    try:
        mock_df = MockSparkDataFrame(pdf)
        processor._process_micro_batch(mock_df, batch_id=1)

        # Verify records written to SQLite
        kpis = test_storage.get_summary_kpis()
        assert kpis["total_events"] == 20
        events_df = test_storage.get_recent_events(limit=50)
        assert len(events_df) == 20

        # Verify window aggregations were calculated
        aggs = test_storage.get_window_aggregations(limit=20)
        assert len(aggs) > 0
        assert "avg_temp" in aggs.columns

        # Verify pipeline metrics recorded
        metrics = test_storage.get_recent_metrics(limit=10)
        assert len(metrics) == 1
        assert metrics.iloc[0]["batch_id"] == 1
    finally:
        spark_processor.storage = original_storage
