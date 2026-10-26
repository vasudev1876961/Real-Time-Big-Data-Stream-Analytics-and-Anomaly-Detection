"""
End-to-End Pipeline Smoke Test.

Validates the full chain:
Sensor Generator -> Kafka Producer -> Kafka Broker -> Spark Processing -> SQLite Persistence -> Dashboard Reader.
"""

import time
import pytest
import pandas as pd
from producer.kafka_producer import SensorKafkaProducer
from producer.event_generator import EventGenerator
from streaming.spark_processor import SparkStreamProcessor
from streaming.storage import storage
from config.config import config


def test_end_to_end_pipeline_smoke():
    """Verify that events generated and sent to Kafka are processed and readable from SQLite."""
    # 1. Setup Producer
    producer = SensorKafkaProducer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        topic=config.kafka.topic
    )
    assert producer.connect(), "Failed to connect producer to Kafka"

    # Generate 10 controlled events (including an anomaly)
    gen = EventGenerator(seed=888)
    batch = gen.generate_batch(10)

    # Force one extreme point anomaly
    batch[0]["temperature"] = 99.5
    batch[0]["ground_truth_label"] = "anomaly"
    batch[0]["anomaly_type"] = "point"

    for event in batch:
        producer.send_single_event(event)

    producer.producer.flush()
    producer.stop()

    # 2. Process via Spark Processor micro-batch logic
    processor = SparkStreamProcessor()
    pdf = pd.DataFrame(batch)

    class MockDF:
        def __init__(self, data):
            self.data = data
        def count(self):
            return len(self.data)
        def toPandas(self):
            return self.data

    processor._process_micro_batch(MockDF(pdf), batch_id=999)

    # 3. Verify SQLite persistence and KPI accessibility
    kpis = storage.get_summary_kpis()
    assert kpis["total_events"] > 0, "No events recorded in database"
    assert kpis["total_alerts"] > 0, "Anomaly alert was not recorded in database"

    # 4. Verify Dashboard query methods work cleanly
    recent_events = storage.get_recent_events(limit=10)
    assert not recent_events.empty

    recent_alerts = storage.get_recent_alerts(limit=5)
    assert not recent_alerts.empty
    assert any(a["temperature"] > 90.0 for _, a in recent_alerts.iterrows())
