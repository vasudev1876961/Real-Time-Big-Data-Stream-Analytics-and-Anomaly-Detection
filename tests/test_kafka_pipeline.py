"""
Integration test for Apache Kafka Producer-Consumer pipeline.
"""

import json
import time
import pytest
from kafka import KafkaConsumer, TopicPartition
from producer.kafka_producer import SensorKafkaProducer
from producer.event_generator import EventGenerator
from config.config import config


def test_kafka_producer_consumer_roundtrip():
    """Verify that events sent by SensorKafkaProducer are received accurately by KafkaConsumer."""
    topic = "sensor-readings"
    bootstrap = config.kafka.bootstrap_servers

    # 1. Setup producer and send batch of unique events
    producer = SensorKafkaProducer(bootstrap_servers=bootstrap, topic=topic)
    assert producer.connect(), "Failed to connect producer to Kafka"

    test_run_id = f"test_{int(time.time()*1000)}"
    gen = EventGenerator(seed=999)
    test_events = gen.generate_batch(5)
    for i, e in enumerate(test_events):
        e["event_id"] = f"{test_run_id}_{i:03d}"
    sent_ids = [e["event_id"] for e in test_events]

    delivered_metadata = []
    for event in test_events:
        fut = producer.producer.send(
            topic,
            key=event["sensor_id"],
            value=event
        )
        meta = fut.get(timeout=10.0)
        delivered_metadata.append(meta)

    producer.stop()
    assert len(delivered_metadata) == 5

    # 2. Setup consumer and read messages from topic partitions
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    partitions = [TopicPartition(topic, p) for p in range(3)]
    consumer.assign(partitions)

    # Find earliest offset for our delivered batch
    for meta in delivered_metadata:
        tp = TopicPartition(topic, meta.partition)
        # ensure consumer is positioned at or before this offset
        curr = consumer.position(tp)
        if curr is None or curr > meta.offset:
            consumer.seek(tp, meta.offset)

    received_events = []
    start_time = time.time()
    for message in consumer:
        val = message.value
        if val.get("event_id") in sent_ids:
            received_events.append(val)
        if len(received_events) >= len(sent_ids) or (time.time() - start_time > 8.0):
            break

    consumer.close()

    # 3. Assertions
    assert len(received_events) == len(sent_ids), f"Expected {len(sent_ids)} events, got {len(received_events)}"

    rec_ids = [e["event_id"] for e in received_events]
    for sid in sent_ids:
        assert sid in rec_ids

    sample = received_events[0]
    assert "temperature" in sample
    assert "vibration" in sample
    assert "pressure" in sample
    assert "ground_truth_label" in sample
