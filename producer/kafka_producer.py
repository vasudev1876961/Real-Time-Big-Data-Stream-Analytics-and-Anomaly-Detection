"""
Kafka Telemetry Producer.

Streams synthetic industrial sensor events to Apache Kafka topic.
Uses sensor_id as partition key for strict per-sensor temporal ordering.
"""

import sys
import time
import json
import logging
import signal
from pathlib import Path
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config
from producer.event_generator import EventGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Producer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class SensorKafkaProducer:
    """Publishes continuous or batch sensor telemetry to Apache Kafka."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        rate_hz: Optional[float] = None,
        seed: Optional[int] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.topic = topic or config.kafka.topic
        self.rate_hz = rate_hz if rate_hz is not None else config.generator.default_rate_hz
        self.generator = EventGenerator(seed=seed)
        self.running = False
        self.producer: Optional[KafkaProducer] = None

    def connect(self, max_retries: int = 5, retry_interval: float = 2.0) -> bool:
        """Establish connection to Kafka broker with retry backoff."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers} (Attempt {attempt}/{max_retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                    acks="all",
                    retries=3,
                    linger_ms=10,
                    batch_size=16384,
                    request_timeout_ms=10000,
                )
                logger.info("Successfully connected to Kafka broker.")
                return True
            except NoBrokersAvailable:
                logger.warning(f"Kafka broker not available at {self.bootstrap_servers}. Retrying in {retry_interval}s...")
                time.sleep(retry_interval)
            except Exception as e:
                logger.error(f"Unexpected connection error: {e}")
                time.sleep(retry_interval)

        logger.error(f"Failed to connect to Kafka after {max_retries} attempts.")
        return False

    def send_single_event(self, event: Dict[str, Any]) -> bool:
        """Publish a single event to the configured Kafka topic."""
        if not self.producer:
            raise RuntimeError("Producer is not connected. Call connect() first.")

        try:
            future = self.producer.send(
                self.topic,
                key=event["sensor_id"],
                value=event
            )
            # Do not block on future for streaming throughput, but register error handler
            future.add_errback(lambda err: logger.error(f"Failed to deliver event {event.get('event_id')}: {err}"))
            return True
        except KafkaError as ke:
            logger.error(f"Kafka delivery error: {ke}")
            return False

    def run_stream(self, max_events: Optional[int] = None, duration_seconds: Optional[float] = None):
        """Run continuous event publishing loop."""
        if not self.producer and not self.connect():
            raise ConnectionError("Cannot stream without active Kafka connection.")

        self.running = True

        def signal_handler(sig, frame):
            logger.info("Interruption received. Shutting down Kafka producer...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info(f"Starting event stream -> Topic: '{self.topic}' @ {self.rate_hz} Hz...")
        delay = 1.0 / self.rate_hz if self.rate_hz > 0 else 0.0

        sent_count = 0
        start_time = time.time()
        last_log_time = start_time

        try:
            for event in self.generator.stream_events(rate_hz=self.rate_hz, max_events=max_events):
                if not self.running:
                    break
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    logger.info(f"Stream duration limit ({duration_seconds}s) reached.")
                    break

                self.send_single_event(event)
                sent_count += 1

                # Periodic throughput logging every 5 seconds
                curr_time = time.time()
                if curr_time - last_log_time >= 5.0:
                    elapsed = curr_time - start_time
                    actual_rate = sent_count / elapsed if elapsed > 0 else 0.0
                    logger.info(f"Stream progress: {sent_count} events sent | Current throughput: {actual_rate:.1f} evt/s")
                    last_log_time = curr_time

        finally:
            self.stop()
            total_elapsed = time.time() - start_time
            avg_rate = sent_count / total_elapsed if total_elapsed > 0 else 0.0
            logger.info(f"Streaming stopped. Total sent: {sent_count} events in {total_elapsed:.2f}s (Avg: {avg_rate:.1f} evt/s).")

    def stop(self):
        """Flush buffers and close producer."""
        self.running = False
        if self.producer:
            logger.info("Flushing pending events to Kafka...")
            try:
                self.producer.flush(timeout=5)
                self.producer.close(timeout=5)
            except Exception as e:
                logger.warning(f"Error during producer shutdown: {e}")
            finally:
                self.producer = None
            logger.info("Kafka producer closed cleanly.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Sensor Event Producer")
    parser.add_argument("--rate", type=float, default=10.0, help="Event rate (Hz)")
    parser.add_argument("--events", type=int, default=None, help="Max events to send")
    parser.add_argument("--duration", type=float, default=None, help="Duration in seconds")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    producer = SensorKafkaProducer(rate_hz=args.rate, seed=args.seed)
    producer.run_stream(max_events=args.events, duration_seconds=args.duration)
