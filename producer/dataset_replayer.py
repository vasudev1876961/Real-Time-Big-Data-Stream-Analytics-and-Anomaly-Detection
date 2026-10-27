"""
Historical / Public Dataset Streaming Replayer.

Replays recorded industrial machine sensor records (CSV) into Apache Kafka
as if they were arriving from physical sensors in real time.
Updates timestamps to live UTC to ensure seamless Spark windowing and watermarking.
"""

import sys
import time
import json
import logging
import signal
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config, DATA_DIR
from producer.kafka_producer import SensorKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Replayer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class DatasetStreamReplayer:
    """Replays industrial sensor datasets into Kafka topic at configurable stream rates."""

    def __init__(
        self,
        dataset_path: Optional[str] = None,
        rate_hz: float = 10.0,
        loop: bool = True,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None
    ):
        self.dataset_path = Path(dataset_path or (DATA_DIR / "real_sensor_stream.csv"))
        self.rate_hz = rate_hz
        self.loop = loop
        self.producer = SensorKafkaProducer(
            bootstrap_servers=bootstrap_servers or config.kafka.bootstrap_servers,
            topic=topic or config.kafka.topic
        )
        self.running = False

    def start_replay(self, max_records: Optional[int] = None):
        """Begin streaming replay of dataset to Kafka."""
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset file not found at: {self.dataset_path}")

        logger.info(f"Loading dataset from: {self.dataset_path}")
        df = pd.read_csv(self.dataset_path)
        logger.info(f"Loaded {len(df)} records. Columns: {list(df.columns)}")

        if not self.producer.connect():
            raise ConnectionError("Failed to connect to Kafka broker.")

        self.running = True

        def signal_handler(sig, frame):
            logger.info("Interrupt received. Stopping dataset replay...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        delay = 1.0 / self.rate_hz if self.rate_hz > 0 else 0.0
        logger.info(f"Replaying dataset to topic '{self.producer.topic}' at {self.rate_hz} Hz (Loop: {self.loop})...")

        total_sent = 0
        start_time = time.time()
        last_log = start_time

        try:
            while self.running:
                for idx, row in df.iterrows():
                    if not self.running:
                        break
                    if max_records and total_sent >= max_records:
                        self.running = False
                        break

                    t0 = time.perf_counter()

                    # Convert row to event dict with live UTC timestamp
                    event = {
                        "event_id": f"rep_{total_sent:07d}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "sensor_id": str(row["sensor_id"]),
                        "temperature": float(row["temperature"]),
                        "vibration": float(row["vibration"]),
                        "pressure": float(row["pressure"]),
                        "ground_truth_label": str(row.get("ground_truth_label", "normal")),
                        "anomaly_type": str(row.get("anomaly_type", "none")),
                        "anomaly_severity": str(row.get("anomaly_severity", "none"))
                    }

                    self.producer.send_single_event(event)
                    total_sent += 1

                    # Periodic throughput log
                    curr_time = time.time()
                    if curr_time - last_log >= 5.0:
                        elapsed = curr_time - start_time
                        rate = total_sent / elapsed if elapsed > 0 else 0.0
                        logger.info(f"Replay progress: {total_sent} records sent | Current rate: {rate:.1f} evt/s")
                        last_log = curr_time

                    # Rate throttle
                    elapsed_step = time.perf_counter() - t0
                    sleep_t = delay - elapsed_step
                    if sleep_t > 0:
                        time.sleep(sleep_t)

                if not self.loop:
                    break
        finally:
            self.producer.stop()
            total_time = time.time() - start_time
            avg_rate = total_sent / total_time if total_time > 0 else 0.0
            logger.info(f"Dataset replay finished. Sent {total_sent} events in {total_time:.1f}s (Avg: {avg_rate:.1f} evt/s).")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Dataset Streaming Replayer")
    parser.add_argument("--rate", type=float, default=10.0, help="Replay rate (Hz)")
    parser.add_argument("--file", type=str, default=None, help="Path to CSV dataset")
    parser.add_argument("--max", type=int, default=None, help="Max records to send")
    parser.add_argument("--no-loop", action="store_true", help="Do not loop dataset continuously")
    args = parser.parse_args()

    replayer = DatasetStreamReplayer(
        dataset_path=args.file,
        rate_hz=args.rate,
        loop=not args.no_loop
    )
    replayer.start_replay(max_records=args.max)
