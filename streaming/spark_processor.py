"""
Spark Structured Streaming Analytics Processor.

Consumes real-time telemetry from Apache Kafka, performs schema validation,
event-time windowed aggregations, runs multi-method anomaly detection via
UnifiedAnomalyEngine, measures micro-batch latency and throughput, and
persists results into SQLite for the live Streamlit dashboard.
"""

import os
import sys
import time
import logging
from pathlib import Path
from typing import Optional

# Setup environment variables before PySpark initialization
BASE_DIR = Path(__file__).resolve().parent.parent
JDK_PATH = r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
HADOOP_PATH = str(BASE_DIR / "tools" / "hadoop")

os.environ["JAVA_HOME"] = JDK_PATH
os.environ["HADOOP_HOME"] = HADOOP_PATH
os.environ["PATH"] = f"{JDK_PATH}\\bin;{HADOOP_PATH}\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min as spark_min, max as spark_max, count, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

sys.path.append(str(BASE_DIR))
from config.config import config
from streaming.anomaly_detector import UnifiedAnomalyEngine
from streaming.storage import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SparkStream] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Schema of sensor telemetry JSON payload
SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("sensor_id", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("vibration", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("ground_truth_label", StringType(), True),
    StructField("anomaly_type", StringType(), True),
    StructField("anomaly_severity", StringType(), True),
])


class SparkStreamProcessor:
    """Production Spark Structured Streaming processor for IoT Sensor Stream."""

    def __init__(self, app_name: str = "RealTimeSensorStreamAnalytics"):
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self.anomaly_engine = UnifiedAnomalyEngine()
        self.batch_counter = 0

    def init_spark_session(self) -> SparkSession:
        """Create and configure SparkSession with Kafka SQL connector package."""
        logger.info("Initializing SparkSession with Kafka SQL connector...")
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master("local[*]") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .config("spark.python.worker.reuse", "false")

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"SparkSession active: version {self.spark.version}")
        return self.spark

    def _process_micro_batch(self, batch_df, batch_id: int):
        """Micro-batch sink handler: computes analytics, runs anomaly models, persists data."""
        start_time = time.perf_counter()
        row_count = batch_df.count()

        if row_count == 0:
            return

        self.batch_counter += 1
        logger.info(f"--- Processing Micro-Batch #{batch_id} ({row_count} events) ---")

        # Convert batch to local Pandas for detection and atomic storage
        pdf = batch_df.toPandas()

        raw_events = []
        alerts = []
        aggregations = []

        # 1. Anomaly detection on each event
        for _, row in pdf.iterrows():
            event_dict = row.to_dict()
            raw_events.append(event_dict)

            # Evaluate with Unified Anomaly Engine (Threshold, Z-score, Isolation Forest)
            det_result = self.anomaly_engine.process_event(event_dict)
            if det_result["is_anomaly"]:
                alerts.append(det_result)

        # 2. Window Aggregations per sensor
        sensor_groups = pdf.groupby("sensor_id")
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        for s_id, group in sensor_groups:
            anom_sub = [a for a in alerts if a["sensor_id"] == s_id]
            agg = {
                "window_start": str(group["timestamp"].min()),
                "window_end": str(group["timestamp"].max()),
                "window_type": "microbatch",
                "sensor_id": s_id,
                "event_count": len(group),
                "avg_temp": round(float(group["temperature"].mean()), 2),
                "min_temp": round(float(group["temperature"].min()), 2),
                "max_temp": round(float(group["temperature"].max()), 2),
                "avg_vib": round(float(group["vibration"].mean()), 4),
                "avg_press": round(float(group["pressure"].mean()), 2),
                "anomaly_count": len(anom_sub),
                "anomaly_rate": round(len(anom_sub) / len(group), 4),
            }
            aggregations.append(agg)

        # 3. Atomic persistence to SQLite
        storage.insert_events_batch(raw_events)
        if alerts:
            storage.insert_alerts_batch(alerts)
        if aggregations:
            storage.insert_window_aggregations(aggregations)

        # 4. Measure latency & throughput
        batch_duration_ms = (time.perf_counter() - start_time) * 1000.0
        process_rate = (row_count / (batch_duration_ms / 1000.0)) if batch_duration_ms > 0 else 0.0

        storage.record_pipeline_metrics(
            batch_id=batch_id,
            batch_duration_ms=batch_duration_ms,
            num_rows=row_count,
            input_rate=process_rate,
            process_rate=process_rate,
            latency_ms=batch_duration_ms
        )

        logger.info(
            f"Batch #{batch_id} complete in {batch_duration_ms:.1f}ms | "
            f"Processed: {row_count} | Alerts: {len(alerts)} | "
            f"Throughput: {process_rate:.1f} evt/s"
        )

    def start_streaming(self, trigger_seconds: int = 2):
        """Start reading stream from Kafka and processing with foreachBatch."""
        if not self.spark:
            self.init_spark_session()

        bootstrap = config.kafka.bootstrap_servers
        topic = config.kafka.topic
        checkpoint_dir = config.streaming.checkpoint_location

        logger.info(f"Connecting Structured Stream to Kafka '{topic}' at {bootstrap}...")

        # 1. Ingest Kafka stream
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # 2. Parse JSON payload and enforce schema
        parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data")) \
            .select("data.*") \
            .withColumn("event_time", to_timestamp(col("timestamp")))

        # 3. Output stream with micro-batch trigger
        logger.info(f"Starting query execution with trigger processingTime='{trigger_seconds} seconds'...")
        query = parsed_stream.writeStream \
            .foreachBatch(self._process_micro_batch) \
            .trigger(processingTime=f"{trigger_seconds} seconds") \
            .option("checkpointLocation", checkpoint_dir) \
            .start()

        logger.info("Spark Structured Streaming query is RUNNING.")
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Termination requested. Stopping streaming query...")
            query.stop()
            self.spark.stop()
            logger.info("Spark Structured Streaming stopped cleanly.")


if __name__ == "__main__":
    processor = SparkStreamProcessor()
    processor.start_streaming()
