"""
Thread-safe SQLite Storage & Persistence Layer for Streaming Analytics.

Features:
- Write-Ahead Logging (WAL mode) for non-blocking concurrent reads (Streamlit) and writes (Spark)
- Structured schemas for raw events, anomaly alerts, window aggregations, and pipeline metrics
- High-performance batch insertion and analytical query methods for the live dashboard
"""

import sys
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config

logger = logging.getLogger(__name__)


class AnalyticsStorage:
    """Manages SQLite tables and atomic batch operations for the streaming pipeline."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.streaming.sqlite_db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Create a connection with WAL enabled and optimized pragmas."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    def init_database(self):
        """Create tables and indexes if they do not exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 1. Raw Telemetry Events
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                timestamp TEXT,
                sensor_id TEXT,
                temperature REAL,
                vibration REAL,
                pressure REAL,
                ground_truth_label TEXT,
                anomaly_type TEXT,
                created_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON sensor_events(timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_sensor ON sensor_events(sensor_id);")

            # 2. Anomaly Alerts Feed
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                timestamp TEXT,
                sensor_id TEXT,
                temperature REAL,
                vibration REAL,
                pressure REAL,
                ground_truth_label TEXT,
                detected_by TEXT,
                severity TEXT,
                explanation TEXT,
                flag_threshold INTEGER,
                flag_zscore INTEGER,
                flag_isolation_forest INTEGER,
                created_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON anomaly_alerts(timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_sev ON anomaly_alerts(severity);")

            # 3. Time-Window Aggregations
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS window_aggregations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TEXT,
                window_end TEXT,
                window_type TEXT,
                sensor_id TEXT,
                event_count INTEGER,
                avg_temp REAL,
                min_temp REAL,
                max_temp REAL,
                avg_vib REAL,
                avg_press REAL,
                anomaly_count INTEGER,
                anomaly_rate REAL,
                updated_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_win_start ON window_aggregations(window_start);")

            # 4. Pipeline System Performance Metrics
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER,
                timestamp TEXT,
                batch_duration_ms REAL,
                num_input_rows INTEGER,
                input_rows_per_second REAL,
                process_rows_per_second REAL,
                sink_latency_ms REAL,
                created_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            """)
            conn.commit()

    def insert_events_batch(self, events: List[Dict[str, Any]]):
        """Batch insert raw sensor telemetry records."""
        if not events:
            return
        sql = """
        INSERT OR IGNORE INTO sensor_events (
            event_id, timestamp, sensor_id, temperature, vibration, pressure, ground_truth_label, anomaly_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        records = [
            (
                e.get("event_id"),
                e.get("timestamp"),
                e.get("sensor_id"),
                float(e.get("temperature", 0.0)),
                float(e.get("vibration", 0.0)),
                float(e.get("pressure", 0.0)),
                e.get("ground_truth_label", "normal"),
                e.get("anomaly_type", "none")
            )
            for e in events
        ]
        with self._get_connection() as conn:
            conn.executemany(sql, records)
            conn.commit()

    def insert_alerts_batch(self, alerts: List[Dict[str, Any]]):
        """Batch insert anomaly alerts."""
        if not alerts:
            return
        sql = """
        INSERT INTO anomaly_alerts (
            event_id, timestamp, sensor_id, temperature, vibration, pressure,
            ground_truth_label, detected_by, severity, explanation,
            flag_threshold, flag_zscore, flag_isolation_forest
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        records = [
            (
                a.get("event_id"),
                a.get("timestamp"),
                a.get("sensor_id"),
                float(a.get("temperature", 0.0)),
                float(a.get("vibration", 0.0)),
                float(a.get("pressure", 0.0)),
                a.get("ground_truth_label", "normal"),
                a.get("detected_by", "unknown"),
                a.get("severity", "none"),
                a.get("explanation", ""),
                1 if a.get("flag_threshold") else 0,
                1 if a.get("flag_zscore") else 0,
                1 if a.get("flag_isolation_forest") else 0
            )
            for a in alerts
        ]
        with self._get_connection() as conn:
            conn.executemany(sql, records)
            conn.commit()

    def insert_window_aggregations(self, aggregations: List[Dict[str, Any]]):
        """Insert or replace windowed analytical aggregates."""
        if not aggregations:
            return
        sql = """
        INSERT INTO window_aggregations (
            window_start, window_end, window_type, sensor_id,
            event_count, avg_temp, min_temp, max_temp,
            avg_vib, avg_press, anomaly_count, anomaly_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        records = [
            (
                agg.get("window_start"),
                agg.get("window_end"),
                agg.get("window_type", "tumbling"),
                agg.get("sensor_id"),
                int(agg.get("event_count", 0)),
                float(agg.get("avg_temp", 0.0)),
                float(agg.get("min_temp", 0.0)),
                float(agg.get("max_temp", 0.0)),
                float(agg.get("avg_vib", 0.0)),
                float(agg.get("avg_press", 0.0)),
                int(agg.get("anomaly_count", 0)),
                float(agg.get("anomaly_rate", 0.0))
            )
            for agg in aggregations
        ]
        with self._get_connection() as conn:
            conn.executemany(sql, records)
            conn.commit()

    def record_pipeline_metrics(
        self,
        batch_id: int,
        batch_duration_ms: float,
        num_rows: int,
        input_rate: float,
        process_rate: float,
        latency_ms: float = 0.0
    ):
        """Record Spark streaming micro-batch performance."""
        now_utc = datetime.now(timezone.utc).isoformat()
        sql = """
        INSERT INTO pipeline_metrics (
            batch_id, timestamp, batch_duration_ms, num_input_rows,
            input_rows_per_second, process_rows_per_second, sink_latency_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        with self._get_connection() as conn:
            conn.execute(sql, (
                batch_id, now_utc, batch_duration_ms, num_rows,
                input_rate, process_rate, latency_ms
            ))
            conn.commit()

    # --- Query Methods for Streamlit Live Dashboard ---

    def get_summary_kpis(self) -> Dict[str, Any]:
        """Fetch total events, total alerts, latest rates, and sensor count."""
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sensor_events")
            total_events = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM anomaly_alerts")
            total_alerts = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT sensor_id) FROM sensor_events")
            active_sensors = cur.fetchone()[0]

            cur.execute("""
            SELECT AVG(process_rows_per_second), AVG(batch_duration_ms)
            FROM (SELECT process_rows_per_second, batch_duration_ms FROM pipeline_metrics ORDER BY id DESC LIMIT 10)
            """)
            metric_row = cur.fetchone()
            avg_throughput = round(metric_row[0] or 0.0, 1)
            avg_latency = round(metric_row[1] or 0.0, 1)

            return {
                "total_events": total_events,
                "total_alerts": total_alerts,
                "active_sensors": active_sensors,
                "avg_throughput": avg_throughput,
                "avg_latency_ms": avg_latency,
            }

    def get_recent_events(self, limit: int = 100, sensor_id: Optional[str] = None) -> pd.DataFrame:
        """Fetch recent raw events as DataFrame."""
        with self._get_connection() as conn:
            if sensor_id and sensor_id != "All":
                query = "SELECT * FROM sensor_events WHERE sensor_id = ? ORDER BY id DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(sensor_id, limit))
            else:
                query = "SELECT * FROM sensor_events ORDER BY id DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(limit,))

    def get_recent_alerts(self, limit: int = 50, severity: Optional[str] = None) -> pd.DataFrame:
        """Fetch recent anomaly alerts."""
        with self._get_connection() as conn:
            if severity and severity != "All":
                query = "SELECT * FROM anomaly_alerts WHERE severity = ? ORDER BY id DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(severity, limit))
            else:
                query = "SELECT * FROM anomaly_alerts ORDER BY id DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(limit,))

    def get_window_aggregations(self, limit: int = 50) -> pd.DataFrame:
        """Fetch latest window summaries."""
        with self._get_connection() as conn:
            query = "SELECT * FROM window_aggregations ORDER BY id DESC LIMIT ?"
            return pd.read_sql_query(query, conn, params=(limit,))

    def get_recent_metrics(self, limit: int = 50) -> pd.DataFrame:
        """Fetch pipeline performance metrics history."""
        with self._get_connection() as conn:
            query = "SELECT * FROM pipeline_metrics ORDER BY id DESC LIMIT ?"
            return pd.read_sql_query(query, conn, params=(limit,))

    def clear_database(self):
        """Reset all tables for clean benchmarking runs."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM sensor_events;")
            conn.execute("DELETE FROM anomaly_alerts;")
            conn.execute("DELETE FROM window_aggregations;")
            conn.execute("DELETE FROM pipeline_metrics;")
            conn.commit()


# Global singleton storage instance
storage = AnalyticsStorage()
