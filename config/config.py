"""
Central Configuration Module for Real-Time Big Data Stream Analytics.
Provides strongly-typed settings, defaults, and helper paths for all pipeline components.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any

# Root directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = DATA_DIR / "results"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
MODELS_DIR = BASE_DIR / "models"
CONFIG_DIR = BASE_DIR / "config"

# Ensure runtime directories exist
DATA_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)
CHECKPOINT_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)


@dataclass
class KafkaSettings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "sensor-readings")
    consumer_group: str = "bda-spark-stream-group"
    auto_offset_reset: str = "latest"


@dataclass
class SensorBaseline:
    temp_mean: float = 45.0
    temp_std: float = 4.0
    vib_mean: float = 0.35
    vib_std: float = 0.05
    press_mean: float = 101.3
    press_std: float = 2.5


@dataclass
class GeneratorSettings:
    sensors: List[str] = field(default_factory=lambda: [f"sensor_{i:02d}" for i in range(1, 6)])
    default_rate_hz: float = 10.0  # events per second
    anomaly_probability: float = 0.08  # 8% total anomalies
    point_anomaly_ratio: float = 0.5  # 50% point, 30% collective, 20% multivariate
    collective_anomaly_ratio: float = 0.3
    multivariate_anomaly_ratio: float = 0.2
    random_seed: int = 42


@dataclass
class ThresholdLimits:
    temp_min: float = 20.0
    temp_max: float = 75.0
    vib_min: float = 0.05
    vib_max: float = 0.65
    press_min: float = 85.0
    press_max: float = 115.0


@dataclass
class ZScoreSettings:
    window_size: int = 30  # rolling history per sensor
    threshold: float = 3.0  # std deviations
    min_periods: int = 10  # warm-up period
    epsilon: float = 1e-4  # zero-div guard


@dataclass
class IsolationForestSettings:
    contamination: float = 0.05
    n_estimators: int = 100
    random_state: int = 42
    model_path: str = str(MODELS_DIR / "isolation_forest.joblib")
    features: List[str] = field(default_factory=lambda: ["temperature", "vibration", "pressure"])


@dataclass
class StreamingSettings:
    watermark_delay: str = "10 seconds"
    tumbling_window_duration: str = "30 seconds"
    sliding_window_duration: str = "60 seconds"
    sliding_window_slide: str = "10 seconds"
    trigger_processing_time: str = "2 seconds"
    checkpoint_location: str = str(CHECKPOINT_DIR / "spark_checkpoint")
    sqlite_db_path: str = str(RESULTS_DIR / "analytics.db")


@dataclass
class PipelineConfig:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    baseline: SensorBaseline = field(default_factory=SensorBaseline)
    generator: GeneratorSettings = field(default_factory=GeneratorSettings)
    thresholds: ThresholdLimits = field(default_factory=ThresholdLimits)
    zscore: ZScoreSettings = field(default_factory=ZScoreSettings)
    isolation_forest: IsolationForestSettings = field(default_factory=IsolationForestSettings)
    streaming: StreamingSettings = field(default_factory=StreamingSettings)


# Global singleton configuration
config = PipelineConfig()
