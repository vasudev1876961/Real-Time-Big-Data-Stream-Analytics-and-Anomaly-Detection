"""
Offline Isolation Forest Training for Sensor Anomaly Detection.

Trains an Isolation Forest model on synthetic baseline sensor telemetry and saves
the serialized model pipeline to models/isolation_forest.joblib.
"""

import sys
import logging
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config
from producer.event_generator import EventGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ModelTraining] %(message)s"
)
logger = logging.getLogger(__name__)


def train_isolation_forest(
    n_samples: int = 20000,
    contamination: float = 0.05,
    random_state: int = 42,
    output_path: str = None
) -> Pipeline:
    """Train and persist Isolation Forest model on representative sensor data."""
    output_path = output_path or config.isolation_forest.model_path
    features = config.isolation_forest.features

    logger.info(f"Generating {n_samples} training events using EventGenerator (seed={random_state})...")
    gen = EventGenerator(seed=random_state)
    events = gen.generate_batch(n_samples)
    df = pd.DataFrame(events)

    X = df[features]
    logger.info(f"Training features: {features}")
    logger.info(f"Summary stats:\n{X.describe().round(2)}")

    # Pipeline: StandardScaler -> IsolationForest
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("model", IsolationForest(
            contamination=contamination,
            n_estimators=config.isolation_forest.n_estimators,
            random_state=random_state,
            n_jobs=-1
        ))
    ])

    logger.info("Fitting Isolation Forest model...")
    pipeline.fit(X)

    # Evaluate on training batch
    preds = pipeline.predict(X)  # 1 for inlier (normal), -1 for outlier (anomaly)
    anom_count = (preds == -1).sum()
    logger.info(f"Model identified {anom_count} anomalies out of {n_samples} ({anom_count/n_samples*100:.2f}%)")

    # Save to disk
    joblib.dump(pipeline, output_path)
    logger.info(f"Saved trained Isolation Forest pipeline to: {output_path}")

    return pipeline


if __name__ == "__main__":
    train_isolation_forest()
