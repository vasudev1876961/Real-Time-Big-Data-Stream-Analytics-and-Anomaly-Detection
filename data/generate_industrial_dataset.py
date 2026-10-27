"""
Public / Historical Industrial Sensor Dataset Generator.

Generates a realistic multi-sensor telemetry log modeled after real-world
industrial machinery (e.g., pumps, compressors, and CNC motors).
Features:
- Operating cycles (idle, ramp-up, steady-state, peak load)
- Multi-axis vibration, bearing temperature, hydraulic oil pressure
- Realistically labeled anomaly episodes:
  * Stage 1: Bearing wear & vibration harmonic escalation
  * Stage 2: Thermal runaway due to cooling jacket failure
  * Stage 3: Pressure drop due to hydraulic seal rupture
"""

import sys
import time
import math
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import DATA_DIR


def generate_industrial_dataset(n_samples: int = 5000, output_path: Optional[Path] = None) -> pd.DataFrame:
    """Generate realistic physical machine telemetry time-series."""
    output_path = output_path or (DATA_DIR / "real_sensor_stream.csv")
    rng = np.random.default_rng(2026)

    base_time = datetime(2026, 10, 24, 8, 0, 0, tzinfo=timezone.utc)
    records = []

    sensors = ["pump_unit_01", "pump_unit_02", "compressor_01"]

    # Machine baseline parameters
    state = {
        s: {
            "temp": 42.0 + rng.uniform(-2, 2),
            "vib": 0.32 + rng.uniform(-0.03, 0.03),
            "press": 102.5 + rng.uniform(-1, 1),
            "cycle_phase": 0.0
        }
        for s in sensors
    }

    print(f"Generating {n_samples} real-world industrial machine telemetry records...")

    for i in range(n_samples):
        timestamp = base_time + timedelta(seconds=i * 2)
        sensor_id = sensors[i % len(sensors)]
        s_state = state[sensor_id]

        # Natural diurnal / load cycle (sine wave variation)
        s_state["cycle_phase"] += 0.005
        cycle_load = math.sin(s_state["cycle_phase"])

        # Base physical readings with slight Markovian persistence
        s_state["temp"] = 0.85 * s_state["temp"] + 0.15 * (45.0 + 3.0 * cycle_load + rng.normal(0, 0.6))
        s_state["vib"] = 0.85 * s_state["vib"] + 0.15 * (0.35 + 0.04 * cycle_load + rng.normal(0, 0.015))
        s_state["press"] = 0.85 * s_state["press"] + 0.15 * (101.3 + 2.5 * cycle_load + rng.normal(0, 0.4))

        temp = round(float(s_state["temp"]), 2)
        vib = round(float(max(0.01, s_state["vib"])), 4)
        press = round(float(max(10.0, s_state["press"])), 2)

        ground_truth = "normal"
        anomaly_type = "none"
        severity = "none"

        # Controlled anomaly episodes in the dataset:
        # Episode 1: Bearing damage in pump_unit_01 (records 1000 to 1250) -> Vibration anomaly
        if sensor_id == "pump_unit_01" and 1000 <= i <= 1250:
            vib = round(vib + float(rng.uniform(0.40, 0.85)), 4)
            ground_truth = "anomaly"
            anomaly_type = "collective"
            severity = "high"

        # Episode 2: Overheating in compressor_01 (records 2200 to 2400) -> Thermal runaway
        elif sensor_id == "compressor_01" and 2200 <= i <= 2400:
            temp = round(temp + float(rng.uniform(32.0, 55.0)), 2)
            ground_truth = "anomaly"
            anomaly_type = "point" if (i % 10 == 0) else "collective"
            severity = "high"

        # Episode 3: Hydraulic pressure cavitation in pump_unit_02 (records 3500 to 3700) -> Pressure drop
        elif sensor_id == "pump_unit_02" and 3500 <= i <= 3700:
            press = round(max(15.0, press - float(rng.uniform(25.0, 48.0))), 2)
            vib = round(vib + float(rng.uniform(0.12, 0.28)), 4)
            ground_truth = "anomaly"
            anomaly_type = "multivariate"
            severity = "medium"

        records.append({
            "event_id": f"ind_{i:07d}",
            "timestamp": timestamp.isoformat(),
            "sensor_id": sensor_id,
            "temperature": temp,
            "vibration": vib,
            "pressure": press,
            "ground_truth_label": ground_truth,
            "anomaly_type": anomaly_type,
            "anomaly_severity": severity
        })

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)
    print(f"Industrial dataset written to: {output_path}")
    print(f"Total rows: {len(df)} | Anomalies: {(df['ground_truth_label'] == 'anomaly').sum()} ({(df['ground_truth_label'] == 'anomaly').mean()*100:.1f}%)")
    return df


if __name__ == "__main__":
    generate_industrial_dataset()
