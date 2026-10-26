"""
Synthetic Industrial Sensor Event Generator.

Generates realistic timestamped telemetry with:
- Normal Gaussian fluctuation within operating baselines
- Point anomalies: isolated extreme spikes (e.g., sudden temperature jump)
- Collective anomalies: gradual drift or sustained abnormal state across consecutive readings
- Multivariate anomalies: unusual combinations of normal-range values
- Ground-truth labeling for exact evaluation (status, anomaly_type, severity)
"""

import time
import json
import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Generator, List, Optional
from pathlib import Path
import numpy as np

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config, GeneratorSettings, SensorBaseline


class SensorState:
    """Maintains state for a single sensor to generate realistic temporal continuity and collective anomalies."""

    def __init__(self, sensor_id: str, baseline: SensorBaseline):
        self.sensor_id = sensor_id
        self.baseline = baseline
        self.current_temp = baseline.temp_mean
        self.current_vib = baseline.vib_mean
        self.current_press = baseline.press_mean

        # State for active collective (sustained) anomalies
        self.collective_anomaly_active = False
        self.collective_steps_remaining = 0
        self.collective_temp_bias = 0.0
        self.collective_vib_bias = 0.0
        self.collective_press_bias = 0.0


class EventGenerator:
    """Configurable synthetic sensor event stream generator."""

    def __init__(self, settings: Optional[GeneratorSettings] = None, seed: Optional[int] = None):
        self.settings = settings or config.generator
        self.baseline = config.baseline
        self.seed = seed if seed is not None else self.settings.random_seed
        self.rng = np.random.default_rng(self.seed)
        random.seed(self.seed)

        # Initialize sensor states
        self.sensors: Dict[str, SensorState] = {
            s_id: SensorState(s_id, self.baseline) for s_id in self.settings.sensors
        }
        self.event_counter = 0

    def _generate_normal_reading(self, state: SensorState) -> Dict[str, float]:
        """Generate normal reading with slight Markovian auto-correlation around baseline."""
        # Auto-regressive step: 0.8 * previous + 0.2 * new Gaussian draw
        new_temp = 0.75 * state.current_temp + 0.25 * self.rng.normal(self.baseline.temp_mean, self.baseline.temp_std)
        new_vib = 0.75 * state.current_vib + 0.25 * self.rng.normal(self.baseline.vib_mean, self.baseline.vib_std)
        new_press = 0.75 * state.current_press + 0.25 * self.rng.normal(self.baseline.press_mean, self.baseline.press_std)

        # Physics bounds: vibration cannot be negative; pressure > 0
        new_vib = max(0.01, new_vib)
        new_press = max(10.0, new_press)

        state.current_temp = new_temp
        state.current_vib = new_vib
        state.current_press = new_press

        return {
            "temperature": round(float(new_temp), 2),
            "vibration": round(float(new_vib), 4),
            "pressure": round(float(new_press), 2),
        }

    def generate_single_event(self, sensor_id: Optional[str] = None, fixed_timestamp: Optional[str] = None) -> Dict[str, Any]:
        """Generate one timestamped telemetry record with ground truth."""
        self.event_counter += 1
        if sensor_id is None:
            sensor_id = str(self.rng.choice(self.settings.sensors))

        state = self.sensors[sensor_id]
        now_utc = fixed_timestamp or datetime.now(timezone.utc).isoformat()

        # Check if an ongoing collective anomaly is in progress for this sensor
        if state.collective_anomaly_active:
            normal_reading = self._generate_normal_reading(state)
            state.collective_steps_remaining -= 1
            if state.collective_steps_remaining <= 0:
                state.collective_anomaly_active = False

            temp = round(normal_reading["temperature"] + state.collective_temp_bias, 2)
            vib = round(max(0.01, normal_reading["vibration"] + state.collective_vib_bias), 4)
            press = round(max(10.0, normal_reading["pressure"] + state.collective_press_bias), 2)

            return {
                "event_id": f"evt_{self.event_counter:07d}",
                "timestamp": now_utc,
                "sensor_id": sensor_id,
                "temperature": temp,
                "vibration": vib,
                "pressure": press,
                "ground_truth_label": "anomaly",
                "anomaly_type": "collective",
                "anomaly_severity": "high",
            }

        # Determine if a new anomaly should be triggered
        roll = self.rng.uniform(0.0, 1.0)
        if roll < self.settings.anomaly_probability:
            anomaly_kind_roll = self.rng.uniform(0.0, 1.0)

            if anomaly_kind_roll < self.settings.point_anomaly_ratio:
                # 1. Point Anomaly: sudden extreme spike or drop
                spike_feature = self.rng.choice(["temperature", "vibration", "pressure"])
                normal_reading = self._generate_normal_reading(state)

                if spike_feature == "temperature":
                    normal_reading["temperature"] = round(float(self.rng.uniform(85.0, 115.0)), 2)
                    severity = "high"
                elif spike_feature == "vibration":
                    normal_reading["vibration"] = round(float(self.rng.uniform(0.75, 1.40)), 4)
                    severity = "high"
                else:
                    normal_reading["pressure"] = round(float(self.rng.choice([
                        self.rng.uniform(125.0, 150.0),
                        self.rng.uniform(40.0, 70.0)
                    ])), 2)
                    severity = "medium"

                return {
                    "event_id": f"evt_{self.event_counter:07d}",
                    "timestamp": now_utc,
                    "sensor_id": sensor_id,
                    "temperature": normal_reading["temperature"],
                    "vibration": normal_reading["vibration"],
                    "pressure": normal_reading["pressure"],
                    "ground_truth_label": "anomaly",
                    "anomaly_type": "point",
                    "anomaly_severity": severity,
                }

            elif anomaly_kind_roll < (self.settings.point_anomaly_ratio + self.settings.collective_anomaly_ratio):
                # 2. Collective Anomaly: Start a sustained anomaly sequence (5-15 events)
                state.collective_anomaly_active = True
                state.collective_steps_remaining = int(self.rng.integers(6, 16))
                # Add sustained offset
                state.collective_temp_bias = float(self.rng.uniform(15.0, 28.0))
                state.collective_vib_bias = float(self.rng.uniform(0.15, 0.35))
                state.collective_press_bias = float(self.rng.uniform(12.0, 25.0))

                normal_reading = self._generate_normal_reading(state)
                temp = round(normal_reading["temperature"] + state.collective_temp_bias, 2)
                vib = round(normal_reading["vibration"] + state.collective_vib_bias, 4)
                press = round(normal_reading["pressure"] + state.collective_press_bias, 2)

                return {
                    "event_id": f"evt_{self.event_counter:07d}",
                    "timestamp": now_utc,
                    "sensor_id": sensor_id,
                    "temperature": temp,
                    "vibration": vib,
                    "pressure": press,
                    "ground_truth_label": "anomaly",
                    "anomaly_type": "collective",
                    "anomaly_severity": "high",
                }

            else:
                # 3. Multivariate Anomaly: Each reading is individually in marginal range,
                # but their combination is abnormal (e.g., high vibration with elevated temperature and low pressure)
                normal_reading = self._generate_normal_reading(state)
                # Elevated but below hard threshold
                abnormal_temp = round(float(self.baseline.temp_mean + 1.8 * self.baseline.temp_std), 2)
                abnormal_vib = round(float(self.baseline.vib_mean + 1.9 * self.baseline.vib_std), 4)
                abnormal_press = round(float(self.baseline.press_mean - 1.8 * self.baseline.press_std), 2)

                return {
                    "event_id": f"evt_{self.event_counter:07d}",
                    "timestamp": now_utc,
                    "sensor_id": sensor_id,
                    "temperature": abnormal_temp,
                    "vibration": abnormal_vib,
                    "pressure": abnormal_press,
                    "ground_truth_label": "anomaly",
                    "anomaly_type": "multivariate",
                    "anomaly_severity": "medium",
                }

        # Normal event
        reading = self._generate_normal_reading(state)
        return {
            "event_id": f"evt_{self.event_counter:07d}",
            "timestamp": now_utc,
            "sensor_id": sensor_id,
            "temperature": reading["temperature"],
            "vibration": reading["vibration"],
            "pressure": reading["pressure"],
            "ground_truth_label": "normal",
            "anomaly_type": "none",
            "anomaly_severity": "none",
        }

    def stream_events(self, rate_hz: Optional[float] = None, max_events: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """Continuously generate events at the specified frequency (events per second)."""
        rate = rate_hz if rate_hz is not None else self.settings.default_rate_hz
        delay = 1.0 / rate if rate > 0 else 0.0
        count = 0

        while max_events is None or count < max_events:
            start_t = time.perf_counter()
            event = self.generate_single_event()
            yield event
            count += 1

            if delay > 0:
                elapsed = time.perf_counter() - start_t
                sleep_t = delay - elapsed
                if sleep_t > 0:
                    time.sleep(sleep_t)

    def generate_batch(self, count: int) -> List[Dict[str, Any]]:
        """Fast generation of a batch of events without delay (for benchmarking/training)."""
        return [self.generate_single_event() for _ in range(count)]


if __name__ == "__main__":
    generator = EventGenerator(seed=42)
    print("Generating 10 sample sensor events:")
    for _ in range(10):
        evt = generator.generate_single_event()
        print(json.dumps(evt))
