"""
Real-Time Anomaly Detection Engine.

Implements three distinct analytical detection methods:
1. Method A: Threshold-Based Detection (Static Operating Envelope)
2. Method B: Rolling Z-Score (Stateful Statistical Dynamic Window)
3. Method C: Isolation Forest (Trained Multivariate Unsupervised Learning)

Provides individual detectors and a UnifiedAnomalyEngine that compares all three methods
with explainability, severity classification, and ground-truth validation.
"""

import sys
import math
from pathlib import Path
from collections import deque
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import joblib

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config, ThresholdLimits, ZScoreSettings


class ThresholdDetector:
    """Method 1: Static operating limits for temperature, vibration, and pressure."""

    def __init__(self, limits: Optional[ThresholdLimits] = None):
        self.limits = limits or config.thresholds

    def detect(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Check if event violates upper or lower bounds."""
        temp = float(event.get("temperature", 0.0))
        vib = float(event.get("vibration", 0.0))
        press = float(event.get("pressure", 0.0))

        violations = []
        severity = "none"

        # Temperature check
        if temp > self.limits.temp_max:
            excess = temp - self.limits.temp_max
            violations.append(f"Temperature {temp:.1f}°C exceeds max {self.limits.temp_max:.1f}°C (+{excess:.1f}°C)")
            severity = "high" if temp > (self.limits.temp_max * 1.15) else "medium"
        elif temp < self.limits.temp_min:
            violations.append(f"Temperature {temp:.1f}°C below min {self.limits.temp_min:.1f}°C")
            severity = "medium"

        # Vibration check
        if vib > self.limits.vib_max:
            excess = vib - self.limits.vib_max
            violations.append(f"Vibration {vib:.3f}g exceeds max {self.limits.vib_max:.3f}g (+{excess:.3f}g)")
            severity = "high" if vib > (self.limits.vib_max * 1.3) else "medium"
        elif vib < self.limits.vib_min:
            violations.append(f"Vibration {vib:.3f}g below minimum operating threshold {self.limits.vib_min:.3f}g")
            severity = "low"

        # Pressure check
        if press > self.limits.press_max:
            excess = press - self.limits.press_max
            violations.append(f"Pressure {press:.1f}kPa exceeds max {self.limits.press_max:.1f}kPa (+{excess:.1f}kPa)")
            severity = "high" if press > (self.limits.press_max * 1.2) else "medium"
        elif press < self.limits.press_min:
            violations.append(f"Pressure {press:.1f}kPa below min {self.limits.press_min:.1f}kPa")
            severity = "high" if press < (self.limits.press_min * 0.8) else "medium"

        is_anomaly = len(violations) > 0
        return {
            "method": "threshold",
            "is_anomaly": is_anomaly,
            "severity": severity if is_anomaly else "none",
            "reason": "; ".join(violations) if is_anomaly else "Within normal operating envelope",
            "metrics": {"temp": temp, "vib": vib, "press": press}
        }


class RollingZScoreDetector:
    """Method 2: Rolling Z-Score with per-sensor rolling history, zero-std guard, and warm-up."""

    def __init__(self, settings: Optional[ZScoreSettings] = None):
        self.settings = settings or config.zscore
        # History dict: sensor_id -> {feature: deque(maxlen=window_size)}
        self.history: Dict[str, Dict[str, deque]] = {}

    def _get_sensor_history(self, sensor_id: str) -> Dict[str, deque]:
        if sensor_id not in self.history:
            self.history[sensor_id] = {
                "temperature": deque(maxlen=self.settings.window_size),
                "vibration": deque(maxlen=self.settings.window_size),
                "pressure": deque(maxlen=self.settings.window_size),
            }
        return self.history[sensor_id]

    def detect(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Compute rolling Z-scores for each feature and flag outliers exceeding threshold."""
        sensor_id = str(event.get("sensor_id", "unknown"))
        hist = self._get_sensor_history(sensor_id)

        features = ["temperature", "vibration", "pressure"]
        current_values = {f: float(event.get(f, 0.0)) for f in features}

        # Check if warm-up period is complete
        sample_size = len(hist["temperature"])
        if sample_size < self.settings.min_periods:
            # During warm-up, update state and do not flag false alarms
            for f in features:
                hist[f].append(current_values[f])
            return {
                "method": "zscore",
                "is_anomaly": False,
                "severity": "none",
                "reason": f"Warm-up period active ({sample_size}/{self.settings.min_periods} observations)",
                "z_scores": {f: 0.0 for f in features}
            }

        # Calculate Z-score for each feature
        z_scores = {}
        violations = []
        max_abs_z = 0.0

        # Minimum physical resolution noise floors to prevent division by near-zero std
        min_std_floors = {
            "temperature": 0.5,
            "vibration": 0.01,
            "pressure": 0.5,
        }

        for f in features:
            val = current_values[f]
            obs = list(hist[f])
            mean = sum(obs) / len(obs)
            variance = sum((x - mean) ** 2 for x in obs) / len(obs)
            std = math.sqrt(variance)

            # Apply physical noise floor
            floor = min_std_floors.get(f, self.settings.epsilon)
            effective_std = max(std, floor)

            z = (val - mean) / effective_std
            z_scores[f] = round(z, 2)
            abs_z = abs(z)
            if abs_z > max_abs_z:
                max_abs_z = abs_z

            if abs_z >= self.settings.threshold:
                direction = "high" if z > 0 else "low"
                violations.append(f"{f.capitalize()} Z={z:+.2f} ({direction}) exceeds limit {self.settings.threshold:.1f}")

        # Update history with current reading
        for f in features:
            hist[f].append(current_values[f])

        is_anomaly = len(violations) > 0
        severity = "none"
        if is_anomaly:
            if max_abs_z >= 4.5:
                severity = "high"
            elif max_abs_z >= 3.5:
                severity = "medium"
            else:
                severity = "low"

        return {
            "method": "zscore",
            "is_anomaly": is_anomaly,
            "severity": severity,
            "reason": "; ".join(violations) if is_anomaly else "Z-scores within bounds",
            "z_scores": z_scores,
            "max_abs_z": round(max_abs_z, 2)
        }


class IsolationForestDetector:
    """Method 3: Unsupervised multivariate anomaly detection using pre-trained Isolation Forest."""

    def __init__(self, model_path: Optional[str] = None):
        self.model_path = model_path or config.isolation_forest.model_path
        self.pipeline = None
        self._load_model()

    def _load_model(self):
        p = Path(self.model_path)
        if p.exists():
            self.pipeline = joblib.load(p)
        else:
            self.pipeline = None

    def detect(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Perform inference on sensor measurements using fitted pipeline."""
        if self.pipeline is None:
            self._load_model()
            if self.pipeline is None:
                return {
                    "method": "isolation_forest",
                    "is_anomaly": False,
                    "severity": "none",
                    "reason": "Isolation Forest model not loaded",
                    "score": 0.0
                }

        temp = float(event.get("temperature", 0.0))
        vib = float(event.get("vibration", 0.0))
        press = float(event.get("pressure", 0.0))

        import pandas as pd
        X = pd.DataFrame([[temp, vib, press]], columns=["temperature", "vibration", "pressure"])
        pred = self.pipeline.predict(X)[0]  # 1: normal, -1: outlier
        # score_samples or decision_function: lower (negative) values indicate greater abnormality
        decision_score = float(self.pipeline.decision_function(X)[0])

        is_anomaly = (pred == -1)
        severity = "none"
        if is_anomaly:
            if decision_score < -0.15:
                severity = "high"
            elif decision_score < -0.05:
                severity = "medium"
            else:
                severity = "low"

        reason = (
            f"Multivariate outlier detected (Isolation Forest score={decision_score:+.3f})"
            if is_anomaly else "Normal multivariate profile"
        )

        return {
            "method": "isolation_forest",
            "is_anomaly": is_anomaly,
            "severity": severity,
            "reason": reason,
            "anomaly_score": round(decision_score, 4)
        }


class UnifiedAnomalyEngine:
    """Coordinates and compares all three detection methods on incoming sensor streams."""

    def __init__(self):
        self.threshold_detector = ThresholdDetector()
        self.zscore_detector = RollingZScoreDetector()
        self.isolation_forest_detector = IsolationForestDetector()

    def process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all three methods and return unified comparison results."""
        res_thresh = self.threshold_detector.detect(event)
        res_zscore = self.zscore_detector.detect(event)
        res_iforest = self.isolation_forest_detector.detect(event)

        methods_flagged = []
        reasons = []
        severities = []

        if res_thresh["is_anomaly"]:
            methods_flagged.append("threshold")
            reasons.append(f"[Threshold] {res_thresh['reason']}")
            severities.append(res_thresh["severity"])

        if res_zscore["is_anomaly"]:
            methods_flagged.append("zscore")
            reasons.append(f"[Z-Score] {res_zscore['reason']}")
            severities.append(res_zscore["severity"])

        if res_iforest["is_anomaly"]:
            methods_flagged.append("isolation_forest")
            reasons.append(f"[Isolation Forest] {res_iforest['reason']}")
            severities.append(res_iforest["severity"])

        # Determine overall severity (high > medium > low > none)
        if "high" in severities:
            overall_severity = "high"
        elif "medium" in severities:
            overall_severity = "medium"
        elif "low" in severities:
            overall_severity = "low"
        else:
            overall_severity = "none"

        is_any_anomaly = len(methods_flagged) > 0

        return {
            "event_id": event.get("event_id"),
            "timestamp": event.get("timestamp"),
            "sensor_id": event.get("sensor_id"),
            "temperature": float(event.get("temperature", 0.0)),
            "vibration": float(event.get("vibration", 0.0)),
            "pressure": float(event.get("pressure", 0.0)),
            "ground_truth_label": event.get("ground_truth_label", "normal"),
            "anomaly_type": event.get("anomaly_type", "none"),
            # Method-specific flags
            "flag_threshold": res_thresh["is_anomaly"],
            "flag_zscore": res_zscore["is_anomaly"],
            "flag_isolation_forest": res_iforest["is_anomaly"],
            "is_anomaly": is_any_anomaly,
            "detected_by": ",".join(methods_flagged) if methods_flagged else "none",
            "severity": overall_severity,
            "explanation": " | ".join(reasons) if reasons else "Normal operating conditions",
            "z_scores": res_zscore.get("z_scores", {}),
            "iforest_score": res_iforest.get("anomaly_score", 0.0),
        }
