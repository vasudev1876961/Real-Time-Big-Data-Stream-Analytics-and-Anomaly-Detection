"""
Experiment C: Anomaly Detection Quality Benchmark.

Compares Threshold-Based Detection, Rolling Z-Score, and Isolation Forest
against known ground-truth labels across simulated normal, point, collective,
and multivariate anomalies. Computes Precision, Recall, F1-Score, False Positive Rate,
and Confusion Matrices, saving plots and results to data/results/.
"""

import sys
import json
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config, RESULTS_DIR
from producer.event_generator import EventGenerator
from streaming.anomaly_detector import UnifiedAnomalyEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [ExpC] %(message)s")
logger = logging.getLogger(__name__)


def run_detection_quality_experiment(n_events: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Execute detection quality benchmark and produce metrics."""
    logger.info(f"Running Experiment C with {n_events} events (seed={seed})...")

    gen = EventGenerator(seed=seed)
    events = gen.generate_batch(n_events)
    df = pd.DataFrame(events)

    engine = UnifiedAnomalyEngine()
    results = [engine.process_event(e) for e in events]
    res_df = pd.DataFrame(results)

    # Ground truth
    y_true = (df["ground_truth_label"] == "anomaly").astype(int)

    methods = {
        "Threshold Detection": res_df["flag_threshold"].astype(int),
        "Rolling Z-Score": res_df["flag_zscore"].astype(int),
        "Isolation Forest": res_df["flag_isolation_forest"].astype(int),
    }

    metrics_records = []
    confusion_matrices = {}

    for name, y_pred in methods.items():
        prec = precision_score(y_true, y_pred, zero_division=0)
        rec = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

        metrics_records.append({
            "Method": name,
            "Precision": round(float(prec), 4),
            "Recall": round(float(rec), 4),
            "F1_Score": round(float(f1), 4),
            "FPR": round(float(fpr), 4),
            "TP": int(tp),
            "FP": int(fp),
            "FN": int(fn),
            "TN": int(tn)
        })
        confusion_matrices[name] = [[int(tn), int(fp)], [int(fn), int(tp)]]

    metrics_df = pd.DataFrame(metrics_records)

    # Save to disk
    csv_path = RESULTS_DIR / "experiment_c_detection_quality.csv"
    json_path = RESULTS_DIR / "experiment_c_detection_quality.json"

    metrics_df.to_csv(csv_path, index=False)
    with open(json_path, "w") as f:
        json.dump({
            "sample_size": n_events,
            "anomaly_rate": float(y_true.mean()),
            "metrics": metrics_records,
            "confusion_matrices": confusion_matrices
        }, f, indent=2)

    logger.info(f"Results saved to {csv_path} and {json_path}")
    print("\n=== Experiment C: Anomaly Detection Quality Benchmark ===")
    print(metrics_df[["Method", "Precision", "Recall", "F1_Score", "FPR"]].to_string(index=False))

    # Generate Publication-Quality Visualizations
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Bar chart of metrics
    metrics_melted = metrics_df.melt(id_vars=["Method"], value_vars=["Precision", "Recall", "F1_Score"], var_name="Metric", value_name="Score")
    sns.barplot(data=metrics_melted, x="Method", y="Score", hue="Metric", ax=axes[0], palette="crest")
    axes[0].set_title("Detection Quality Metric Comparison", fontsize=12, fontweight="bold")
    axes[0].set_ylim(0, 1.05)
    axes[0].grid(axis="y", linestyle="--", alpha=0.5)

    # False positive rate comparison
    sns.barplot(data=metrics_df, x="Method", y="FPR", ax=axes[1], palette="flare")
    axes[1].set_title("False Positive Rate (Lower is Better)", fontsize=12, fontweight="bold")
    axes[1].set_ylim(0, max(0.1, metrics_df["FPR"].max() * 1.3))
    axes[1].grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    plot_path = RESULTS_DIR / "detection_quality_comparison.png"
    plt.savefig(plot_path, dpi=300)
    plt.close()
    logger.info(f"Chart saved to {plot_path}")

    return metrics_df


if __name__ == "__main__":
    run_detection_quality_experiment()
