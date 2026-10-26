"""
Experiments A & D: Volume Scalability & Window Size Trade-offs.

Experiment A: Data Volume Scalability (Measures processing time, throughput, and RAM scaling).
Experiment D: Window Size Comparison (Compares 10s, 30s, and 60s/10s slide aggregation windows).
"""

import sys
import time
import json
import tracemalloc
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config, RESULTS_DIR
from producer.event_generator import EventGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [ExpAD] %(message)s")
logger = logging.getLogger(__name__)


def run_volume_scalability_experiment(volumes=(10000, 50000, 100000), seed: int = 42) -> pd.DataFrame:
    """Experiment A: Evaluate scaling behavior across increasing data volumes."""
    logger.info("Starting Experiment A: Data Volume Scalability...")
    results = []

    for n_events in volumes:
        logger.info(f"Testing volume: {n_events:,} events...")
        gen = EventGenerator(seed=seed)

        tracemalloc.start()
        start_t = time.perf_counter()

        # Batch generation
        events = gen.generate_batch(n_events)
        df = pd.DataFrame(events)

        # Baseline aggregations & threshold checking
        agg_temp = df.groupby("sensor_id")["temperature"].agg(["count", "mean", "min", "max"])
        anom_count = (df["ground_truth_label"] == "anomaly").sum()

        elapsed_sec = time.perf_counter() - start_t
        current_mem, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak_mem / (1024 * 1024)
        throughput = n_events / elapsed_sec if elapsed_sec > 0 else 0.0

        results.append({
            "Record_Volume": n_events,
            "Processing_Time_s": round(elapsed_sec, 2),
            "Throughput_evt_s": round(throughput, 1),
            "Peak_Memory_MB": round(peak_mb, 2),
            "Anomalies_Identified": int(anom_count)
        })

    results_df = pd.DataFrame(results)

    # Save results
    csv_path = RESULTS_DIR / "experiment_a_scalability.csv"
    json_path = RESULTS_DIR / "experiment_a_scalability.json"
    results_df.to_csv(csv_path, index=False)
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Experiment A results saved to {csv_path}")
    print("\n=== Experiment A: Data Volume Scalability Results ===")
    print(results_df.to_string(index=False))

    # Visualization
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    sns.lineplot(data=results_df, x="Record_Volume", y="Processing_Time_s", marker="o", ax=axes[0], color="#3b82f6", linewidth=2.5)
    axes[0].set_title("Processing Time vs. Record Volume", fontsize=12, fontweight="bold")
    axes[0].set_xlabel("Number of Events")
    axes[0].set_ylabel("Execution Time (seconds)")
    axes[0].grid(True, linestyle="--", alpha=0.5)

    sns.lineplot(data=results_df, x="Record_Volume", y="Peak_Memory_MB", marker="s", ax=axes[1], color="#10b981", linewidth=2.5)
    axes[1].set_title("Peak Memory Usage vs. Record Volume", fontsize=12, fontweight="bold")
    axes[1].set_xlabel("Number of Events")
    axes[1].set_ylabel("Memory (MB)")
    axes[1].grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    plot_path = RESULTS_DIR / "volume_scalability_benchmark.png"
    plt.savefig(plot_path, dpi=300)
    plt.close()
    logger.info(f"Scalability chart saved to {plot_path}")

    return results_df


def run_window_size_experiment(n_events: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Experiment D: Evaluate trade-offs between window durations and calculation overhead."""
    logger.info("Starting Experiment D: Aggregation Window Size Comparison...")
    gen = EventGenerator(seed=seed)
    events = gen.generate_batch(n_events)
    df = pd.DataFrame(events)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)

    window_configs = [
        {"name": "10s Tumbling", "rule": "10s", "sliding": False},
        {"name": "30s Tumbling", "rule": "30s", "sliding": False},
        {"name": "60s Sliding (10s step)", "rule": "60s", "step": "10s", "sliding": True},
    ]

    results = []
    for cfg in window_configs:
        start_t = time.perf_counter()

        if not cfg["sliding"]:
            # Tumbling window resampling
            resampled = df.groupby("sensor_id").resample(cfg["rule"]).agg({
                "temperature": ["count", "mean", "min", "max"],
                "vibration": "mean",
                "pressure": "mean"
            })
            window_count = len(resampled)
            freshness_delay_sec = int(cfg["rule"].replace("s", ""))
        else:
            # Sliding window rolling
            rolled = df.groupby("sensor_id")[["temperature", "vibration", "pressure"]].rolling(cfg["rule"]).mean()
            window_count = len(rolled)
            freshness_delay_sec = int(cfg["step"].replace("s", ""))

        calc_time_ms = (time.perf_counter() - start_t) * 1000.0

        results.append({
            "Window_Configuration": cfg["name"],
            "Window_Outputs_Generated": window_count,
            "Calculation_Time_ms": round(calc_time_ms, 2),
            "Summary_Freshness_Interval_s": freshness_delay_sec,
            "Overhead_Index": round(calc_time_ms / (window_count if window_count > 0 else 1) * 100, 3)
        })

    results_df = pd.DataFrame(results)

    # Save results
    csv_path = RESULTS_DIR / "experiment_d_windows.csv"
    json_path = RESULTS_DIR / "experiment_d_windows.json"
    results_df.to_csv(csv_path, index=False)
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Experiment D results saved to {csv_path}")
    print("\n=== Experiment D: Window Size Comparison Results ===")
    print(results_df.to_string(index=False))

    # Visualization
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    sns.barplot(data=results_df, x="Window_Configuration", y="Calculation_Time_ms", ax=axes[0], palette="Blues_d")
    axes[0].set_title("Calculation Time Across Window Strategies", fontsize=12, fontweight="bold")
    axes[0].set_ylabel("Duration (ms)")
    axes[0].grid(axis="y", linestyle="--", alpha=0.5)

    sns.barplot(data=results_df, x="Window_Configuration", y="Summary_Freshness_Interval_s", ax=axes[1], palette="Reds_d")
    axes[1].set_title("Summary Freshness Update Interval (Lower is Fresher)", fontsize=12, fontweight="bold")
    axes[1].set_ylabel("Update Interval (seconds)")
    axes[1].grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    plot_path = RESULTS_DIR / "window_size_comparison.png"
    plt.savefig(plot_path, dpi=300)
    plt.close()
    logger.info(f"Window comparison chart saved to {plot_path}")

    return results_df


if __name__ == "__main__":
    run_volume_scalability_experiment()
    run_window_size_experiment()
