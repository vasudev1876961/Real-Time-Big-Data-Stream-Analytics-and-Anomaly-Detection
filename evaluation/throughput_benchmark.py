"""
Experiment B: Streaming Throughput & Ingestion Rate Benchmark.

Evaluates pipeline ingestion and processing throughput across configurable
workload rates (e.g., 100, 500, 1,000 events per second).
Measures achieved throughput, end-to-end processing latency, and generates
visualizations saved to data/results/.
"""

import sys
import time
import json
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
from streaming.anomaly_detector import UnifiedAnomalyEngine
from producer.kafka_producer import SensorKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [ExpB] %(message)s")
logger = logging.getLogger(__name__)


def run_throughput_benchmark(
    target_rates=(100, 500, 1000),
    test_duration_sec: float = 3.0,
    seed: int = 42
) -> pd.DataFrame:
    """Benchmark generator, transmission, and processing throughput at varying rates."""
    logger.info("Starting Experiment B: Streaming Throughput Benchmark...")
    results = []

    engine = UnifiedAnomalyEngine()

    for target_hz in target_rates:
        logger.info(f"--- Benchmarking Target Rate: {target_hz} evt/s ---")
        gen = EventGenerator(seed=seed)

        # 1. Ingestion / Generation Rate measurement
        start_t = time.perf_counter()
        events = []
        delay = 1.0 / target_hz if target_hz > 0 else 0.0

        num_events = int(target_hz * test_duration_sec)
        for _ in range(num_events):
            t0 = time.perf_counter()
            events.append(gen.generate_single_event())
            t_rem = delay - (time.perf_counter() - t0)
            if t_rem > 0:
                time.sleep(t_rem)

        gen_elapsed = time.perf_counter() - start_t
        actual_gen_rate = len(events) / gen_elapsed if gen_elapsed > 0 else 0.0

        # 2. Analytical Processing Throughput (All 3 anomaly detectors combined)
        proc_start = time.perf_counter()
        processed_count = 0
        latencies = []

        for e in events:
            l_start = time.perf_counter()
            engine.process_event(e)
            l_ms = (time.perf_counter() - l_start) * 1000.0
            latencies.append(l_ms)
            processed_count += 1

        proc_elapsed = time.perf_counter() - proc_start
        actual_proc_rate = processed_count / proc_elapsed if proc_elapsed > 0 else 0.0
        avg_latency_ms = float(np.mean(latencies))
        p95_latency_ms = float(np.percentile(latencies, 95))
        p99_latency_ms = float(np.percentile(latencies, 99))

        logger.info(
            f"Target: {target_hz} evt/s | Gen Rate: {actual_gen_rate:.1f} evt/s | "
            f"Processing Rate: {actual_proc_rate:.1f} evt/s | Avg Latency: {avg_latency_ms:.3f}ms"
        )

        results.append({
            "Target_Rate_Hz": target_hz,
            "Total_Events": num_events,
            "Actual_Ingestion_Rate": round(actual_gen_rate, 1),
            "Actual_Processing_Throughput": round(actual_proc_rate, 1),
            "Avg_Latency_ms": round(avg_latency_ms, 3),
            "P95_Latency_ms": round(p95_latency_ms, 3),
            "P99_Latency_ms": round(p99_latency_ms, 3),
        })

    results_df = pd.DataFrame(results)

    # Save to disk
    csv_path = RESULTS_DIR / "experiment_b_throughput.csv"
    json_path = RESULTS_DIR / "experiment_b_throughput.json"
    results_df.to_csv(csv_path, index=False)
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Results saved to {csv_path} and {json_path}")
    print("\n=== Experiment B: Streaming Throughput Benchmark Results ===")
    print(results_df.to_string(index=False))

    # Visualizations
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Throughput comparison
    x = np.arange(len(target_rates))
    width = 0.35
    axes[0].bar(x - width/2, results_df["Target_Rate_Hz"], width, label="Target Rate", color="#94a3b8")
    axes[0].bar(x + width/2, results_df["Actual_Processing_Throughput"], width, label="Processed Throughput", color="#3b82f6")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f"{r} Hz" for r in target_rates])
    axes[0].set_title("Target vs. Achieved Stream Processing Throughput", fontsize=12, fontweight="bold")
    axes[0].set_ylabel("Events / Second")
    axes[0].legend()
    axes[0].grid(axis="y", linestyle="--", alpha=0.5)

    # Latency across percentiles
    sns.lineplot(data=results_df, x="Target_Rate_Hz", y="Avg_Latency_ms", marker="o", ax=axes[1], label="Average Latency", color="#10b981")
    sns.lineplot(data=results_df, x="Target_Rate_Hz", y="P95_Latency_ms", marker="s", ax=axes[1], label="P95 Latency", color="#f59e0b")
    sns.lineplot(data=results_df, x="Target_Rate_Hz", y="P99_Latency_ms", marker="^", ax=axes[1], label="P99 Latency", color="#ef4444")
    axes[1].set_title("Processing Latency vs. Ingestion Load", fontsize=12, fontweight="bold")
    axes[1].set_xlabel("Target Ingestion Rate (Hz)")
    axes[1].set_ylabel("Latency per Event (ms)")
    axes[1].grid(True, linestyle="--", alpha=0.5)
    axes[1].legend()

    plt.tight_layout()
    plot_path = RESULTS_DIR / "streaming_throughput_benchmark.png"
    plt.savefig(plot_path, dpi=300)
    plt.close()
    logger.info(f"Throughput benchmark chart saved to {plot_path}")

    return results_df


if __name__ == "__main__":
    run_throughput_benchmark()
