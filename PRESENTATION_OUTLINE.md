# Presentation Outline & Demonstration Guide

## Project Title:
**Real-Time Big Data Stream Analytics and Anomaly Detection Using Apache Kafka and Spark Structured Streaming**

---

## Slide 1: Title & Introduction
- **Project Title:** Real-Time Big Data Stream Analytics and Anomaly Detection
- **Technologies:** Apache Kafka (KRaft), Apache Spark Structured Streaming, Python, Scikit-Learn, SQLite, Streamlit
- **Domain:** Big Data Analytics · Distributed Streaming Systems · Anomaly Detection
- **Speaker:** Vasudeva

---

## Slide 2: Problem Statement & Motivation
- **The Core Problem:** Batch analytics creates dangerous blind spots. Critical sensor failures (temperature spikes, pressure loss, vibration peaks) must be detected in seconds, not hours.
- **The Streaming Solution:** Ingest continuously, aggregate across sliding and tumbling time windows, detect anomalous patterns in real time, and present live actionable alerts.
- **Key Constraints:** Real distributed streaming technologies (Kafka + Spark) running on commodity hardware without external cloud costs.

---

## Slide 3: End-to-End Pipeline Architecture
- **Data Flow:** Generator → Kafka Broker (:9092) → Spark Structured Streaming → SQLite Analytical Store → Streamlit Live Dashboard (:8501).
- **Decoupled Architecture:** Kafka buffers surges in event rates; Spark consumes at controlled micro-batch intervals; SQLite WAL mode ensures non-blocking concurrent reads and writes.

---

## Slide 4: Anomaly Detection: 3 Analytical Methods
- **Method A — Threshold-Based Detection:** Physical safety bounds ($T > 75^\circ\text{C}, V > 0.65\text{g}, P > 115\text{kPa}$). Ultra-fast, zero-overhead.
- **Method B — Rolling Z-Score:** Adaptive moving window ($N=30$) measuring standard deviations from recent sensor history. Captures baseline drifts.
- **Method C — Isolation Forest:** Unsupervised decision-tree ensemble isolating multivariate anomalies that single thresholds miss.

---

## Slide 5: Experimental Evaluation: 4 Big Data Benchmarks
- **Experiment A (Volume Scalability):** Tested up to 100,000 events. Memory remained bounded at 63.4 MB with linear $O(N)$ execution time.
- **Experiment B (Streaming Throughput):** Evaluated ingestion rates at 100, 500, and 1,000 Hz. Analytical throughput clocked at ~60 evt/s per core with ~16.7 ms latency.
- **Experiment C (Detection Quality):** Threshold achieved 1.000 precision and 0.891 recall; Z-Score demonstrated the theoretical "masking effect" during collective drifts; Isolation Forest delivered zero false positives.
- **Experiment D (Window Size Comparison):** Compared 10s and 30s tumbling against 60s sliding windows for summary freshness and processing overhead.

---

## Slide 6: Verification & Testing
- 14 automated unit, integration, and smoke tests across all modules.
- 100% test pass rate with reproducible random seeds and schema enforcement.

---

## Slide 7: Live Demonstration Script

Follow this sequence during your presentation:

1. **Show Terminal 1 (Kafka Broker):**
   ```powershell
   .\scripts\start_kafka.bat
   ```
   *Explain: Kafka running in KRaft mode (no ZooKeeper needed).*

2. **Show Terminal 2 (Spark Structured Streaming):**
   ```powershell
   .\scripts\run_spark.bat
   ```
   *Explain: Spark ingests Kafka stream in micro-batches and runs the three anomaly detectors.*

3. **Show Terminal 3 (Streamlit Dashboard):**
   ```powershell
   .\scripts\run_dashboard.bat
   ```
   *Open browser at `http://localhost:8501` to show the live glassmorphic dashboard.*

4. **Show Terminal 4 (Event Producer):**
   ```powershell
   .\scripts\run_producer.bat
   ```
   *Watch live metrics jump on the dashboard!*

5. **Trigger Controlled Anomaly:**
   *Click the "Inject Anomaly 🚨" button on the dashboard sidebar.*
   *Point out the immediate red alert, the severity classification, the explanation, and the multi-method flag!*

---

## Slide 8: Limitations & Future Enhancements
- **Latency Model:** Micro-batch processing (default 2s triggers) vs continuous microsecond processing.
- **Scaling:** Single-laptop execution emulates distributed computing; future work includes deploying across a Kubernetes cluster or AWS EMR.
- **Questions & Answers.**
