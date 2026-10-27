@echo off
setlocal
title Big Data Analytics Pipeline Launcher

echo =========================================================================
echo    REAL-TIME BIG DATA STREAM ANALYTICS & ANOMALY DETECTION PIPELINE
echo =========================================================================
echo.
echo Launching all 4 pipeline components in separate dedicated windows:
echo   [1/4] Apache Kafka Broker (KRaft mode :9092)
echo   [2/4] Spark Structured Streaming Processor
echo   [3/4] Streamlit Live Analytics Dashboard
echo   [4/4] Real-time Sensor Telemetry Producer
echo.
echo =========================================================================

set BDA_HOME=%~dp0..
cd /d "%BDA_HOME%"

:: 1. Launch Kafka KRaft Broker
echo [Step 1/4] Starting Apache Kafka Broker...
start "Kafka-Broker-:9092" cmd /k "title [1/4] Apache Kafka KRaft Broker && scripts\start_kafka.bat"

echo Waiting 7 seconds for Kafka to initialize...
timeout /t 7 /nobreak > nul

:: 2. Launch Spark Structured Streaming Processor
echo [Step 2/4] Starting Apache Spark Structured Streaming Engine...
start "Spark-Streaming-Processor" cmd /k "title [2/4] Spark Structured Streaming Engine && scripts\run_spark.bat"

echo Waiting 5 seconds for Spark session initialization...
timeout /t 5 /nobreak > nul

:: 3. Launch Streamlit Live Dashboard
echo [Step 3/4] Launching Streamlit Live Dashboard...
start "Streamlit-Dashboard" cmd /k "title [3/4] Streamlit Live Analytics Dashboard && scripts\run_dashboard.bat"

echo Waiting 3 seconds...
timeout /t 3 /nobreak > nul

:: 4. Launch Telemetry Producer
echo [Step 4/4] Starting Real-Time Sensor Telemetry Producer...
start "Sensor-Telemetry-Producer" cmd /k "title [4/4] Sensor Telemetry Producer && scripts\run_producer.bat"

echo.
echo =========================================================================
echo  ALL 4 PIPELINE SERVICES ARE RUNNING!
echo  Dashboard URL: http://localhost:8501
echo  To shut down all services cleanly, run: scripts\stop_all.bat
echo =========================================================================
pause
