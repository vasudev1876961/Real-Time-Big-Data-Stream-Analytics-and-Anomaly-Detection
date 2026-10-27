@echo off
setlocal
echo =========================================================================
echo Replaying Industrial Machine Telemetry Stream (real_sensor_stream.csv)...
echo =========================================================================
.\venv\Scripts\python.exe producer\dataset_replayer.py --rate 10.0
