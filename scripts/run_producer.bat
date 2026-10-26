@echo off
setlocal
echo ========================================================
echo Starting Kafka Sensor Event Producer...
echo ========================================================
.\venv\Scripts\python.exe producer\kafka_producer.py --rate 10.0
