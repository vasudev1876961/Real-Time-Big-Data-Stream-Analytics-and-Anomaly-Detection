@echo off
setlocal
echo =========================================================================
echo    STOPPING ALL BIG DATA STREAM ANALYTICS SERVICES
echo =========================================================================
echo.

:: 1. Stop Kafka broker via stop script
echo Stopping Apache Kafka broker...
call "%~dp0stop_kafka.bat"

:: 2. Terminate running python processes related to BDA
echo Terminating any active producer, spark, and streamlit processes...
powershell -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*spark_processor.py*' -or $_.CommandLine -like '*kafka_producer.py*' -or $_.CommandLine -like '*dashboard\app.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"

echo.
echo =========================================================================
echo  All services have been shut down cleanly.
echo =========================================================================
pause
