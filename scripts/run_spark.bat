@echo off
setlocal
echo ========================================================
echo Starting Spark Structured Streaming Processor...
echo ========================================================
set JAVA_HOME=C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot
set HADOOP_HOME=C:\Users\vasud\OneDrive\Desktop\BDA\tools\hadoop
set PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%PATH%
.\venv\Scripts\python.exe streaming\spark_processor.py
