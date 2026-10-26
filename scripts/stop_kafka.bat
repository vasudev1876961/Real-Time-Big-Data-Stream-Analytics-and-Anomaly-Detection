@echo off
setlocal
echo Stopping Apache Kafka server...
set JAVA_HOME=C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
"tools\kafka\bin\windows\kafka-server-stop.bat"
