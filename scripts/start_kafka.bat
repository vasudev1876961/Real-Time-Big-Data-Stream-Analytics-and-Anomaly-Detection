@echo off
setlocal
echo Starting Apache Kafka in KRaft mode...
set JAVA_HOME=C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
set KAFKA_HEAP_OPTS=-Xmx512M -Xms256M
"tools\kafka\bin\windows\kafka-server-start.bat" config\kafka_kraft.properties
