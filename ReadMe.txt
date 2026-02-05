IoT Simulator: Generates raw JSON data.
Apache Kafka: Acts as the high-speed ingest buffer.
Apache Flink: Performs "Stream Processing" (filtering, windowing, and cleaning).
ClickHouse: Stores the data in an OLAP (Analytical) format.
Apache Superset: Visualizes the results via your newly installed clickhouse-connect driver.

$ find . | sed 's|[^/]*/|  |g'
..
  clickhouse-init
    create_tables.sql
    init.sql
  clickhouse_logs
    clickhouse.err.log
    clickhouse.log
  conf
    clickhouse-server
      config.d
        listen_all.xml
        my_custom_server_settings.xml
      config.xml
      users.d
        default-user.xml
      users.xml
    flink-conf.yaml

docker-compose up -d

# Stop services only
docker-compose stop

# Stop and remove containers, networks..
docker-compose down 

# Down and remove volumes
docker-compose down --volumes 

# Down and remove images
docker-compose down --rmi all 

docker volume prune

docker system prune -a

docker-compose ps 
docker-compose ps -a

docker-compose logs -f zookeeper-1

docker-compose logs -f kafka-1

docker-compose logs -f taskmanager

docker exec -it clickhouse-server clickhouse-client

http://localhost:8123


show databases;

use iot_data;

CREATE TABLE IF NOT EXISTS my_test_table (id UInt64,name String) ENGINE = MergeTree() ORDER BY id;

INSERT INTO my_test_table VALUES (1, 'Test Item 1');

SELECT * FROM my_test_table;


select * from sensor_raw_readings

------------------------------------------------------


docker exec -it iot-project-kafka-1-1 bash

kafka-topics --bootstrap-server localhost:9092 --list

kafka-topics --bootstrap-server localhost:9092 --create --topic iot_sensor_data --partitions 1 --replication-factor 1


kafka test 
==========
kafka-console-producer --bootstrap-server localhost:9092 --topic iot_sensor_data 

(input messages)

kafka-console-consumer --bootstrap-server localhost:9092 --topic iot_sensor_data --from-beginning

------------------------------------------------------

docker exec -it iot-project-kafka-1-1 bash

kafka-console-consumer --bootstrap-server localhost:9092 --topic iot_sensor_data --from-beginning --max-messages 5

docker-compose exec iot-project-kafka-1-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic iot_sensor_data \
  --from-beginning \
  --max-messages 5
  
------------------------------------------------------

For Zookeeper: 

	docker exec -it iot-project-zookeeper-1-1 bash

	/usr/bin/zookeeper-shell localhost:2181


For Kafka: 

	docker exec -it iot-project-kafka-1-1 bash 

	/bin/kafka-topics --bootstrap-server localhost:9092 --list
	
------------------------------------------------------	
clickhouse : 

docker exec -it clickhouse-server clickhouse-client

------------------------------------------------------
For Flink : 


(IF REQ)
docker-compose rm -s -f jobmanager taskmanager 


docker-compose up -d jobmanager taskmanager
	
	
docker-compose logs jobmanager
	
docker-compose logs -f taskmanager

The fact that both jobmanager and taskmanager are running and you can successfully access http://localhost:8081/ means that Apache Flink is now fully operational in your Docker environment.


http://localhost:8081/#/overview

http://localhost:8081/#/submit

mvn clean package the flink folder

------------------------------------------------------
iot-project/clickhouse-init/create_tables.sql

docker-compose stop clickhouse-server && docker-compose start clickhouse-server
OR
docker-compose restart clickhouse-server

-- Use the database specified in docker-compose.yml (iot_data)
USE iot_data;

-- Table for raw sensor data (optional, useful for debugging/auditing)
-- This table will store every incoming sensor reading
CREATE TABLE IF NOT EXISTS sensor_raw_readings (device_id String,sensor_type String,value Float64,timestamp DateTime('UTC'),location String,battery_level Float64) ENGINE = MergeTree() ORDER BY (device_id, sensor_type, timestamp) TTL timestamp + INTERVAL 1 DAY SETTINGS index_granularity = 8192;

-- Table for minute-level aggregated sensor data
-- Flink will aggregate raw readings and write to this table
CREATE TABLE IF NOT EXISTS sensor_minute_aggregates (event_time DateTime('UTC'),device_id String, sensor_type String,min_value Float64,max_value Float64,avg_value Float64,reading_count UInt64) ENGINE = ReplacingMergeTree(event_time) ORDER BY (device_id, sensor_type, event_time) SETTINGS index_granularity = 8192;

-- Table for detected anomalies
-- Flink will write anomaly events to this table
CREATE TABLE IF NOT EXISTS sensor_anomalies (anomaly_time DateTime('UTC'),    device_id String,    sensor_type String,    sensor_value Float64,    threshold_min Float64,    threshold_max Float64,    anomaly_reason String) ENGINE = MergeTree()ORDER BY (device_id, sensor_type, anomaly_time)SETTINGS index_granularity = 8192;

SHOW tables;

-----

choco install openjdk maven



Create the Flink Application Project Structure

mkdir -p flink-iot-job/src/main/java/com/iot/analytics

mvn clean package

http://localhost:8081/ - UPLOAD FILNK JAR AND SUBMIT JAR WITH PARALEL 1 OR 2

POWERSHELL 

docker run --rm -v "C:/Arun_HP/iot-project/flink-iot-job:/usr/src/mymaven" -w /usr/src/mymaven maven:3.9.6-eclipse-temurin-11 mvn clean package -U -Dmaven.repo.local=/tmp/.m2/repository


-----


mkdir data-simulator

docker-compose up -d --build

docker-compose down --volumes --remove-orphans (if errs)



-----------------------------------------------------------------------------

more /opt/flink/conf/flink-conf.yaml

sed -i '/jobmanager.rpc.address:/cjobmanager.rpc.address: jobmanager' /opt/flink/conf/flink-conf.yaml



docker run --rm apache/flink:1.18.1-scala_2.12-java11 ls -l conf/flink-conf.yaml


# Temporarily run a Flink TaskManager container to get its default config
docker run --rm apache/flink:1.18.1-scala_2.12-java11 cat conf/flink-conf.yaml > ./conf/flink-conf.yaml


docker-compose down jobmanager taskmanager
docker-compose up -d jobmanager taskmanager




apt-get update && apt-get install -y net-tools 

netstat -tulnp | grep 8123

apt-get update && apt-get install -y telnet # Only if telnet is not found

telnet clickhouse-server 8123




# From inside a Kafka container (e.g., by running docker exec -it <kafka-container-id> bash)

kafka-topics --delete --topic iot_sensor_data --bootstrap-server kafka-1:9092

kafka-topics --bootstrap-server localhost:9092 --delete --topic iot_sensor_data 



# From inside a Kafka container
kafka-topics --create --topic iot_sensor_data --bootstrap-server kafka-1:9092 --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --topic iot_sensor_data --partitions 1 --replication-factor 1






cat clickhouse-init/create_tables.sql | docker exec -i clickhouse-server clickhouse-client


docker compose down -v --rmi all

clickhouse-client --user="iot_user" --port=9000 --host="127.0.0.1" --database="iot_data" 


CREATE DATABASE IF NOT EXISTS iot_data;

USE iot_data;

DROP TABLE IF EXISTS iot_data.sensor_anomalies;
DROP TABLE IF EXISTS iot_data.sensor_minute_aggregates;
DROP TABLE IF EXISTS iot_data.sensor_raw_readings; 

CREATE TABLE IF NOT EXISTS sensor_raw_readings (    device_id String,    sensor_type String,    value Float64,    timestamp DateTime('UTC'),    location String,   battery_level Float64) ENGINE = MergeTree() ORDER BY (device_id, sensor_type, timestamp) TTL timestamp + INTERVAL 1 DAY SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS sensor_minute_aggregates (    event_time DateTime('UTC'),     device_id String,     sensor_type String,     min_value Float64,    max_value Float64,     avg_value Float64,     reading_count UInt64 ) ENGINE = ReplacingMergeTree(event_time) ORDER BY (device_id, sensor_type, event_time) SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS sensor_anomalies (    anomaly_time DateTime('UTC'),    device_id String,    sensor_type String,    sensor_value Float64,    threshold_min Float64,    threshold_max Float64,     anomaly_reason String) ENGINE = MergeTree() ORDER BY (device_id, sensor_type, anomaly_time) SETTINGS index_granularity = 8192;



================================================================================================================================================================================


docker compose restart data-simulator

docker compose exec data-simulator python /app/simulator_fix.py


docker compose exec data-simulator bash


 python /app/simulator_fix.py

=====================================================================================
# 1. Install the driver
docker exec -u root -it superset pip install clickhouse-connect

# 2. Restart the container to refresh the app
docker restart superset


docker-compose build --no-cache superset

docker exec -u root -it superset pip install "sqlalchemy" "clickhouse-connect" "clickhouse-sqlalchemy"

docker exec -it superset python3 -c "import clickhouse_connect; client = clickhouse_connect.get_client(host='clickhouse-server', port=8123, user='iot_user', password='iot_password', database='iot_data'); print('Connected to:', client.server_version)"

pip install clickhouse-connect

docker exec -it superset curl http://clickhouse-server:8123/

clickhouse+http://iot_user:iot_password@clickhouse-server:8123/iot_data

clickhouse://iot_user:iot_password@clickhouse-server:8123/iot_data

clickhousedb://iot_user:iot_password@clickhouse-server:8123/iot_data

clickhousedb+connect://iot_user:iot_password@clickhouse-server:8123/iot_data

docker exec -it superset python -c "import clickhouse_connect; print('Driver is Active')"



 docker compose restart superset

=====================================================================================


SELECT * FROM "iot_data"."sensor_minute_aggregates" order by event_time desc LIMIT 10;

SELECT * FROM "iot_data"."sensor_anomalies" order by anomaly_time desc LIMIT 10;













