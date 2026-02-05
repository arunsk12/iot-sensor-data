-- Use the database specified in docker-compose.yml (iot_data)

CREATE DATABASE IF NOT EXISTS iot_data;

USE iot_data;

DROP TABLE IF EXISTS iot_data.sensor_anomalies;
DROP TABLE IF EXISTS iot_data.sensor_minute_aggregates;
DROP TABLE IF EXISTS iot_data.sensor_raw_readings; 

-- Table for raw sensor data (optional, useful for debugging/auditing)
-- This table will store every incoming sensor reading
CREATE TABLE IF NOT EXISTS sensor_raw_readings (
    device_id String,
    sensor_type String,
    value Float64,
    timestamp DateTime('UTC'),
    location String,
    battery_level Float64
) ENGINE = MergeTree()
ORDER BY (device_id, sensor_type, timestamp)
TTL timestamp + INTERVAL 1 DAY -- Optional: Data will expire after 1 day to save space
SETTINGS index_granularity = 8192;

-- Table for minute-level aggregated sensor data
-- Flink will aggregate raw readings and write to this table
CREATE TABLE IF NOT EXISTS sensor_minute_aggregates (
    event_time DateTime('UTC'),
    device_id String,
    sensor_type String,
    min_value Float64,
    max_value Float64,
    avg_value Float64,
    reading_count UInt64
) ENGINE = ReplacingMergeTree(event_time) -- ReplacingMergeTree handles duplicates (useful for Flink's exactly-once if needed)
ORDER BY (device_id, sensor_type, event_time)
SETTINGS index_granularity = 8192;

-- Table for detected anomalies
-- Flink will write anomaly events to this table
CREATE TABLE IF NOT EXISTS sensor_anomalies (
    anomaly_time DateTime('UTC'),
    device_id String,
    sensor_type String,
    sensor_value Float64,
    threshold_min Float64,
    threshold_max Float64,
    anomaly_reason String
) ENGINE = MergeTree()
ORDER BY (device_id, sensor_type, anomaly_time)
SETTINGS index_granularity = 8192;