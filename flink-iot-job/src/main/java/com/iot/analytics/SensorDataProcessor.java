package com.iot.analytics;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector; // Added import for KeySelector

import java.sql.Timestamp;
import java.time.Duration;

public class SensorDataProcessor {

    public static void main(String[] args) throws Exception {
        // 1. Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism for simplicity

        // Configure Kafka Source
        String kafkaBootstrapServers = "kafka-1:9093"; // Internal Docker network name for Kafka
        String kafkaTopic = "iot_sensor_data";
        String clickhouseHost = "clickhouse-server"; // Internal Docker network name for ClickHouse
        String clickhousePort = "8123"; // HTTP port for ClickHouse
		//String clickhousePort = "9000"; 
        String clickhouseDatabase = "iot_data"; // Your ClickHouse database name
        String clickhouseUser = "iot_user";     // Your ClickHouse user
        String clickhousePassword = "iot_password"; // Your ClickHouse password

        // Optional: Read from program arguments if submitted via CLI with args
        if (args.length > 0) {
            kafkaBootstrapServers = args[0];
            clickhouseHost = args[1];
            clickhousePort = args[2];
            clickhouseDatabase = args[3];
            clickhouseUser = args[4];
            clickhousePassword = args[5];
        }

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:9092")
                .setTopics("iot_sensor_data")
                .setGroupId("flink-iot-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest()) // Start consuming from the latest offset
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2. Parse JSON and transform to SensorReading object
        DataStream<SensorReading> sensorReadings = kafkaStream
                .map(new MapFunction<String, SensorReading>() {
                    private static final long serialVersionUID = 1L;
                    private transient ObjectMapper objectMapper;

					/*
                    @Override
                    public SensorReading map(String value) throws Exception {
                        if (objectMapper == null) {
                            objectMapper = new ObjectMapper();
                        }
                        JsonNode jsonNode = objectMapper.readTree(value);
                        return new SensorReading(
                                jsonNode.get("device_id").asText(),
                                jsonNode.get("sensor_type").asText(),
                                jsonNode.get("value").asDouble(),
                                new Timestamp(jsonNode.get("timestamp").asLong()), // Assuming timestamp is epoch milliseconds
                                jsonNode.get("location").asText(),
                                jsonNode.get("battery_level").asDouble()
                        );
                    }
					*/
					
					// Inside your MapFunction in SensorDataProcessor.java, around line 62:
					@Override
					public SensorReading map(String value) throws Exception {
						if (objectMapper == null) {
							objectMapper = new ObjectMapper();
						}
						JsonNode jsonNode = objectMapper.readTree(value);

						// Safely extract fields, providing default values or throwing a more informative exception
						String deviceId = jsonNode.has("device_id") && !jsonNode.get("device_id").isNull() ? jsonNode.get("device_id").asText() : null;
						String sensorType = jsonNode.has("sensor_type") && !jsonNode.get("sensor_type").isNull() ? jsonNode.get("sensor_type").asText() : null;
						double sensorValue = jsonNode.has("value") && !jsonNode.get("value").isNull() ? jsonNode.get("value").asDouble() : 0.0; // Default to 0.0 or handle as needed
						long timestampMs = jsonNode.has("timestamp") && !jsonNode.get("timestamp").isNull() ? jsonNode.get("timestamp").asLong() : System.currentTimeMillis(); // Default to current time
						String location = jsonNode.has("location") && !jsonNode.get("location").isNull() ? jsonNode.get("location").asText() : null;
						double batteryLevel = jsonNode.has("battery_level") && !jsonNode.get("battery_level").isNull() ? jsonNode.get("battery_level").asDouble() : 0.0; // Default to 0.0

						// You might want to throw an IllegalArgumentException if critical fields are null
						if (deviceId == null || sensorType == null) {
							throw new IllegalArgumentException("Missing critical fields (device_id or sensor_type) in JSON: " + value);
						}

						return new SensorReading(
							deviceId,
							sensorType,
							sensorValue,
							new Timestamp(timestampMs),
							location,
							batteryLevel
						);
					}		
					
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp().getTime())
                );


        // 3. Aggregate data per device_id and sensor_type in 1-minute tumbling windows
        DataStream<SensorAggregate> minuteAggregates = sensorReadings
                .keyBy(new KeySelector<SensorReading, Tuple2<String, String>>() { // Corrected keyBy with anonymous KeySelector
                    @Override
                    public Tuple2<String, String> getKey(SensorReading reading) throws Exception {
                        return Tuple2.of(reading.getDeviceId(), reading.getSensorType());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 1-minute tumbling windows
                .aggregate(new SensorAggregateFunction()); // Custom aggregate function


        // 4. Sink aggregated data to ClickHouse
// Locate your first JdbcSink.sink() call for minuteAggregates
minuteAggregates.addSink(
    JdbcSink.sink(
        "INSERT INTO sensor_minute_aggregates (event_time, device_id, sensor_type, min_value, max_value, avg_value, reading_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (statement, aggregate) -> {
            statement.setTimestamp(1, aggregate.getEventTime());
            statement.setString(2, aggregate.getDeviceId());
            statement.setString(3, aggregate.getSensorType());
            statement.setDouble(4, aggregate.getMinValue());
            statement.setDouble(5, aggregate.getMaxValue());
            statement.setDouble(6, aggregate.getAvgValue());
            statement.setLong(7, aggregate.getReadingCount());
        },
        // ADD THIS PART: JdbcExecutionOptions
        JdbcExecutionOptions.builder()
            .withBatchSize(100) // Default batch size, you can adjust this
            .withBatchIntervalMs(0) // Immediately flush if batch size is reached
            .withMaxRetries(5) // Retry attempts for failures
            .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:clickhouse://" + clickhouseHost + ":" + clickhousePort + "/" + clickhouseDatabase + "?connect_timeout=30000")
			//.withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
			.withDriverName("ru.yandex.clickhouse.ClickHouseDriver") // CHANGE THIS DRIVER NAME
            .withUsername(clickhouseUser)
            .withPassword(clickhousePassword)
            .build()
    )
).name("ClickHouse Sink - Minute Aggregates");


        // --- Optional: Anomaly Detection Example (Simple Thresholding) ---

        
        DataStream<SensorAnomaly> anomalies = sensorReadings
                .filter(reading -> {
                    // Simple anomaly detection: value outside 0-100 range (example)
                    return reading.getValue() < 0 || reading.getValue() > 100;
                })
                .map(reading -> {
                    String reason = reading.getValue() < 0 ? "Below Threshold" : "Above Threshold";
                    return new SensorAnomaly(
                            reading.getTimestamp(),
                            reading.getDeviceId(),
                            reading.getSensorType(),
                            reading.getValue(),
                            0.0, // Example threshold min
                            100.0, // Example threshold max
                            reason
                    );
                });

// print detected anomalies to TaskManager logs for debugging
anomalies.print().name("Detected Anomalies (DEBUG)");

// Locate your second JdbcSink.sink() call for anomalies
anomalies.addSink(
    JdbcSink.sink(
        "INSERT INTO sensor_anomalies (anomaly_time, device_id, sensor_type, sensor_value, threshold_min, threshold_max, anomaly_reason) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (statement, anomaly) -> {
            statement.setTimestamp(1, anomaly.getAnomalyTime());
            statement.setString(2, anomaly.getDeviceId());
            statement.setString(3, anomaly.getSensorType());
            statement.setDouble(4, anomaly.getSensorValue());
            statement.setDouble(5, anomaly.getThresholdMin());
            statement.setDouble(6, anomaly.getThresholdMax());
            statement.setString(7, anomaly.getAnomalyReason());
        },
        // ADD THIS PART: JdbcExecutionOptions
        JdbcExecutionOptions.builder()
            .withBatchSize(100) // Default batch size, you can adjust this
            .withBatchIntervalMs(0) // Immediately flush if batch size is reached
            .withMaxRetries(5) // Retry attempts for failures
            .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:clickhouse://" + clickhouseHost + ":" + clickhousePort + "/" + clickhouseDatabase + "?connect_timeout=30000")
			//.withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
			.withDriverName("ru.yandex.clickhouse.ClickHouseDriver") // CHANGE THIS DRIVER NAME
            .withUsername(clickhouseUser)
            .withPassword(clickhousePassword)
            .build()
    )
).name("ClickHouse Sink - Anomalies");


        // Execute the Flink job
        env.execute("IoT Sensor Data Processor");
    }

    // --- Data Model Classes ---
    public static class SensorReading {
        public String deviceId;
        public String sensorType;
        public double value;
        public Timestamp timestamp;
        public String location;
        public double batteryLevel;

        public SensorReading() {}

        public SensorReading(String deviceId, String sensorType, double value, Timestamp timestamp, String location, double batteryLevel) {
            this.deviceId = deviceId;
            this.sensorType = sensorType;
            this.value = value;
            this.timestamp = timestamp;
            this.location = location;
            this.batteryLevel = batteryLevel;
        }

        // Getters for Flink's access
        public String getDeviceId() { return deviceId; }
        public String getSensorType() { return sensorType; }
        public double getValue() { return value; }
        public Timestamp getTimestamp() { return timestamp; }
        public String getLocation() { return location; }
        public double getBatteryLevel() { return batteryLevel; }

        @Override
        public String toString() {
            return "SensorReading{" +
                   "deviceId='" + deviceId + '\'' +
                   ", sensorType='" + sensorType + '\'' +
                   ", value=" + value +
                   ", timestamp=" + timestamp +
                   '}';
        }
    }

    public static class SensorAggregate {
        public Timestamp eventTime; // Window end time
        public String deviceId;
        public String sensorType;
        public double minValue;
        public double maxValue;
        public double avgValue;
        public long readingCount;

        public SensorAggregate() {}

        public SensorAggregate(Timestamp eventTime, String deviceId, String sensorType, double minValue, double maxValue, double avgValue, long readingCount) {
            this.eventTime = eventTime;
            this.deviceId = deviceId;
            this.sensorType = sensorType;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.avgValue = avgValue;
            this.readingCount = readingCount;
        }

        // Getters
        public Timestamp getEventTime() { return eventTime; }
        public String getDeviceId() { return deviceId; }
        public String getSensorType() { return sensorType; }
        public double getMinValue() { return minValue; }
        public double getMaxValue() { return maxValue; }
        public double getAvgValue() { return avgValue; }
        public long getReadingCount() { return readingCount; }

        @Override
        public String toString() {
            return "SensorAggregate{" +
                   "eventTime=" + eventTime +
                   ", deviceId='" + deviceId + '\'' +
                   ", sensorType='" + sensorType + '\'' +
                   ", count=" + readingCount +
                   '}';
        }
    }

    public static class SensorAnomaly {
        public Timestamp anomalyTime;
        public String deviceId;
        public String sensorType;
        public double sensorValue;
        public double thresholdMin;
        public double thresholdMax;
        public String anomalyReason;

        public SensorAnomaly() {}

        public SensorAnomaly(Timestamp anomalyTime, String deviceId, String sensorType, double sensorValue, double thresholdMin, double thresholdMax, String anomalyReason) {
            this.anomalyTime = anomalyTime;
            this.deviceId = deviceId;
            this.sensorType = sensorType;
            this.sensorValue = sensorValue;
            this.thresholdMin = thresholdMin;
            this.thresholdMax = thresholdMax;
            this.anomalyReason = anomalyReason;
        }

        // Getters
        public Timestamp getAnomalyTime() { return anomalyTime; }
        public String getDeviceId() { return deviceId; }
        public String getSensorType() { return sensorType; }
        public double getSensorValue() { return sensorValue; }
        public double getThresholdMin() { return thresholdMin; }
        public double getThresholdMax() { return thresholdMax; }
        public String getAnomalyReason() { return anomalyReason; }
    }

    // --- Custom Aggregate Function for Sensor Data ---
    public static class SensorAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<SensorReading, SensorAccumulator, SensorAggregate> {

        @Override
        public SensorAccumulator createAccumulator() {
            return new SensorAccumulator();
        }

        @Override
        public SensorAccumulator add(SensorReading value, SensorAccumulator accumulator) {
            if (accumulator.count == 0) {
                accumulator.min = value.getValue();
                accumulator.max = value.getValue();
            } else {
                accumulator.min = Math.min(accumulator.min, value.getValue());
                accumulator.max = Math.max(accumulator.max, value.getValue());
            }
            accumulator.sum += value.getValue();
            accumulator.count++;
            // Update event time to the latest timestamp in the window, to be used as window end time
            accumulator.eventTime = value.getTimestamp();
            accumulator.deviceId = value.getDeviceId();
            accumulator.sensorType = value.getSensorType();
            return accumulator;
        }

        @Override
        public SensorAggregate getResult(SensorAccumulator accumulator) {
            return new SensorAggregate(
                    accumulator.eventTime, // Use the latest timestamp in the window as event_time
                    accumulator.deviceId,
                    accumulator.sensorType,
                    accumulator.min,
                    accumulator.max,
                    accumulator.sum / accumulator.count,
                    accumulator.count
            );
        }

        @Override
        public SensorAccumulator merge(SensorAccumulator a, SensorAccumulator b) {
            a.sum += b.sum;
            a.count += b.count;
            a.min = Math.min(a.min, b.min);
            a.max = Math.max(a.max, b.max);
            // Take the latest event time for the merged window
            if (b.eventTime.after(a.eventTime)) {
                a.eventTime = b.eventTime;
            }
            // Device ID and Sensor Type should be the same for merged accumulators within a keyBy group
            return a;
        }
    }

    // Accumulator for the AggregateFunction
    public static class SensorAccumulator {
        public Timestamp eventTime;
        public String deviceId;
        public String sensorType;
        public double sum;
        public double min;
        public double max;
        public long count;

        public SensorAccumulator() {
            this.sum = 0;
            this.min = Double.MAX_VALUE;
            this.max = Double.MIN_VALUE;
            this.count = 0;
            this.eventTime = new Timestamp(0); // Initialize with epoch
        }
    }
}