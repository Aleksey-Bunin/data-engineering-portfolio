-- Create database
CREATE DATABASE IF NOT EXISTS network;

-- Create Kafka Engine table (reads from Kafka topic)
CREATE TABLE IF NOT EXISTS network.kafka_events (
    event_id String,
    timestamp DateTime,
    event_type String,
    base_station_id String,
    region String,
    error_code String,
    call_duration Int32,
    data_usage_mb Float32
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'network_events',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow';

-- Create main storage table
CREATE TABLE IF NOT EXISTS network.events (
    event_id String,
    timestamp DateTime,
    event_type String,
    base_station_id String,
    region String,
    error_code String,
    call_duration Int32,
    data_usage_mb Float32,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, event_id);

-- Create materialized view to auto-insert from Kafka
CREATE MATERIALIZED VIEW IF NOT EXISTS network.events_mv TO network.events AS
SELECT 
    event_id,
    timestamp,
    event_type,
    base_station_id,
    region,
    error_code,
    call_duration,
    data_usage_mb
FROM network.kafka_events;

-- Aggregation tables for Airflow pipelines

-- 5-minute health check aggregation
CREATE TABLE IF NOT EXISTS network.health_5min (
    time_window DateTime,
    total_events Int64,
    error_count Int64,
    error_rate Float32,
    avg_call_duration Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY time_window;

-- 30-minute call quality aggregation
CREATE TABLE IF NOT EXISTS network.call_quality_30min (
    time_window DateTime,
    region String,
    total_calls Int64,
    dropped_calls Int64,
    drop_rate Float32,
    avg_call_duration Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (time_window, region);

-- Hourly traffic analysis aggregation
CREATE TABLE IF NOT EXISTS network.traffic_hourly (
    time_window DateTime,
    base_station_id String,
    region String,
    total_data_mb Float32,
    peak_usage_mb Float32,
    active_sessions Int64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (time_window, base_station_id);

-- Daily SLA aggregation
CREATE TABLE IF NOT EXISTS network.sla_daily (
    date Date,
    region String,
    total_events Int64,
    error_events Int64,
    uptime_percentage Float32,
    sla_met Boolean,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date, region);
