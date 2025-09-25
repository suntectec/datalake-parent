CREATE CATALOG paimon_catalog WITH (
    'type'='paimon',
    'warehouse'='s3://lakehouse/paimon/',
    's3.endpoint'='http://192.168.138.15:9000',
    's3.access-key'='minioadmin',
    's3.secret-key'='minioadmin',
    's3.path.style.access'='true'
);

USE CATALOG paimon_catalog;

CREATE DATABASE IF NOT EXISTS inventory;

USE inventory;

CREATE TEMPORARY TABLE orders_kafka_source
(
    id              BIGINT,
    order_id        STRING,
    supplier_id     INT,
    item_id         INT,
    status          STRING,
    qty             INT,
    net_price       INT,
    issued_at       TIMESTAMP,
    completed_at    TIMESTAMP,
    spec            STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _row_kind STRING,
    _ingestion_time TIMESTAMP,
    _process_time   TIMESTAMP,
    _source_time    TIMESTAMP,
    _record_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'    -- reads and writes a Kafka record's timestamp
) WITH (
      'connector' = 'kafka',
      'topic' = 'ods_orders_topic',
      'properties.bootstrap.servers' = '192.168.138.15:9092',
      'properties.group.id' = 'data_group',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json');

CREATE TABLE IF NOT EXISTS orders_from_kafka (
    id              BIGINT,
    order_id        STRING,
    supplier_id     INT,
    item_id         INT,
    status          STRING,
    qty             INT,
    net_price       INT,
    issued_at       TIMESTAMP,
    completed_at    TIMESTAMP,
    spec            STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _row_kind STRING,
    _ingestion_time TIMESTAMP,
    _process_time   TIMESTAMP,
    _source_time    TIMESTAMP,
    _record_time    TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
    );

-- required set before submit insert job, otherwise data not observe
-- execution.checkpointing.interval: default - none, The base interval setting. To enable checkpointing, you need to set this value larger than 0.
-- SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO orders_from_kafka SELECT * FROM orders_kafka_source;