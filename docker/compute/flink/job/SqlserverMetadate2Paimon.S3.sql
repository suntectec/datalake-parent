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

-- register a SqlServer table in Flink SQL
CREATE TEMPORARY TABLE sqlserver_source (
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    schema_name STRING METADATA  FROM 'schema_name' VIRTUAL,
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    id BIGINT,
    order_id VARCHAR(36),
    supplier_id INT,
    item_id INT,
    status VARCHAR(20),
    qty INT,
    net_price INT,
    issued_at TIMESTAMP,
    completed_at TIMESTAMP,
    spec VARCHAR(1024),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = '192.168.138.15',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'Abcd1234',
    'database-name' = 'inventory',
    'table-name' = 'INV.orders'
);

CREATE TABLE IF NOT EXISTS orders (
    table_name STRING,
    schema_name STRING,
    db_name STRING,
    operation_ts TIMESTAMP_LTZ(3),
    id BIGINT,
    order_id VARCHAR(36),
    supplier_id INT,
    item_id INT,
    status VARCHAR(20),
    qty INT,
    net_price INT,
    issued_at TIMESTAMP,
    completed_at TIMESTAMP,
    spec VARCHAR(1024),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
);

-- required set before submit insert job, otherwise data not observe
-- execution.checkpointing.interval: default - none, The base interval setting. To enable checkpointing, you need to set this value larger than 0.
SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO orders SELECT * FROM sqlserver_source;