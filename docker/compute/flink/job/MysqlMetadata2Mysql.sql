USE CATALOG default_catalog;

USE default_database;

create temporary table orders_source (
    -- db_name       STRING METADATA FROM 'database_name' VIRTUAL,
    -- table_name    STRING METADATA FROM 'table_name' VIRTUAL,
    -- operation_ts  TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    -- operation     STRING METADATA FROM 'row_kind' VIRTUAL,
    order_number INT,
    order_date DATE,
    purchaser INT,
    quantity INT,
    product_id INT,
    PRIMARY KEY (order_number) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'connection.pool.size' = '10',
    'hostname' = '192.168.138.15',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'inventory',
    'table-name' = 'orders',
    'server-time-zone'='Asia/Macau'
);

-- need create mysql sink table first
CREATE TABLE orders_sink (
    order_number INT,
    order_date DATE,
    purchaser INT,
    quantity INT,
    product_id INT,
    PRIMARY KEY (order_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.138.15:3306/inventory',
    'username' = 'root',
    'password' = '123456',
    'table-name' = 'orders1'
);

SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO orders_sink SELECT * FROM orders_source;
