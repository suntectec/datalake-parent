USE CATALOG default_catalog;

CREATE CATALOG s3_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 's3://warehouse/paimon',
    's3.endpoint'='http://192.168.138.15:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin'
);

USE CATALOG s3_catalog;

CREATE DATABASE IF NOT EXISTS my_database;

USE my_database;

CREATE TABLE IF NOT EXISTS myproducts (
    id INT PRIMARY KEY NOT ENFORCED,
    name VARCHAR,
    price DECIMAL(10, 2)
);

create temporary table products (
    id INT,
    name VARCHAR,
    price DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'connection.pool.size' = '10',
    'hostname' = '192.168.138.15',
    'port' = '13306',
    'username' = 'root',
    'password' = 'rootpassword',
    'database-name' = 'mydatabase',
    'table-name' = 'products'
);

SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO myproducts (id,name) SELECT id, name FROM products;