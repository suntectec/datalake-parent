CREATE TABLE sqlserver_source (
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
      'username' = 'SA',
      'password' = 'Abcd1234',
      'database-name' = 'inventory',
      'table-name' = 'INV.orders'
      );

CREATE TABLE paimon_sink (
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
      'connector' = 'paimon',
      'path' = 'file:///tmp/paimon/warehouse',
      'auto-create' = 'true'
      );

SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO paimon_sink SELECT * FROM sqlserver_source;