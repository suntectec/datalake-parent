# Mssql

set alias in bashrc

```shell
echo "alias dmssql='cd /opt/poc-allin1/docker/data/mssql && docker compose -f docker-compose.yml '" >> ~/.bashrc

source ~/.bashrc
```

SqlServer 

1、启动时，开启 Agent

2、开启库 CDC

3、开启表 CDC

```sql
-- ============= SQL Server CDC enable ================
-- Enable SqlServer Agent before starting sqlserver
-- create a user only for read
-- optional CDC cleanup strategy defoult 3 days
EXEC sys.sp_cdc_change_job
     @job_type = 'cleanup',
     @retention = 43200 -- 单位：分钟（30天）

-- 1. 为数据库启用 CDC
-- Enable CDC for a Database
USE test
GO
EXEC sys.sp_cdc_enable_db
GO

-- 2. 为表启用 CDC
-- Enable CDC for a Table
USE test
GO
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name   = N'users',
     @role_name     = NULL,
     @supports_net_changes = 0
GO
```

Confirm CDC Enable:

```sql
-- database
select *
from sys.databases where is_cdc_enabled = 1;

-- table
select name, is_tracked_by_cdc
from sys.tables where is_tracked_by_cdc = '1'
```