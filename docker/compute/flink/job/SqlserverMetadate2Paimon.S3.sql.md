[Flink CDC 3.4.0 - SqlServer CDC - Available MetaData 实践](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.4/zh/docs/connectors/flink-sources/sqlserver-cdc/)

# SqlServerTableSource Available Metadata Keys for Reading

The following format metadata can be exposed as read-only (VIRTUAL) columns in a table definition.

Key	DataType	Description

table_name	STRING NOT NULL	Name of the table that contain the row.

schema_name	STRING NOT NULL	Name of the schema that contain the row.

database_name	STRING NOT NULL	Name of the database that contain the row.

op_ts	TIMESTAMP_LTZ(3) NOT NULL	It indicates the time that the change was made in the database.

If the record is read from snapshot of the table instead of the change stream, the value is always 0.