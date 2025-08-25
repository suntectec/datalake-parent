# [Flink CDC 3.4.0 - MySQL CDC -Available Metadata Keys for Reading](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.4/docs/connectors/flink-sources/mysql-cdc/)

The following format metadata can be exposed as read-only (VIRTUAL) columns in a table definition.

Key	DataType	Description

table_name	STRING NOT NULL	Name of the table that contain the row.

database_name	STRING NOT NULL	Name of the database that contain the row.

op_ts	TIMESTAMP_LTZ(3) NOT NULL	It indicates the time that the change was made in the database. If the record is read from snapshot of the table instead of the binlog, the value is always 0.

row_kind	STRING NOT NULL	It indicates the row kind of the changelog,Note: The downstream SQL operator may fail to compare due to this new added column when processing the row retraction if the source operator chooses to output the 'row_kind' column for each record. It is recommended to use this metadata column only in simple synchronization jobs. '+I' means INSERT message, '-D' means DELETE message, '-U' means UPDATE_BEFORE message and '+U' means UPDATE_AFTER message.

