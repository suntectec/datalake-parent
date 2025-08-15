package org.example.utils;

import com.alibaba.fastjson2.JSONObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
/**
 * @author Jagger
 * @since 2025/8/13 10:42
 */
public class CustomerSqlserverTransactionLogSourceDeserialization implements DebeziumDeserializationSchema<String> {
    /**
     * TransactionLog格式：
     * SourceRecord{
     * 	sourcePartition={server=sqlserver_transaction_log_source},
     * 	sourceOffset={transaction_id=null, event_serial_no=0, commit_lsn=NULL, change_lsn=80}}
     * ConnectRecord{
     * 	topic='sqlserver_transaction_log_source.INV.orders',
     * 	kafkaPartition=null,
     * 	key=Struct{id=4545004},
     * 	keySchema=Schema{sqlserver_transaction_log_source.INV.orders.Key:STRUCT},
     * 	value=Struct{
     * 		after=Struct{id=4545004,order_id=ec6663ab-05b6-46eb-907c-320af106f38d,supplier_id=5230,item_id=55,status=completed,qty=700,net_price=330,issued_at=1753221632607,completed_at=1753221632607,created_at=1753221632607,updated_at=1753264605857},
     * 		source=Struct{version=1.9.8.Final,connector=sqlserver,name=sqlserver_transaction_log_source,ts_ms=0,db=inventory,schema=INV,table=orders,change_lsn=80},
     * 		op=r,
     * 		ts_ms=1755053118372},
     * 	valueSchema=Schema{sqlserver_transaction_log_source.INV.orders.Envelope:STRUCT},
     * 	timestamp=null,
     * 	headers=ConnectHeaders(headers=)}
     * @param sourceRecord
     * @param collector
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // Implement your deserialization logic here
        // For example, convert the SourceRecord to a String and collect it
        String topic = sourceRecord.topic();
        String[] tableInfos = topic.split("\\.");
        String schemaName = tableInfos[1];
        String tableName = tableInfos[2];

        Struct value = (Struct)sourceRecord.value();

        Struct source = value.getStruct("source");
        List<Field> fields = null;
        JSONObject sourceJson = new JSONObject();
        if (source != null) {
            fields = source.schema().fields();
            fields.forEach(field -> {
                Object v = source.get(field.name());
                sourceJson.put(field.name(), v);
            });
        }

        String dbName = sourceJson.getString("db");
        schemaName = sourceJson.getString("schema");
        tableName = sourceJson.getString("table");

        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            fields = before.schema().fields();
            fields.forEach(field -> {
                Object v = before.get(field.name());
                beforeJson.put(field.name(), v);
            });
        }

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            fields = after.schema().fields();
            fields.forEach(field -> {
                Object v = after.get(field.name());
                afterJson.put(field.name(), v);
            });
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        JSONObject result = new JSONObject();
        result.put("dbName",dbName);
        result.put("schemaName",schemaName);
        result.put("tableName",tableName);
        result.put("type",type);
        result.put("before",beforeJson);
        result.put("after",afterJson);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
