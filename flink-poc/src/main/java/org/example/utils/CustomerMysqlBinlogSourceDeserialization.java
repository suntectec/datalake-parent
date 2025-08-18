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
public class CustomerMysqlBinlogSourceDeserialization implements DebeziumDeserializationSchema<String> {
    /**
     * BINLOG格式：
     * SourceRecord{
     * 	sourcePartition={server=mysql_binlog_source},
     * 	sourceOffset={transaction_id=null, ts_sec=1752464336, file=mysql-bin.000002, pos=3092028, row=9, server_id=223344, event=2}}
     * ConnectRecord{
     * 	topic='mysql_binlog_source.inventory.products', kafkaPartition=null, key=Struct{id=109},
     * 	keySchema=Schema{mysql_binlog_source.inventory.products.Key:STRUCT},
     * 	value=Struct{
     * 		after=Struct{id=109,name=spare tire,description=24 inch spare tire,weight=22.2},
     * 		source=Struct{version=1.9.8.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1752464336000,db=inventory,table=products,server_id=223344,file=mysql-bin.000002,pos=3092169,row=8,thread=7},
     * 		op=c,
     * 		ts_ms=1755057652724},
     * 	valueSchema=Schema{mysql_binlog_source.inventory.products.Envelope:STRUCT},
     * 	timestamp=null,
     * 	headers=ConnectHeaders(headers=)}
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {
        // Implement your deserialization logic here
        // For example, convert the SourceRecord to a String and collect it
        String topic = sourceRecord.topic();
        String[] tableInfos = topic.split("\\.");
        String tableName = tableInfos[2];
        String dbName = tableInfos[1];

        Struct value = (Struct)sourceRecord.value();
        Struct before = value.getStruct("before");
        List<Field> fields;
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
