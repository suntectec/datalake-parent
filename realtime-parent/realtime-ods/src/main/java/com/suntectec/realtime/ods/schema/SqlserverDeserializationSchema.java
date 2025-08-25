package com.suntectec.realtime.ods.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.jose4j.json.internal.json_simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义反序列化格式，将数据按照标准统一数据输出
 *
 * @author Jagger
 * @since 2025/8/22 15:32
 */
public class SqlserverDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = -1L;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        /** String topic = sourceRecord.topic();
        String[] split = topic.split("[.]");
        String database = split[1];
        String table = split[2];
        resultMap.put("db", database);
        resultMap.put("tableName", table); */
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //获取数据本身
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");
        Struct before = struct.getStruct("before");
        String op = operation.name();
        resultMap.put("op", op);
        // 修复 db name
        Struct source = struct.getStruct("source");
        String db = source.getString("db");
        String db_schema = source.getString("schema");
        String table = source.getString("table");
        resultMap.put("dbName", db);
        resultMap.put("schemaName", db_schema);
        resultMap.put("tableName", table);

        // 新增,更新或者初始化
        if (op.equals(Envelope.Operation.CREATE.name()) || op.equals(Envelope.Operation.READ.name()) || op.equals(Envelope.Operation.UPDATE.name())) {
            JSONObject afterJson = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                for (Field field : schema.fields()) {
                    afterJson.put(field.name(), after.get(field.name()));
                }
                resultMap.put("after", afterJson);
            }
        }

        // 删除
        if (op.equals(Envelope.Operation.DELETE.name())) {
            JSONObject beforeJson = new JSONObject();
            if (before != null) {
                Schema schema = before.schema();
                for (Field field : schema.fields()) {
                    beforeJson.put(field.name(), before.get(field.name()));
                }
                resultMap.put("before", beforeJson);
            }
        }

        collector.collect(JSON.toJSONString(resultMap, JSONWriter.Feature.FieldBased, JSONWriter.Feature.LargeObject));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
