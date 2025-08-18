package org.example.function;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Collection;
import java.util.Set;

/**
 * @author Jagger
 * @since 2025/8/13 15:36
 */
public class CustomerDebezium2TableMapFunction implements MapFunction<String, String> {

    @Override
    public String map(String s) throws Exception {
        JSONObject jsonObject = JSON.parseObject(s);

        JSONObject beforeJson = new JSONObject();
        if (jsonObject.getJSONObject("before") != null) {
            beforeJson = jsonObject.getJSONObject("before");
        }
        JSONObject afterJson = new JSONObject();
        if (jsonObject.getJSONObject("after") != null) {
            afterJson = jsonObject.getJSONObject("after");
        }
        JSONObject sourceJson = new JSONObject();
        if (jsonObject.getJSONObject("source") != null) {
            sourceJson = jsonObject.getJSONObject("source");
        }
        JSONObject transactionJson = new JSONObject();
        if (transactionJson.getJSONObject("transaction") != null) {
            transactionJson = jsonObject.getJSONObject("transaction");
        }
        String op = jsonObject.getString("op");
        String ts_ms = jsonObject.getString("ts_ms");

        JSONObject result = new JSONObject();
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("source", sourceJson);
        result.put("op", op);
        result.put("ts_ms", ts_ms);
        result.put("transaction", transactionJson);

        String dbName = sourceJson.getString("db");
        String schemaName = sourceJson.getString("schema");
        String tableName = sourceJson.getString("table");

        // Determine SQL operation type
        StringBuilder sqlQuery = new StringBuilder();

        String endName = dbName + '.' + schemaName + '.' + tableName;

        if ("r".equals(op) || "c".equals(op)) {
            sqlQuery.append("INSERT INTO ").append(endName).append(' ');
        } else if ("u".equals(op)) {
            sqlQuery.append("UPDATE ").append(endName).append(" SET ");
        } else if ("d".equals(op)) {
            sqlQuery.append("DELETE FROM ").append(endName).append(" WHERE ");
        } else {
            sqlQuery.append("UNKNOWN operation type");
        }

        String columns = columnsGenerator(afterJson);
        sqlQuery.append(columns);

        result.put("sql", sqlQuery.toString());

        return result.toJSONString();
    }

    private static String columnsGenerator(JSONObject jsonObject) {
        StringBuilder columns = new StringBuilder();

        columns.append("(");

        Set<String> keys = jsonObject.keySet();
        for (String key : keys) {
            if (columns.length() > 1) {
                columns.append(", ");
            }
            columns.append(key);
        }

        columns.append(") VALUES (");

        Collection<Object> values = jsonObject.values();
        int length = 0;
        for (Object value : values) {
            if (value instanceof String) {
                // columns.append(" STRING");
                columns.append('"').append(value).append('"');
            } else if (value instanceof Integer || value instanceof Long) {
                // columns.append(" BIGINT");
                columns.append(value);
            } else if (value instanceof Double || value instanceof Float) {
                // columns.append(" DOUBLE");
                columns.append(value);
            } else if (value instanceof Boolean) {
                // columns.append(" BOOLEAN");
                columns.append(value);
            } else {
                // Default type for unknown types
                columns.append(value);
            }
            length+=1;
            if (length != values.size()) {
                columns.append(", ");
            }
        }

        columns.append(")");
        return columns.toString();
    }

    public static void main(String[] args) {
        String after = "{\n" +
                "        \"id\": 4589252,\n" +
                "        \"order_id\": \"910ece6f-eb9f-450e-897e-9558ff0e7a4e\",\n" +
                "        \"supplier_id\": 5406,\n" +
                "        \"item_id\": 67,\n" +
                "        \"status\": \"delivered\",\n" +
                "        \"qty\": 600,\n" +
                "        \"net_price\": 560,\n" +
                "        \"issued_at\": 1753244160897,\n" +
                "        \"completed_at\": 1753244160897,\n" +
                "        \"spec\": null,\n" +
                "        \"created_at\": 1753244160897,\n" +
                "        \"updated_at\": 1753270536167\n" +
                "    }";
        System.out.println(columnsGenerator(JSON.parseObject(after)));
    }
}
