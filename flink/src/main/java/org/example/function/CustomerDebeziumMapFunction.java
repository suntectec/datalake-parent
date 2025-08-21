package org.example.function;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Jagger
 * @since 2025/8/13 15:24
 */
public class CustomerDebeziumMapFunction implements MapFunction<String, String> {

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

        return result.toJSONString();
    }
}
