package com.suntectec.realtime.ods.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * 入参展平
 *
 * @author Jagger
 * @since 2025/8/18 22:36
 */
public class MyFlatMapFunction extends RichFlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        Map<String, String> parameters = getRuntimeContext().getGlobalJobParameters();
        value = value + "=" + parameters.get(value);
        out.collect(value);
    }

}