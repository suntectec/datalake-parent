package org.example;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class ParameterTest {
    public static void main(String[] args) throws IOException {
        String propertiesFilePath = "flink/application-native/src/main/resources/application.properties";
        ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);

        String ip_addr = parameter.get("ip.address");
        System.out.println(ip_addr);
    }
}
