package org.example.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class MyParameter {
    public static String getParameter(String env, String key) {
        try {
            String propertiesFilePath = "flink/application-native/src/main/resources/application-" + env +".properties";
            ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);
            return parameter.get(key);
        } catch (IOException e) {
            System.err.println("Error reading properties file: " + e.getMessage());
            return null;
        }
    }
}
