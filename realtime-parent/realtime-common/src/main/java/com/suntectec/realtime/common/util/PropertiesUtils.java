package com.suntectec.realtime.common.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

public class PropertiesUtils {
    private final static ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("application");

    private final static ResourceBundle ENV_RESOURCE_BUNDLE = ResourceBundle.getBundle("application-" + RESOURCE_BUNDLE.getString("profiles.active"));

    public static String getProperty(String key) {
        return ENV_RESOURCE_BUNDLE.getString(key);
    }

    public static ParameterTool getPropertiesParameters () throws IOException {
        return ParameterTool.fromPropertiesFile("realtime-parent/realtime-common/src/main/resources/application-" + RESOURCE_BUNDLE.getString("profiles.active") + ".properties");
    }
}