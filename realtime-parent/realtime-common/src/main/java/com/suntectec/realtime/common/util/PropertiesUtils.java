package com.suntectec.realtime.common.util;

import java.util.ResourceBundle;

public class PropertiesUtils {
    private final static ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("application");

    private final static ResourceBundle ENV_RESOURCE_BUNDLE = ResourceBundle.getBundle("application-" + RESOURCE_BUNDLE.getString("profiles.active"));

    public static String getProperty(String key) {
        return ENV_RESOURCE_BUNDLE.getString(key);
    }
}