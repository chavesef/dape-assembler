package com.dape.assembler.util;

import java.io.Serializable;
import java.util.Properties;

public class PropertyUtils implements Serializable {

    private static final String CONFIG_APPLICATION_PROPERTIES_PATH = "/application";
    private static final String CONFIG_APPLICATION_PROPERTIES_EXTENSION = ".properties";
    private static PropertyUtils instance;
    protected Properties properties = new Properties();

    public static synchronized PropertyUtils getInstance() {
        if (instance == null) {
            instance = buildPropertyUtils();
        }
        return instance;
    }

    public static PropertyUtils buildPropertyUtils() {
        instance = new PropertyUtils();
        instance.initializeProfile();
        return instance;
    }

    public void initializeProfile() {
        defaultInit();
    }

    private void defaultInit() {
        try {
            final var resourceAsStream = PropertyUtils.class.getResourceAsStream(CONFIG_APPLICATION_PROPERTIES_PATH + CONFIG_APPLICATION_PROPERTIES_EXTENSION);
            if (resourceAsStream != null) {
                properties.load(resourceAsStream);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }
}
