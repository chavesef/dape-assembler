package com.dape.assembler.config.wrapper;

import com.dape.assembler.util.PropertyUtils;
import org.apache.spark.SparkConf;

public class SparkConfWrapper {

    private final SparkConf sparkConf;

    private final PropertyUtils propertyUtils;

    public SparkConfWrapper (final PropertyUtils propertyUtils,
                             String appNameConfigKey) {
        this.propertyUtils = propertyUtils;
        this.sparkConf = new SparkConf().setAppName(propertyUtils.getProperty(appNameConfigKey));
    }

    public SparkConfWrapper set(String configKey, boolean nullable) {
        String propertyValue = propertyUtils.getProperty(configKey);
        return this.set(configKey, propertyValue, nullable);
    }

    public SparkConfWrapper set(String key, String value, boolean nullable) {

        if (nullable && (value == null || value.isEmpty())) {
            return this;
        }
        this.sparkConf.set(key, value);
        return this;
    }

    public SparkConf getSparkConf () {
        return sparkConf;
    }
}

