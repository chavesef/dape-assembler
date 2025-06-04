package com.dape.assembler.config;

import com.dape.assembler.config.wrapper.SparkConfWrapper;
import com.dape.assembler.util.PropertyUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {

    private final PropertyUtils propertyUtils;

    public SparkConfig(PropertyUtils propertyUtils) {
        this.propertyUtils = propertyUtils;
    }

    public SparkSession createSession() {
        final SparkConf sparkConf = createSparkConf();

        return SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
    }


    private SparkConf createSparkConf() {
        final SparkConfWrapper sparkConfWrapper = new SparkConfWrapper(propertyUtils, "spark.appName");
        sparkConfWrapper.set("spark.master", true)
                .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", true)
                .set("spark.dynamicAllocation.maxExecutors", "15", true)
                .set("fs.s3a.change.detection.version.required", "false", false)
                .set("spark.driver.maxResultSize", true)
                .set("spark.eventLog.enabled", true)
                .set("fs.s3a.access.key", true)
                .set("fs.s3a.secret.key", true)
                .set("fs.s3a.endpoint", true)
                .set("fs.s3a.path.style.access", true)
                .set("spark.sql.shuffle.partitions", "8", false)
                .set("spark.ui.enabled", "false", false)
                .set("spark.default.parallelism", "8", false);

        return sparkConfWrapper.getSparkConf();
    }
}
