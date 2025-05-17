package com.dape.assembler;

import com.dape.assembler.config.S3Config;
import com.dape.assembler.config.SparkConfig;
import com.dape.assembler.util.PropertyUtils;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DapeAssemblerApplication {

	public static void main(String[] args) {
		new DapeAssemblerApplication().run(args);
	}

	public void run(String[] args) {
		final PropertyUtils propertyUtils = PropertyUtils.getInstance();
		final S3Config s3Config = new S3Config(propertyUtils);
		final SparkSession session = new SparkConfig(propertyUtils).createSession();
	}
}
