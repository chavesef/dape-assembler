package com.dape.assembler.config;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.dape.assembler.util.PropertyUtils;

public class S3Config {

    private final PropertyUtils propertyUtils;
    private final AmazonS3 s3;

    public S3Config(PropertyUtils propertyUtils) {
        this.propertyUtils = propertyUtils;
        this.s3 = amazonS3Client();
    }

    public AmazonS3 getS3() {
        return s3;
    }

    private AmazonS3 amazonS3Client() {

        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(propertyUtils.getProperty("cloud.aws.endpoint.static"), propertyUtils.getProperty("cloud.aws.region.static")))
                .withPathStyleAccessEnabled(true)
                .build();
    }
}
