package com.dape.assembler.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.dape.assembler.config.S3Config;
import com.dape.assembler.exceptions.S3OperationException;
import com.dape.assembler.util.PropertyUtils;
import com.dape.assembler.util.SparkUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryManagementService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryManagementService.class);

    private final AmazonS3 s3;
    private static final PropertyUtils propertyUtils = PropertyUtils.getInstance();
    private static final String BUCKET_NAME = propertyUtils.getProperty("dape.s3.bucket.name");
    private static final String OUTPUT_FOLDER = propertyUtils.getProperty("dape.s3.output.folder.name");
    private static final String FILE_NAME = "dape_generated";
    private static final String FILE_EXTENSION = ".txt";
    private static final String GENERATED_FILE_PREFIX = "output/tmp_output/part-";
    private static final String FOLDER_SEPARATOR = "/";

    public DirectoryManagementService(S3Config s3) {
        this.s3 = s3.getS3();
    }

    public void getInputFiles(SparkSession sparkSession, String filePath) {
        LOGGER.info("m=getInputFiles, msg=Reading input files from path={}", filePath);

        SparkUtils sparkUtils = getSparkUtils(sparkSession);
        sparkUtils.readCSV(filePath, SparkUtils.BET).createOrReplaceTempView("bet");
        sparkUtils.readCSV(filePath, SparkUtils.CLIENT).createOrReplaceTempView("client");
        sparkUtils.readCSV(filePath, SparkUtils.TICKET).createOrReplaceTempView("ticket");
        sparkUtils.readCSV(filePath, SparkUtils.TICKET_BET).createOrReplaceTempView("ticket_bet");
    }

    public void writeOutputFile(Dataset<String> mountedDataset, String outputFilePath) {
        LOGGER.info("m=writeOutputFile, msg=Writing output file to path={}", outputFilePath);

        String tempOutputPath = outputFilePath + "/tmp_output";

        mountedDataset.coalesce(1)
                .write()
                .mode("overwrite")
                .text(tempOutputPath);

    }

    public void renameS3OutputFile() {
        try {
            LOGGER.info("m=compressS3OutputFile, msg=Renaming output file");

            final List<S3ObjectSummary> outputFiles = s3.listObjectsV2(BUCKET_NAME, OUTPUT_FOLDER).getObjectSummaries();
            final String originKey = getTxtFileInObjectList(outputFiles);

            final File downloadedFile = downloadFile(originKey);
            final String outputFolder = OUTPUT_FOLDER.concat(FOLDER_SEPARATOR).concat(FILE_NAME.concat(FILE_EXTENSION));

            outputFiles.forEach(s3ObjectSummary -> s3.deleteObject(BUCKET_NAME, s3ObjectSummary.getKey()));
            s3.putObject(BUCKET_NAME, outputFolder, downloadedFile);

            Files.deleteIfExists(Paths.get(downloadedFile.getPath()));

        } catch (IOException e) {
            throw new S3OperationException(e.getMessage());
        }
    }

    protected SparkUtils getSparkUtils(SparkSession sparkSession) {
        return new SparkUtils(sparkSession);
    }

    protected File downloadFile(String originKey) throws IOException {
        final File file = new File(FILE_NAME.concat(FILE_EXTENSION));
        final S3Object s3object = s3.getObject(new GetObjectRequest(BUCKET_NAME, originKey));

        FileUtils.copyInputStreamToFile(s3object.getObjectContent(), file);

        return file;
    }

    protected String getTxtFileInObjectList(List<S3ObjectSummary> outputFiles) {
        LOGGER.info("m=getTxtFileInObjectList, msg=Searching for generated file in output files");

        return outputFiles.stream().
                filter(s3ObjectSummary -> s3ObjectSummary.getKey().startsWith(GENERATED_FILE_PREFIX))
                .findFirst()
                .orElseThrow()
                .getKey();
    }
}
