package com.dape.assembler;

import com.dape.assembler.config.ArgsConfigs;
import com.dape.assembler.config.S3Config;
import com.dape.assembler.config.SparkConfig;
import com.dape.assembler.exceptions.ProcessingFlowException;
import com.dape.assembler.service.DapeAssemblerService;
import com.dape.assembler.service.DapeFileAssembler;
import com.dape.assembler.service.DirectoryManagementService;
import com.dape.assembler.util.PropertyUtils;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DapeAssemblerApplication {
    public static final Logger LOGGER = LoggerFactory.getLogger(DapeAssemblerApplication.class);

    public static void main(String[] args) throws IOException {
        new DapeAssemblerApplication().run(args);
    }

    public void run(String[] args) throws IOException {
        LOGGER.info("m=run, msg=Starting execution with argsLength={}, args=[{}]", args.length, String.join(", ", args));

        final ArgsConfigs argsConfigs = ArgsConfigs.buildArgsConfigs(args);
        final PropertyUtils propertyUtils = PropertyUtils.getInstance();
        final S3Config s3Config = new S3Config(propertyUtils);
        final String inputFilePath = mountInputFilePath(propertyUtils);

        LOGGER.info("m=run msg=Starting Dape Assembler");

        try (final SparkSession session = new SparkConfig(propertyUtils).createSession()) {
            final DapeFileAssembler dapeFileAssembler = new DapeFileAssembler(session);
            final DirectoryManagementService directoryManagementService = new DirectoryManagementService(s3Config);
            final DapeAssemblerService assemblerService = new DapeAssemblerService(argsConfigs.getParameters().getInvestigationClientIdt(), inputFilePath, dapeFileAssembler, directoryManagementService);

            final String outputFilePath = mountOutputFilePath(propertyUtils);
            final Dataset<String> mountedDataset = assemblerService.buildDapeFile(session);

            directoryManagementService.writeOutputFile(mountedDataset, outputFilePath);
            directoryManagementService.renameS3OutputFile();
        } catch (Exception e) {
            throw new ProcessingFlowException(e.getMessage());
        } finally {
            LOGGER.info("m=run msg=Finished Dape Assembler");
        }
    }

    private String mountInputFilePath(PropertyUtils propertyUtils) {
        final String bucketName = propertyUtils.getProperty("dape.s3.bucket.name");
        final String inputFolder = propertyUtils.getProperty("dape.s3.input.folder.name");

        return String.format("s3a://%s/%s", bucketName, inputFolder);
    }

    private String mountOutputFilePath(PropertyUtils propertyUtils) {
        final String bucketName = propertyUtils.getProperty("dape.s3.bucket.name");
        final String outputFolder = propertyUtils.getProperty("dape.s3.output.folder.name");

        return String.format("s3a://%s/%s", bucketName, outputFolder);
    }
}
