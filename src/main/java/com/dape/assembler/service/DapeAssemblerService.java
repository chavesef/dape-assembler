package com.dape.assembler.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class DapeAssemblerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DapeAssemblerService.class);

    private final String investigationClientIdt;
    private final String filePath;
    private final DapeFileAssembler dapeFileAssembler;
    private final DirectoryManagementService directoryManagementService;

    public DapeAssemblerService(String investigationClientIdt, String filePath, DapeFileAssembler dapeFileAssembler, DirectoryManagementService directoryManagementService) {
        this.investigationClientIdt = investigationClientIdt;
        this.filePath = filePath;
        this.dapeFileAssembler = dapeFileAssembler;
        this.directoryManagementService = directoryManagementService;
    }

    public Dataset<String> buildDapeFile(SparkSession sparkSession) {
        LOGGER.info("m=buildDapeFile, msg=Reading input files from path={}", filePath);
        directoryManagementService.getInputFiles(sparkSession, filePath);

        Dataset<String> finalDataset;

        if (investigationClientIdt == null)
            finalDataset = dapeFileAssembler.mountFullReport();
        else
            finalDataset = dapeFileAssembler.mountInvestigationReport(investigationClientIdt);

        LOGGER.info("m=buildDapeFile, msg=final dataset size={}", finalDataset.count());
        return finalDataset;
    }
}
