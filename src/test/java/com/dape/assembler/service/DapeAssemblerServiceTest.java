package com.dape.assembler.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DapeAssemblerServiceTest {

    private static final String INVESTIGATION_CLIENT_IDT = "1";
    private static final String INVESTIGATION_CLIENT_NULL = null;
    private static final String FILE_PATH = "mockFilePath";
    private final DapeFileAssembler dapeFileAssembler = mock(DapeFileAssembler.class);
    private final DirectoryManagementService directoryManagementService = mock(DirectoryManagementService.class);
    private final SparkSession sparkSession = mock(SparkSession.class);

    @Test
    void shouldBuildDapeFileWithInvestigationClientIdt() {
        final Dataset<String> expectedDataset = generateMockDataset();

        doNothing().when(directoryManagementService).getInputFiles(sparkSession, FILE_PATH);
        when(dapeFileAssembler.mountInvestigationReport(INVESTIGATION_CLIENT_IDT)).thenReturn(expectedDataset);

        DapeAssemblerService dapeAssemblerService = new DapeAssemblerService(INVESTIGATION_CLIENT_IDT, FILE_PATH, dapeFileAssembler, directoryManagementService);
        Dataset<String> actualDataset = dapeAssemblerService.buildDapeFile(sparkSession);

        assertThat(actualDataset.count()).isEqualTo(expectedDataset.count());
        verify(directoryManagementService).getInputFiles(sparkSession, FILE_PATH);
        verify(dapeFileAssembler).mountInvestigationReport(INVESTIGATION_CLIENT_IDT);
    }

    @Test
    void shouldBuildDapeFileWithoutInvestigationClientIdt() {
        final Dataset<String> expectedDataset = generateMockDataset();

        doNothing().when(directoryManagementService).getInputFiles(sparkSession, FILE_PATH);
        when(dapeFileAssembler.mountFullReport()).thenReturn(expectedDataset);

        DapeAssemblerService dapeAssemblerService = new DapeAssemblerService(INVESTIGATION_CLIENT_NULL, FILE_PATH, dapeFileAssembler, directoryManagementService);
        Dataset<String> actualDataset = dapeAssemblerService.buildDapeFile(sparkSession);

        assertThat(actualDataset.count()).isEqualTo(expectedDataset.count());
        verify(directoryManagementService).getInputFiles(sparkSession, FILE_PATH);
        verify(dapeFileAssembler).mountFullReport();
    }

    private Dataset<String> generateMockDataset() {
        Dataset<String> mockDataset = mock(Dataset.class);
        when(mockDataset.count()).thenReturn(2L);
        return mockDataset;
    }
}
