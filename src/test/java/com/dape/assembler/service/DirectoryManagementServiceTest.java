package com.dape.assembler.service;

import com.amazonaws.services.s3.AmazonS3;
import com.dape.assembler.config.S3Config;
import com.dape.assembler.util.SparkUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DirectoryManagementServiceTest {
    private static final String VIEW_BET = "bet";
    private static final String VIEW_CLIENT = "client";
    private static final String VIEW_TICKET = "ticket";
    private static final String VIEW_TICKET_BET = "ticket_bet";
    private static final String FILE_PATH = "mockFilePath";

    private final S3Config s3ConfigMock = mock(S3Config.class);
    private final AmazonS3 s3Mock = mock(AmazonS3.class);
    private final SparkSession sparkSessionMock = mock(SparkSession.class);
    private final SparkUtils sparkUtilsMock = mock(SparkUtils.class);


    @Test
    void testGetInputFiles() {
        when(s3ConfigMock.getS3()).thenReturn(s3Mock);
        final DirectoryManagementService serviceSpy = spy(new DirectoryManagementService(s3ConfigMock));
        final Dataset<Row> expectedDataset = generateMockDataset();

        doReturn(sparkUtilsMock).when(serviceSpy).getSparkUtils(sparkSessionMock);
        when(sparkUtilsMock.readCSV(anyString(), anyString())).thenReturn(expectedDataset);

        doNothing().when(expectedDataset).createOrReplaceTempView(anyString());

        serviceSpy.getInputFiles(sparkSessionMock, FILE_PATH);

        verify(expectedDataset).createOrReplaceTempView(VIEW_BET);
        verify(expectedDataset).createOrReplaceTempView(VIEW_CLIENT);
        verify(expectedDataset).createOrReplaceTempView(VIEW_TICKET);
        verify(expectedDataset).createOrReplaceTempView(VIEW_TICKET_BET);
    }

    @Test
    void testWriteOutputFile() {
        DirectoryManagementService service = new DirectoryManagementService(s3ConfigMock);
        Dataset<String> mountedDatasetMock = mock(Dataset.class);
        DataFrameWriter<String> dataFrameWriter = mock(DataFrameWriter.class);

        when(mountedDatasetMock.coalesce(1)).thenReturn(mountedDatasetMock);
        when(mountedDatasetMock.write()).thenReturn(dataFrameWriter);
        when(dataFrameWriter.mode("overwrite")).thenReturn(dataFrameWriter);

        service.writeOutputFile(mountedDatasetMock, FILE_PATH);

        verify(mountedDatasetMock).coalesce(1);
        verify(mountedDatasetMock).write();
        verify(dataFrameWriter).mode("overwrite");
        verify(dataFrameWriter).text(FILE_PATH + "/tmp_output");
    }

    private Dataset<Row> generateMockDataset() {
        Dataset<Row> mockDataset = mock(Dataset.class);
        when(mockDataset.count()).thenReturn(2L);
        return mockDataset;
    }
}
