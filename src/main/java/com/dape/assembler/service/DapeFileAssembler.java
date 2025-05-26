package com.dape.assembler.service;

import com.dape.assembler.enums.TicketTypeEnum;
import com.dape.assembler.exceptions.ProcessingFlowException;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;

public class DapeFileAssembler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DapeFileAssembler.class);

    private static final String IDT_CLIENT = "idt_client";
    private static final String NAM_CLIENT = "nam_client";
    private static final String NUM_DOCUMENT = "num_document";
    private static final String NUM_AMOUNT = "num_amount";
    private static final String NUM_FINAL_ODD = "num_final_odd";
    private static final String COD_TICKET_STATUS = "cod_ticket_status";
    private static final String COD_TICKET_TYPE = "cod_ticket_type";
    private static final String DELIMITER = "|";
    private static final Integer PENDING_STATUS = 1;
    private static final Integer GREEN_STATUS = 2;
    private static final Integer RED_STATUS = 3;

    private final SparkSession sparkSession;

    public DapeFileAssembler(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<String> mountFullReport() {
        LOGGER.info("m=mountFullReport, msg=Starting to mount full report");

        Dataset<String> fileDataset = sparkSession.emptyDataset(Encoders.STRING());
        List<Row> selectIdtClientFromClient = getDatasetForAllClientsWithTransactions();

        for (Row row : selectIdtClientFromClient) {
            String investigationClientIdt = row.get(0).toString();
            fileDataset = fileDataset.union(mountInvestigationReport(investigationClientIdt));
        }

        return fileDataset;
    }

    public Dataset<String> mountInvestigationReport(String investigationClientIdt) {
        LOGGER.info("m=mountInvestigationReport, msg=Starting to mount report for clientIdt={}", investigationClientIdt);

        Dataset<String> fileDataset;
        Long investigationClientIdtLong = Long.valueOf(investigationClientIdt);

        Dataset<Row> datasetInvestigationClient = getDatasetForClient(investigationClientIdtLong);

        if (datasetInvestigationClient.isEmpty()) {
            String errorMessage = String.format("No transactions found for clientIdt=%s", investigationClientIdt);
            LOGGER.error("m=mountInvestigationReport, msg={}", errorMessage);
            throw new ProcessingFlowException(errorMessage);
        }

        fileDataset = mountClientInfo(datasetInvestigationClient);
        fileDataset = fileDataset.union(mountTicketReportByType(datasetInvestigationClient, TicketTypeEnum.SIMPLE.getCodTicketType()));
        fileDataset = fileDataset.union(mountTicketReportByType(datasetInvestigationClient, TicketTypeEnum.MULTIPLE.getCodTicketType()));
        fileDataset = fileDataset.union(mountSummarization(datasetInvestigationClient));

        return fileDataset;
    }

    private Dataset<String> mountClientInfo(Dataset<Row> rawDataset) {
        Row clientInfo = rawDataset.select(IDT_CLIENT, NAM_CLIENT, NUM_DOCUMENT).first();
        Object totalTickets = rawDataset.count();

        String line = DELIMITER +
                      String.join(DELIMITER, clientInfo.get(0).toString(), clientInfo.getString(1), clientInfo.getString(2), totalTickets.toString()) +
                      DELIMITER;
        return createTextDataset(line);
    }

    private Dataset<String> mountTicketReportByType(Dataset<Row> rawDataset, int ticketType) {
        Dataset<Row> datasetByTicketType = rawDataset.select("*").where(col(COD_TICKET_TYPE).equalTo(ticketType));

        long totalTickets = datasetByTicketType.count();
        long pendingTickets = datasetByTicketType.where(col(COD_TICKET_STATUS).equalTo(PENDING_STATUS)).count();
        long greenTickets = datasetByTicketType.where(col(COD_TICKET_STATUS).equalTo(GREEN_STATUS)).count();
        long redTickets = datasetByTicketType.where(col(COD_TICKET_STATUS).equalTo(RED_STATUS)).count();

        List<String> valuesSummarized = summarizeValues(datasetByTicketType);

        String line = DELIMITER + String.join(DELIMITER,
                String.valueOf(ticketType),
                Long.toString(totalTickets),
                String.valueOf(pendingTickets),
                String.valueOf(greenTickets),
                String.valueOf(redTickets),
                valuesSummarized.get(0),
                valuesSummarized.get(1),
                valuesSummarized.get(2)
        ) + DELIMITER;

        return createTextDataset(line);
    }

    private Dataset<String> mountSummarization(Dataset<Row> rawDataset) {
        List<String> valuesSummarized = summarizeValues(rawDataset);

        String line = DELIMITER + String.join(DELIMITER,
                valuesSummarized.get(0),
                valuesSummarized.get(1),
                valuesSummarized.get(2)
        ) + DELIMITER;

        return createTextDataset(line);
    }

    private List<String> summarizeValues(Dataset<Row> rawDataset) {
        Object totalNumAmount = rawDataset.agg(sum(NUM_AMOUNT)).first().get(0) == null ? 0 : rawDataset.agg(sum(NUM_AMOUNT)).first().get(0);
        Object avgNumFinalOdd = rawDataset.agg(round(avg(NUM_FINAL_ODD), 2)).first().get(0) == null ? 0 : rawDataset.agg(round(avg(NUM_FINAL_ODD), 2)).first().get(0);

        Row revenueRow = rawDataset.where(col(COD_TICKET_STATUS).equalTo(GREEN_STATUS))
                .agg(round(sum(col(NUM_AMOUNT).multiply(col(NUM_FINAL_ODD))), 2))
                .first();
        Object totalRevenue = revenueRow.isNullAt(0) ? 0 : revenueRow.get(0);

        return List.of(totalNumAmount.toString(), avgNumFinalOdd.toString(), totalRevenue.toString());
    }

    private Dataset<String> createTextDataset(String text) {
        LOGGER.info("m=createTextDataset, msg=dataset Creating text dataset");
        return sparkSession.createDataset(Collections.singletonList(text), Encoders.STRING());
    }

    private List<Row> getDatasetForAllClientsWithTransactions() {
        Dataset<Row> datasetInvestigationClient = sparkSession.sql("""
                SELECT DISTINCT c.idt_client
                    FROM client c
                    JOIN ticket t ON c.idt_client = t.idt_client
                    ORDER BY c.idt_client
                """);
        return datasetInvestigationClient.collectAsList();
    }

    private Dataset<Row> getDatasetForClient(Long investigationClientIdtLong) {
        return sparkSession.sql(String.format("""
                SELECT c.idt_client, c.nam_client, c.num_document,
                       t.idt_ticket, t.num_amount, t.cod_ticket_type,
                       t.cod_ticket_status, t.num_final_odd
                  FROM client c
                  JOIN ticket t ON c.idt_client = t.idt_client
                 WHERE c.idt_client = %d
                """, investigationClientIdtLong));
    }
}
