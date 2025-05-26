package com.dape.assembler.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
    public static final String BET = "bet.csv";
    public static final String CLIENT = "client.csv";
    public static final String TICKET = "ticket.csv";
    public static final String TICKET_BET = "ticket_bet.csv";
    public static final String FOLDER_SEPARATOR = "/";

    private final SparkSession session;

    public SparkUtils(SparkSession session) {
        this.session = session;
    }

    public Dataset<Row> readCSV(String path, String file) {
        path = path + FOLDER_SEPARATOR + file;

        return this.session.read()
                .option("header", true)
                .option("inferSchema", true)
                .option("delimiter", ",")
                .csv(path);
    }
}
