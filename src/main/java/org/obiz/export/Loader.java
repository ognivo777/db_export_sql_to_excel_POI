package org.obiz.export;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.file.Files;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class Loader {
    private Connection connection;
    private String query;
    private int totalRows;

    public Loader(Connection connection, String query) {
        this.connection = connection;
        this.query = query;
    }

    public File doExport(int batch, Runnable onBatch) {
        try {
            File file = new File("exportResult.xlsx");
            if(file.exists()) {
                file.delete();
            }
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook();
            SXSSFWorkbook wb = new SXSSFWorkbook(xssfWorkbook);
            wb.setCompressTempFiles(true);

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setFetchSize(batch);
            Instant start = Instant.now();
            ResultSet resultSet = preparedStatement.executeQuery();
            System.out.println("DB execution time: " + start.until(Instant.now(), ChronoUnit.MILLIS)/1000f);
            System.out.println("Batch size = " + batch);
            RowProcessor processor = new RowProcessor(resultSet.getMetaData(), wb, batch);

            while(resultSet.next()) {
                processor.consumeRow(resultSet, onBatch);
            }
            totalRows = processor.getCurrrentRow();
            OutputStream stream = Files.newOutputStream(file.toPath());
            wb.write(stream);
            wb.close();
            stream.close();
        } catch (InterruptedException | SQLException | IOException e) {
            e.printStackTrace();
        }

        //TODO
        return new File("export.xlsx");
    }

    public int getTotalRows() {
        return totalRows;
    }

}
