package org.obiz.export;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

public class RowProcessor {

    private final int columnCount;
    private final SXSSFSheet ws;
    private SXSSFWorkbook wb;
    private final ArrayList<String> columnNames;
    private final ArrayList<Integer> columnTypes;
    int currrentRow = 0;
    private int batch;

    public RowProcessor(ResultSetMetaData metaData, SXSSFWorkbook wb, int batch) throws SQLException {
        columnCount = metaData.getColumnCount();
        this.wb = wb;
        this.batch = batch;
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();
        ws = wb.createSheet("Export data");
        Row row = ws.createRow(currrentRow++);
        for (int i = 0; i < columnCount; i++) {
            Cell cell = row.createCell(i);
            cell.setCellValue(metaData.getColumnName(i+1));
            columnNames.add(metaData.getColumnName(i+1));
            columnTypes.add(metaData.getColumnType(i+1));
        }
        currrentRow++;

    }

    public void consumeRow(ResultSet resultSet, Runnable onBatch) throws SQLException, IOException, InterruptedException {
        Row row = ws.createRow(currrentRow++);
        for (int i = 0; i < columnCount; i++) {
            Cell cell = row.createCell(i);
            fillCell(i, cell, resultSet);
        }
        if(currrentRow%batch==0) {
            System.out.print("|");
            onBatch.run();
            if(currrentRow%(batch * 100)==0) {
                System.out.println(" " + currrentRow);
                //Thread.sleep(2000);
            }
            ws.flushRows();
        }
    }

    private void fillCell(int i, Cell cell, ResultSet resultSet) throws SQLException {
        final int dbColumnIndex = i + 1;
        switch (columnTypes.get(i)) {
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.INTEGER:
            case Types.TINYINT:
                cell.setCellValue(resultSet.getLong(dbColumnIndex));
                break;
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                cell.setCellValue(resultSet.getDouble(dbColumnIndex));
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                cell.setCellValue(resultSet.getDate(dbColumnIndex));
                break;
            default:
                cell.setCellValue(resultSet.getString(dbColumnIndex));
        }
    }

    public int getCurrrentRow() {
        return currrentRow;
    }
}
