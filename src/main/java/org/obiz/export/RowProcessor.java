package org.obiz.export;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

public class RowProcessor {

    private final int columnCount;
    private final SXSSFSheet ws;
    private final CellStyle cellStyleDateDt;
    private SXSSFWorkbook wb;
    private final ArrayList<String> columnNames;
    private final ArrayList<Integer> columnTypes;
    int currrentRow = 0;
    private int batch;
    private boolean autoSized;

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

        cellStyleDateDt = wb.createCellStyle();
        final CreationHelper creationHelperDt = wb.getCreationHelper();
        cellStyleDateDt.setDataFormat(creationHelperDt.createDataFormat().getFormat("dd.mm.yyyy HH:mm"));

        ws.trackAllColumnsForAutoSizing();
    }

    public void consumeRow(ResultSet resultSet, Runnable onBatch) throws SQLException, IOException, InterruptedException {
        Row row = ws.createRow(currrentRow++);
        for (int i = 0; i < columnCount; i++) {
            Cell cell = row.createCell(i);
            fillCell(i, cell, resultSet);
        }
        if(currrentRow%batch==0) {
            if(currrentRow==batch) {
                autoSized = true;
                autoSizeWidths();
            }
            System.out.print("|");
            onBatch.run();
            if(currrentRow%(batch * 100)==0) {
                System.out.println(" " + currrentRow);
                //Thread.sleep(2000);
            }
            ws.flushRows();
        }
    }

    public void autoSizeWidths() {
        if(autoSized) {
            return; //already done
        }
        //here is end of first batch
        for (int i = 0; i < columnCount; i++) {
            ws.autoSizeColumn(i+1);
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
                Date sqlDate = resultSet.getDate(dbColumnIndex);
                if(sqlDate!=null) {
                    cell.setCellValue(sqlDate.toLocalDate());
                } else {
                    cell.setBlank();
                }
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                Time sqlTime = resultSet.getTime(dbColumnIndex);
                if(sqlTime!=null) {
                    cell.setCellValue(sqlTime.toLocalTime().toString());
                } else {
                    cell.setBlank();
                }
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                cell.setCellStyle(cellStyleDateDt);
                Timestamp sqlTimestamp = resultSet.getTimestamp(dbColumnIndex);
                if(sqlTimestamp!=null) {
                    cell.setCellValue(sqlTimestamp.toLocalDateTime());
                } else {
                    cell.setBlank();
                }
                break;
            default:
                cell.setCellValue(resultSet.getString(dbColumnIndex));
        }
    }

    public int getCurrrentRow() {
        return currrentRow;
    }

}
