package org.obiz.export;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.Collectors;

public class Main {
    private static final int MegaBytes = 10241024;
    private static Runtime runtime;

    public static void main(String[] args) {
        Config config = new Config().load(args);
        Db db = new Db(config);
        runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory()/MegaBytes;
        try (Connection conn = db.createConnection()) {
            String queryFile = config.getQueryFile();
            String sql = Files.lines(Paths.get(queryFile)).collect(Collectors.joining("\n"));
            System.out.println("sql:\n" + sql);
            Loader loader = new Loader(conn, sql);
            Instant start = Instant.now();
            File exportResult = loader.doExport(100, () -> {
                long freeMemory = runtime.freeMemory()/MegaBytes;
                if(freeMemory < maxMemory*0.05) {
                    //System.gc();
                    System.out.print("*");
                }
            });
            System.out.println("\nExported rows = " + (loader.getTotalRows()-1));
            System.out.println("Takes seconds: " + start.until(Instant.now(), ChronoUnit.MILLIS)/1000f);
        } catch (ClassNotFoundException | SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
