package com.jamesmcguigan.nlp.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Entries {
    public static List<Entry> fromCSV(Path csvFile) {
        List<Entry> entries = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(csvFile, UTF_8)) {
            CSVFormat csv = CSVFormat.RFC4180.withHeader();
            try( CSVParser parser = csv.parse(reader) ) {
                Iterator<CSVRecord> it = parser.iterator();
                it.forEachRemaining(row -> {
                    int id          = Integer.parseInt(row.get("id"));
                    String keyword  = row.get("keyword");
                    String location = row.get("location");
                    String text     = row.get("text");
                    String target   = row.isMapped("target")
                            ? row.get("target")
                            : null;
                    Entry entry     = new Entry(id, keyword, location, text, target);
                    entries.add(entry);
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return entries;
    }

    public static void toSubmissionCSV(Path csvFile, List<Entry> entries, List<String> predictions) {
        if( entries.size() != predictions.size() ) {
            throw new IllegalArgumentException(String.format(
                "entries(%d) must be same size as predictions(%d)",
                entries.size(), predictions.size()
            ));
        }

        try ( var printer = new CSVPrinter(new FileWriter(csvFile.toFile()), CSVFormat.EXCEL) ) {
            printer.printRecord("id", "target");
            for( int i = 0; i < entries.size(); i++ ) {
                printer.printRecord(entries.get(i).id, predictions.get(i));
            }

            System.out.printf("wrote: %s = %d Kb%n", csvFile.toString(), Files.size(csvFile)/1024);
        } catch (IOException ex) {
            System.out.printf("ERROR writing: %s%n", csvFile.toString());
        }
    }

    public static void main( String[] args )
    {
        List<Entry> entries = Entries.fromCSV(Paths.get("input/test.csv"));
        for (Entry entry : entries) {
            System.out.println( entry );
        }
    }
}
