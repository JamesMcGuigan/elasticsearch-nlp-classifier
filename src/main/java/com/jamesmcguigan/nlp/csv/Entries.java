package com.jamesmcguigan.nlp.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Entries {
    static List<Entry> fromCSV(String filename) {
        Path csvFile        = Paths.get(filename);
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
                    Boolean target  = row.isMapped("target")
                            ? Boolean.parseBoolean(row.get("target"))
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

    public static void main( String[] args )
    {
        List<Entry> entries = Entries.fromCSV("input/test.csv");
        for (Entry entry : entries) {
            System.out.println( entry );
        }
    }
}
