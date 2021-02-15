package com.jamesmcguigan.nlp.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


public class Tweets {
    private static final Logger logger = LoggerFactory.getLogger(Tweets.class);

    public static List<Tweet> fromCSV(Path csvFile) {
        List<Tweet> tweets = new ArrayList<>();
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
                    Tweet tweet = new Tweet(id, keyword, location, text, target);
                    tweets.add(tweet);
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tweets;
    }

    public static void toSubmissionCSV(Path csvFile, List<Tweet> tweets, List<String> predictions) {
        if( tweets.size() != predictions.size() ) {
            throw new IllegalArgumentException(String.format(
                "tweets(%d) must be same size as predictions(%d)",
                tweets.size(), predictions.size()
            ));
        }

        try ( var printer = new CSVPrinter(new FileWriter(csvFile.toFile()), CSVFormat.EXCEL) ) {
            printer.printRecord("id", "target");
            for( int i = 0; i < tweets.size(); i++ ) {
                printer.printRecord(tweets.get(i).id, predictions.get(i));
            }
            Tweets.logger.info("wrote: {} = {} Kb", csvFile, Files.size(csvFile)/1024);
        } catch (IOException ex) {
            Tweets.logger.error("unable to write: {}", csvFile);
        }
    }

    public static void main( String[] args )
    {
        List<Tweet> tweets = Tweets.fromCSV(Paths.get("input/test.csv"));
        for ( Tweet tweet : tweets) {
            Tweets.logger.trace("{}", tweet);
        }
    }
}
