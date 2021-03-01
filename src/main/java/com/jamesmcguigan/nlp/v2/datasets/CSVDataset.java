package com.jamesmcguigan.nlp.v2.datasets;

import com.google.common.collect.Streams;
import com.jamesmcguigan.nlp.v2.config.DatasetConfig;
import com.jamesmcguigan.nlp.v2.exceptions.InvalidConfigurationException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;


// TODO: write unit tests
public class CSVDataset extends AbstractDataset {
    public CSVDataset(DatasetConfig config, String condition) {
        super(config, condition);
        if( !condition.isEmpty() ) { throw new InvalidConfigurationException("CSV conditions not implemented"); }
    }


    public Stream<DataRow> getStream(Path path) {
        try( BufferedReader reader = Files.newBufferedReader(path, UTF_8) ) {
            return getStream(reader);
        } catch( IOException e ) {
            throw new InvalidConfigurationException("Failed to parse CSV: "+path.toString(), e);
        }
    }
    public Stream<DataRow> getStream(BufferedReader reader) throws IOException {
        CSVFormat csv = CSVFormat.RFC4180.withHeader();
        try( CSVParser parser = csv.parse(reader) ) {
            Iterator<CSVRecord> iterator = parser.iterator();
            return Stream.iterate(
                iterator.next(),
                i -> iterator.hasNext(),
                i -> iterator.next()
            ).map(record -> new DataRow(new HashMap<>(record.toMap()), this.config));
        }
    }


    @Override
    public Stream<DataRow> getTrainStream() {
        return getStream(config.getFiles().train);
    }

    @Override
    public Stream<DataRow> getTestStream() {
        return getStream(config.getFiles().test);
    }


    @Override
    public List<Stream<DataRow>> getKFoldStreams(int folds) {
        return kFoldStream(getTrainStream(), folds);
    }

    /**
     * Partition stream into K folds based on index without duplicates between streams
     * See {@code AbstractDataset::getKFoldTestTrainStreams()}
     *     to get K duplicates of the dataset partitioned into test/train stream pairs
     */
    public static List<Stream<DataRow>> kFoldStream(Stream<DataRow> sourceStream, int folds) {
        if(!( folds >= 1 )) { throw new IllegalArgumentException("PRECONDITION: folds="+folds+" >= 1"); }

        // QUESTION: Is this thread-safe? Does this load the whole file into memory?
        List<Stream<DataRow>> streams = new ArrayList<>();
        for( int n = 0; n < folds; n++ ) { streams.add( Stream.empty() ); }
        Streams.mapWithIndex(
            sourceStream,
            AbstractMap.SimpleImmutableEntry::new
        ).forEachOrdered((AbstractMap.SimpleImmutableEntry<DataRow, Long> entry) -> {
            // For each item, modulo the index and bucket into: streams[index % folds]
            int index     = Long.valueOf(entry.getValue() % folds).intValue();
            DataRow value = entry.getKey();
            synchronized(streams) {
                streams.set(index, Stream.concat(streams.get(index), Stream.of(value)));
            }
        });

        // NOTE: this is thread-safe, but requires reading/parsing the entire file K times
        //for( int n = 0; n < folds; n++ ) {
        //    final int fold = n;
        //    var stream = getTrainStream();
        //    stream = Streams.mapWithIndex(stream, AbstractMap.SimpleImmutableEntry::new)
        //        .filter(entry -> entry.getValue() % folds == fold)
        //        .map(Map.Entry::getKey)
        //    ;
        //    streams.add(stream);
        //}
        return Collections.unmodifiableList(streams);
    }
}
