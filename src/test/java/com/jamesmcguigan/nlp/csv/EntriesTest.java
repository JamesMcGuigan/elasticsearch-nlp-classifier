package com.jamesmcguigan.nlp.csv;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class EntriesTest {
    @ParameterizedTest
    @ValueSource(strings = { "input/train.csv", "input/test.csv" })
    void canReadCSV(String filename) throws IOException {
        Path path = Paths.get(filename);
        Assertions.assertTrue( Files.exists(path) );

        // .matcher() is required to extract out content
        List<Integer> ids = Pattern.compile("\\n(\\d+),")
                .matcher(Files.readString(path))
                .results()
                .map(match -> match.group(1))
                .map(Integer::parseInt)
                .collect(Collectors.toList())
        ;
        // .split() includes the header row
        String[] lines = Files.readString(path).split("(?=\n\\d+,)");

        // Read as CSV
        List<Entry> entries = Entries.fromCSV(path);

        Assertions.assertEquals(ids.size(), entries.size());
        Assertions.assertEquals(lines.length - 1, entries.size());
    }
}
