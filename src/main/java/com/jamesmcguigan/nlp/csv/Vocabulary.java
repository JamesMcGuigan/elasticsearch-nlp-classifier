package com.jamesmcguigan.nlp.csv;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

class Vocabulary extends TreeSet<String> {
    Vocabulary(List<String> words) {
        this.addAll(words);
    }


    public static Vocabulary fromFile(String filename)       { return Vocabulary.fromFiles(Collections.singletonList(filename)); }
    public static Vocabulary fromFiles(String ...filenames)  { return Vocabulary.fromFiles(Arrays.asList(filenames)); }
    public static Vocabulary fromFiles(List<String> filenames) {
        var entries = filenames.parallelStream()
                .map(Entries::fromCSV)
                .flatMap(List::stream)
                .collect(Collectors.toList())
        ;
        return Vocabulary.fromEntries(entries);
    }


    public static Vocabulary fromEntry(Entry entry)        { return Vocabulary.fromEntries(Collections.singletonList(entry)); }
    public static Vocabulary fromEntries(Entry ...entries) { return Vocabulary.fromEntries(Arrays.asList(entries)); }
    public static Vocabulary fromEntries( List<Entry> entries ) {
        var words = entries.stream()
                .map(Entry::tokenize)
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList())
        ;
        return new Vocabulary(words);
    }
}
