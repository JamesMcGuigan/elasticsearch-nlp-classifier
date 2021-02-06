package com.jamesmcguigan.nlp.csv;

import com.jamesmcguigan.nlp.tokenize.EntryTokenizer;
import opennlp.tools.doccat.DocumentSample;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Entry {

    public final int id;
    public final String keyword;
    public final String location;
    public final String text;
    public final String target;

    private final EntryTokenizer tokenizer = new EntryTokenizer()
        // .setCleanTwitter(true)  // Kaggle score = 0.76248
        // .setTwitter(false)      // Kaggle score = 0.76831
        .setTwitter(true)          // Kaggle score = 0.77229
        .setLowercase(true)
        .setStopwords(true)
        .setStemming(true)
    ;

    public Entry(int id, String keyword, String location, String text, String target) {
        this.id       = id;
        this.keyword  = keyword;
        this.location = location;
        this.text     = text;
        this.target   = target;
    }

    public String toString() {
        return String.format("%s(%s, %s, %s, %s, %s)",
                this.getClass().getSimpleName(),
                this.id,
                this.keyword,
                this.location,
                this.text,
                this.target
        );
    }

    public List<String> tokenize() {
        return Stream.of(
                // this.keyword,
                // this.location,
                this.text
            )
            .map(this.tokenizer::tokenize)
            .flatMap(List<String>::stream)
            .collect(Collectors.toList())
        ;
    }

    public DocumentSample toDocumentSampleKeyword() {
        var tokens = this.tokenizer.tokenize(this.text).toArray(new String[0]);
        return new DocumentSample(this.keyword, tokens);
    }
    public DocumentSample toDocumentSampleTarget() {
        var tokens = this.tokenizer.tokenize(this.text).toArray(new String[0]);
        return new DocumentSample(this.target, tokens);
    }
}
