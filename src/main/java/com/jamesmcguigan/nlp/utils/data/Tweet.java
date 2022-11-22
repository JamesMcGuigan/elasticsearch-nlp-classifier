package com.jamesmcguigan.nlp.utils.data;

import com.jamesmcguigan.nlp.utils.tokenize.ATokenizer;
import com.jamesmcguigan.nlp.utils.tokenize.NLPTokenizer;
import opennlp.tools.doccat.DocumentSample;

import javax.annotation.Nullable;
import java.util.stream.Stream;

public class Tweet {

    public final int id;
    public final String keyword;
    public final String location;
    public final String text;
    public final String target;

    private final ATokenizer tokenizer = NLPTokenizer.getDefaultTokenizer();

    public Tweet(int id, String keyword, String location, String text, @Nullable String target) {
        this.id       = id;
        this.keyword  = keyword;
        this.location = location;
        this.text     = text;
        this.target   = (target == null) ? "" : target;
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

    public String[] tokenize() {
        return Stream.of(
                // this.keyword,
                // this.location,
                this.text
            )
            .map(this.tokenizer::tokenize)
            .flatMap(Stream::of)
            .toArray(String[]::new)
        ;
    }

    public DocumentSample toDocumentSampleKeyword() {
        var tokens = this.tokenizer.tokenize(this.text);
        return new DocumentSample(this.keyword, tokens);
    }
    public DocumentSample toDocumentSampleTarget() {
        String[] tokens = this.tokenizer.tokenize(this.text);
        return new DocumentSample(this.target, tokens);
    }
}
