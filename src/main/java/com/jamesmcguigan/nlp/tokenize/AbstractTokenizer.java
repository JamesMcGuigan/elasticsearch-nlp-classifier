package com.jamesmcguigan.nlp.tokenize;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTokenizer implements Tokenizer {

    /** Noop if string has already been tokenized */
    public String[] tokenize(String[] tokens) { return tokens; }

    /**
     * Finds the boundaries of atomic parts in a string.
     * Required for the OpenNLP interface specification
     * Tested on basic input, but might get confused by a stemmer or lemmatizer
     *
     * @param text The string to be tokenized.
     * @return The Span[] with the spans (offsets into s) for each
     * token as the individuals array elements.
     */
    @Override
    public Span[] tokenizePos(String text) {
        String[]   tokens = this.tokenize(text);
        List<Span> spans  = new ArrayList<>();
        int pos = 0;
        for( String token : tokens ) {
            Span span = new Span(pos + 1, pos + 1, 0.0);

            int start = text.indexOf(token, pos);
            int end   = start + token.length();
            if( start >= pos ) {
                span = new Span(start, end, 1.0);
                pos  = end;
            }
            spans.add(span);
        }
        return spans.toArray(new Span[0]);
    }
}
