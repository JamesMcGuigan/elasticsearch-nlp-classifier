package com.jamesmcguigan.nlp.utils.tokenize;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;


/**
 * This is a wrapper class around opennlp.tools.tokenize.Tokenizer providing some default implementations
 */
public abstract class ATokenizer implements Tokenizer {

    /**
     *  // Tokenizer interface
     *  String[] tokenize(String text);
     *  Span[]   tokenizePos(String text);
     */

    /**
     * Retokenize an array of tokens, which might result in a noop.
     *
     * @param tokens The list of tokens to be retokenized.
     * @return Possibly the same array of tokens
     */
    public String[] tokenize(String[] tokens) { return this.tokenize(List.of(tokens)); }

    /**
     * Tokenize and join an array of tests
     *
     * @param texts The list of texts to be retokenized.
     * @return An ordered array of tokens
     */
    public String[] tokenize(List<String> texts) {
        String[] tokens = texts.stream()
            .map(this::tokenize)
            .flatMap(Stream::of)
            .toArray(String[]::new)
        ;
        return tokens;
    }

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
