package com.jamesmcguigan.nlp.tokenize;

import opennlp.tools.util.Span;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

class TabTokenizerTest {
    private TabTokenizer tokenizer;

    @BeforeEach
    void setUp() {
        this.tokenizer = new TabTokenizer();
    }

    @Test
    void tokenize() {
        String   input    = "Pen\tPineapple\tApple\tPen";
        String[] tokens   = this.tokenizer.tokenize(input);
        String[] expected = new String[] { "Pen", "Pineapple", "Apple", "Pen" };
        assertThat(tokens).isEqualTo(expected);
    }

    @Test
    void tokenizePos() {
        String input = "Pen\tPineapple\tApple\tPen";
        Span[] spans = this.tokenizer.tokenizePos(input);
        Span[] expected = new Span[]{
            new Span( 0,  3, 1.0),
            new Span( 4, 13, 1.0),
            new Span(14, 19, 1.0),
            new Span(20, 23, 1.0),
        };
        assertThat(spans).isEqualTo(expected);
    }
}
