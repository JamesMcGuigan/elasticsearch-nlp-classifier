package com.jamesmcguigan.nlp.utils.tokenize;

/**
 * Splits a tab separated string
 */
public class TabTokenizer extends AbstractTokenizer {
    @Override
    public String[] tokenize(String text) {
        return text.split("\t");
    }
}
