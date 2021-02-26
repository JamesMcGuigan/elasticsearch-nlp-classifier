package com.jamesmcguigan.nlp.utils.data;

import org.elasticsearch.client.core.TermVectorsResponse;

import java.util.Arrays;


/**
 * Wrapper class around TermVectorsResponse to provide easy access to tokenization
 * Tokens generated here are unique per field / document
 * <p/>
 * For duplicated tokens in accordance with {@code getTermFreq()}, use {@link TermVectorTokens}
 */
public class TermVectorDocTokens extends TermVectorTokens {
    public TermVectorDocTokens(TermVectorsResponse response) {
        super(response);
    }

    @Override
    public String[] tokenize() {
        String[] tokens = super.tokenize();
        tokens = Arrays.stream(tokens).distinct().toArray(String[]::new);
        return tokens;
    }

    @Override
    public String[] tokenize(String fieldName) {
        String[] tokens = super.tokenize(fieldName);
        tokens = Arrays.stream(tokens).distinct().toArray(String[]::new);
        return tokens;
    }
}
