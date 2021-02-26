package com.jamesmcguigan.nlp.utils.data;

import org.elasticsearch.client.core.TermVectorsResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Wrapper class around TermVectorsResponse to provide easy access to tokenization
 * Tokens generated here are duplicated in accordance with {@code getTermFreq()}
 * <p/>
 * For a list of unique document tokens, use {@link TermVectorDocTokens}
 */
public class TermVectorTokens {
    private final TermVectorsResponse response;
    private final String id;
    private final List<String> fields;

    public TermVectorTokens(TermVectorsResponse response) {
        this.response = response;
        this.id       = response.getId();
        this.fields   = response.getTermVectorsList().stream()
            .map(TermVectorsResponse.TermVector::getFieldName)
            .collect(Collectors.toList())
        ;
    }
    String       getId()     { return this.id;     }
    List<String> getFields() { return this.fields; }

    /**
     * @return List of all tokens, for all fields,
     *         duplicated in accordance with getTermFreq()
     */
    public String[] tokenize() {
        String[] tokens = this.getFields().stream()
            .map(this::tokenize)
            .flatMap(Stream::of)
            .toArray(String[]::new)
        ;
        return tokens;
    }

    /**
     * @return List of all tokens, for a given fieldName,
     *         duplicated in accordance with getTermFreq()
     */
    public String[] tokenize(String fieldName) {
        String[] tokens = response.getTermVectorsList().stream()
            .filter(termVector -> termVector.getFieldName().equals(fieldName))
            .map(TermVectorsResponse.TermVector::getTerms)
            .flatMap(Collection::stream)
            .map(term -> {
                // Create getTermFreq() copies of each getTerm()
                List<String> termTokens = Collections.nCopies(
                    term.getTermFreq(),
                    term.getTerm()
                );
                return termTokens;
            })
            .flatMap(Collection::stream)
            .toArray(String[]::new)
        ;
        return tokens;
    }
}
