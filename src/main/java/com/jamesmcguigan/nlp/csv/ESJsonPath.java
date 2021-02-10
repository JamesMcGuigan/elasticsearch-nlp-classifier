package com.jamesmcguigan.nlp.csv;

import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ESJsonPath {

    public static NLPTokenizer getDefaultTokenizer() {
        return new NLPTokenizer()
            // .setCleanTwitter(true)  // Kaggle score = 0.76248
            // .setTwitter(false)      // Kaggle score = 0.76831
            .setTwitter(true)          // Kaggle score = 0.77229
            .setLowercase(true)
            .setStopwords(true)
            .setStemming(true)
        ;
    }
    private NLPTokenizer tokenizer = getDefaultTokenizer();
    private final DocumentContext jsonPath;


    public ESJsonPath(String json) {
        this.jsonPath = JsonPath.parse(json);
    }
    public String toString() { return this.jsonPath.toString(); }
    public NLPTokenizer getTokenizer() { return tokenizer; }
    @SuppressWarnings("unchecked")
    public <T extends ESJsonPath> T setTokenizer(NLPTokenizer tokenizer) { this.tokenizer = tokenizer;return (T) this; }


    public String get(String path) { return get(path, ""); }
    public String get(String path, String defaultValue) {
        try {
            if( !path.startsWith("$") ) {
                path = "$." + path;
            }
            String text = jsonPath.read(path, String.class);
            return text;
        } catch ( com.jayway.jsonpath.PathNotFoundException exception ) {
            // System.out.printf("%s not found in %s%n", path, jsonPath.toString());
            // exception.printStackTrace();
            return defaultValue;
        }
    }


    public List<String> tokenize(String path)        { return tokenize(Collections.singletonList(path)); }
    public List<String> tokenize(List<String> paths) {
        List<String> tokens = new ArrayList<>();
        for( String path : paths ) {
            String text = get(path);
            List<String> tokenized = tokenizer.tokenize(text);
            tokens.addAll(tokenized);
        }
        return tokens;
    }
}
