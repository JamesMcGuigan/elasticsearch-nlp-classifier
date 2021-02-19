package com.jamesmcguigan.nlp.data;

import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ESJsonPath {
    private static final Logger logger = LogManager.getLogger();

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
    public String toString()           { return this.jsonPath.jsonString(); }
    public NLPTokenizer getTokenizer() { return tokenizer; }
    @SuppressWarnings("unchecked")
    public <T extends ESJsonPath> T setTokenizer(NLPTokenizer tokenizer) { this.tokenizer = tokenizer;return (T) this; }


    /**
     * JsonPath by default interprets '.' to mean nested object lookup
     * This breaks functionality in cases where a dot is uses as a string literal in a "top.level.key"
     * @param path  desired lookup path
     * @return      list of possible literal/nested path strings for JsonPath.read()
     */
    protected static List<String> getPossiblePaths(String path) {
        return path.contains(".")
            ? Arrays.asList(getLiteralPath(path), path)
            : Collections.singletonList(path)
        ;
    }
    protected static String getLiteralPath(String path) {
        return "$['" + path.replace("'", "\\'") + "']";
    }


    public String get(String path) { return get(path, ""); }
    public String get(String path, String defaultValue) {
        for( String encodedPath : getPossiblePaths(path) ) {
            try {
                String text = jsonPath.read(encodedPath, String.class);
                return text;
            } catch ( com.jayway.jsonpath.PathNotFoundException ignored ) { /* ignored */ }
        }
        logger.debug("{} not found in {}", path::toString, jsonPath::jsonString);
        return defaultValue;
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
