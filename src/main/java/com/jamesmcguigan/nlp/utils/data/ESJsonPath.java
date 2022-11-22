package com.jamesmcguigan.nlp.utils.data;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ESJsonPath {
    private static final Logger logger = LogManager.getLogger();
    private final DocumentContext jsonPath;


    public ESJsonPath(String json)  { this.jsonPath = JsonPath.parse(json); }
    public String toString()        { return this.jsonPath.jsonString(); }


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


    public List<String> get(List<String> paths) {
        return paths.stream().map(this::get).collect(Collectors.toList());
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
}
