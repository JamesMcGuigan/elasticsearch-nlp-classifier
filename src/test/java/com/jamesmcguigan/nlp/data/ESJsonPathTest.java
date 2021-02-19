package com.jamesmcguigan.nlp.data;

import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;


class ESJsonPathTest {

    @ParameterizedTest
    @CsvSource({"""
        target,          $['target']
        _opennlp.target, $['_opennlp.target']
    """})
    void getLiteralPath(String input, String expected) {
        String actual = ESJsonPath.getLiteralPath(input);
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = { "{}", "{\"answer\":42}"} )
    void testToString(String input) {
        String output = new ESJsonPath(input).toString();  // renders without whitespace
        assertThat(output).isEqualTo(input);
    }

    @Test
    void getSetTokenizer() {
        NLPTokenizer input  = new NLPTokenizer();
        NLPTokenizer output = new ESJsonPath("{}").setTokenizer(input).getTokenizer();
        assertEquals(input, output);
    }

    private static Stream<Arguments> datasetForGetPossiblePaths() {
        return Stream.of(
            Arguments.of("target",          Collections.singletonList("target")),
            Arguments.of("_opennlp.target", Arrays.asList("$['_opennlp.target']", "_opennlp.target"))
        );
    }
    @ParameterizedTest
    @MethodSource("datasetForGetPossiblePaths")
    void getPossiblePaths(String input, List<String> expected) {
        List<String> actual = ESJsonPath.getPossiblePaths(input);
        Assertions.assertIterableEquals(expected, actual);
    }



    @ParameterizedTest
    @CsvSource({"""
        { "target":          1   },       target,          1,
        { "target":          "0" },       target,          0,
        { "_opennlp.target": "1" },       _opennlp.target, 1,
        { "_opennlp": { "target": "0" }}, _opennlp.target, 0,
    """})
    void get(String json, String path, String expected) {
        var jsonPath  = new ESJsonPath(json);
        String actual = jsonPath.get(path);
        assertEquals(expected, actual);
        assertEquals(String.class, actual.getClass());
    }



    private static Stream<Arguments> datasetForTokenize() {
        return Stream.of(
            Arguments.of("{ \"text\": 1 }",                            "text",            Collections.singletonList("1")),
            Arguments.of("{ \"text\": \"hello world\" }",              "text",            Arrays.asList("hello", "world")),
            Arguments.of("{ \"_opennlp.target\": \"hello world\" }",   "_opennlp.target", Arrays.asList("hello", "world")),
            Arguments.of("{ \"_opennlp\": { \"target\": \"1.234\" }}", "_opennlp.target", Collections.singletonList("1.234")),
            Arguments.of(
                """
                { 
                    "_opennlp.target": "hello world",
                    "_opennlp": { "target": "1.234" }
                }
                """,
                "_opennlp.target", Arrays.asList("hello", "world"))  // prefer top-level key
        );
    }
    @ParameterizedTest
    @MethodSource("datasetForTokenize")
    void tokenize(String json, String path, List<String> expected) {
        var jsonPath        = new ESJsonPath(json);
        List<String> actual = jsonPath.tokenize(path);
        Assertions.assertIterableEquals(expected, actual);
    }
}
