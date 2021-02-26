package com.jamesmcguigan.nlp.utils.data;

import com.jamesmcguigan.nlp.utils.tokenize.NLPTokenizer;
import opennlp.tools.tokenize.Tokenizer;
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
        Tokenizer input  = new NLPTokenizer();
        Tokenizer output = new ESJsonPath("{}").setTokenizer(input).getTokenizer();
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
            Arguments.of("{ \"text\": 1 }",                            "text",            new String[]{ "1" }),
            Arguments.of("{ \"text\": \"hello world\" }",              "text",            new String[]{ "hello", "world" }),
            Arguments.of("{ \"_opennlp.target\": \"hello world\" }",   "_opennlp.target", new String[]{ "hello", "world" }),
            Arguments.of("{ \"_opennlp\": { \"target\": \"1.234\" }}", "_opennlp.target", new String[]{ "1.234" }),
            Arguments.of(
                """
                { 
                    "_opennlp.target": "hello world",
                    "_opennlp": { "target": "1.234" }
                }
                """,
                "_opennlp.target", new String[]{ "hello", "world" })  // prefer top-level key
        );
    }
    @ParameterizedTest
    @MethodSource("datasetForTokenize")
    void tokenize(String json, String path, String[] expected) {
        var jsonPath    = new ESJsonPath(json);
        String[] actual = jsonPath.tokenize(path);
        assertThat(actual).isEqualTo(expected);
    }
}
