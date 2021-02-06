package com.jamesmcguigan.nlp.tokenize;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

class EntryTokenizerTest {

    @DisplayName("Should calculate the correct sum")
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("datasetForCanTokenize")
    void canTokenize(String sentence, List<String> expectedTokens) {
        EntryTokenizer tokenizer = new EntryTokenizer()
            .setCleanTwitter(true)
            .setLowercase(true)
            .setStopwords(true)
            .setStemming(true)
        ;
        List<String> actualTokens = tokenizer.tokenize(sentence);
        Assertions.assertEquals(expectedTokens, actualTokens);
    }
    private static Stream<Arguments> datasetForCanTokenize() {
        return Stream.of(
            Arguments.of(
                "Hello World",
                Arrays.asList("hello", "world")
            ),
            Arguments.of(
                "Heard about #earthquakes is different cities, stay safe everyone.",
                Arrays.asList("heard", "earthquak", "differ",
                    "citi", "stay", "safe", "everyon")
            ),
            Arguments.of(
                "Burning Man Ablaze! by Turban Diva http://t.co/hodWosAmWS via @Etsy",
                Arrays.asList("burn", "man", "ablaz", "turban", "diva", "via")
            )
        );
    }
}
