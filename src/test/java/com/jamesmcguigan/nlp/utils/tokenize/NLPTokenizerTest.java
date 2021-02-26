package com.jamesmcguigan.nlp.utils.tokenize;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;

class NLPTokenizerTest {

    @DisplayName("Should calculate the correct sum")
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("datasetForCanTokenize")
    void canTokenize(String sentence, String[] expectedTokens) {
        NLPTokenizer tokenizer = new NLPTokenizer()
            .setCleanTwitter(true)
            .setLowercase(true)
            .setStopwords(true)
            .setStemming(true)
        ;
        String[] actualTokens = tokenizer.tokenize(sentence);
        assertThat(actualTokens).isEqualTo(expectedTokens);
    }
    private static Stream<Arguments> datasetForCanTokenize() {
        return Stream.of(
            Arguments.of(
                "Hello World",
                new String[]{ "hello", "world" }
            ),
            Arguments.of(
                "Heard about #earthquakes is different cities, stay safe everyone.",
                new String[]{ "heard", "earthquak", "differ",
                    "citi", "stay", "safe", "everyon" }
            ),
            Arguments.of(
                "Burning Man Ablaze! by Turban Diva http://t.co/hodWosAmWS via @Etsy",
                new String[]{ "burn", "man", "ablaz", "turban", "diva", "via" }
            )
        );
    }
}
