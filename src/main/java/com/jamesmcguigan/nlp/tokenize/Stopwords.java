package com.jamesmcguigan.nlp.tokenize;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Stopwords {
    private static final Pattern regexComment     = Pattern.compile("^#.+$");
    private static final Pattern regexPunctuation = Pattern.compile("^([!\"#$%&'()*+,./:;<=>?@\\[\\]^_`{|}~-])\\1*$");

    private Stopwords() {}

    public static final Set<String> stopwords = new BufferedReader(new InputStreamReader(
            Objects.requireNonNull(
                Stopwords.class.getClassLoader().getResourceAsStream("stopwords.txt")
            )
        ))
        .lines()
        .filter(token -> !Stopwords.regexComment.matcher(token).matches())
        .collect(Collectors.toUnmodifiableSet())
    ;

    public static List<String> removeStopwords(List<String> tokens) {
        tokens = tokens.stream()
            .filter(token -> !Stopwords.stopwords.contains(token))
            .filter(token -> !Stopwords.regexPunctuation.matcher(token).matches())
            .filter(token -> !token.isEmpty())
            .collect(Collectors.toList())
        ;
        return tokens;
    }
}
