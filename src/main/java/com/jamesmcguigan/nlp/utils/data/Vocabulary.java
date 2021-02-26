package com.jamesmcguigan.nlp.utils.data;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Vocabulary extends TreeSet<String> {
    Vocabulary(String[] words) {
        this(Arrays.asList(words));
    }
    Vocabulary(List<String> words) {
        this.addAll(words);
    }


    public static Vocabulary fromFile(String filename)       { return Vocabulary.fromFiles(Collections.singletonList(filename)); }
    public static Vocabulary fromFiles(String ...filenames)  { return Vocabulary.fromFiles(Arrays.asList(filenames)); }
    public static Vocabulary fromFiles(List<String> filenames) {
        var tweets = filenames.parallelStream()
                .map(Paths::get)
                .map(Tweets::fromCSV)
                .flatMap(List::stream)
                .collect(Collectors.toList())
        ;
        return Vocabulary.fromTweets(tweets);
    }


    public static Vocabulary fromTweet(Tweet tweet)     { return Vocabulary.fromTweets(Collections.singletonList(tweet)); }
    public static Vocabulary fromTweets(Tweet...tweets) { return Vocabulary.fromTweets(Arrays.asList(tweets)); }
    public static Vocabulary fromTweets(List<Tweet> tweets ) {
        var words = tweets.stream()
                .map(Tweet::tokenize)
                .flatMap(Stream::of)
                .distinct()
                .collect(Collectors.toList())
        ;
        return new Vocabulary(words);
    }
}
