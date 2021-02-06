package com.jamesmcguigan.nlp.tokenize;

import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import vendor.twittertokenizer.Twokenizer;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM.ENGLISH;

public class EntryTokenizer {

    //***** Default Settings *****//
    private boolean useStopwords = false;
    private boolean useLowercase = false;
    private boolean useTwitter = false;
    private boolean useCleanTwitter = false;
    private boolean useStemming = false;

    //***** Properties *****//
    private final Pattern regexTwitterHandle = Pattern.compile("^@");
    private final Pattern regexHashtag       = Pattern.compile("^#");
    private final Pattern regexUrl           = Pattern.compile("^\\w+://");

    private final Stemmer stemmer = new SnowballStemmer(ENGLISH);


    //***** Constructor *****//

    public EntryTokenizer setStopwords(boolean useStopwords) {
        this.useStopwords = useStopwords;
        return this;
    }
    public EntryTokenizer setLowercase(boolean useLowercase) {
        this.useLowercase = useLowercase;
        return this;
    }
    public EntryTokenizer setTwitter(boolean useTwitter) {
        this.useTwitter = useTwitter;
        return this;
    }
    public EntryTokenizer setCleanTwitter(boolean useCleanTwitter) {
        this.useTwitter      = useCleanTwitter;
        this.useCleanTwitter = useCleanTwitter;
        return this;
    }
    public EntryTokenizer setStemming(boolean useStemming) {
        this.useStemming = useStemming;
        return this;
    }


    //***** Methods *****//


    public List<String> tokenize(String text) {
        List<String> tokens = this.split(text);
        if( this.useCleanTwitter ) {
            tokens = this.cleanTwitter(tokens);
        }
        if( this.useLowercase ) {
            tokens = this.lowercase(tokens);
        }
        if( this.useStopwords ) {
            tokens = Stopwords.removeStopwords(tokens);
        }
        if( this.useStemming) {
            tokens = this.stem(tokens);
        }
        return tokens;
    }

    public List<String> split(String text) {
        if( this.useTwitter ) {
            Twokenizer tokenizer = new Twokenizer();
            List<String> tokens = tokenizer.twokenize(text);
            return tokens;
        } else {
            Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
            List<String> tokens = Arrays.asList( tokenizer.tokenize(text) );
            return tokens;
        }
    }

    public List<String> cleanTwitter(List<String> tokens) {
        tokens = tokens.stream()
            .filter(string -> !this.regexTwitterHandle.matcher(string).find())
            .filter(string -> !this.regexUrl.matcher(string).find())
            .map(string -> this.regexHashtag.matcher(string).replaceAll(""))
            .collect(Collectors.toList())
        ;
        return tokens;
    }

    public List<String> lowercase(List<String> tokens) {
        tokens = tokens.stream()
            .map(String::toLowerCase)
            .collect(Collectors.toList())
        ;
        return tokens;
    }

    public List<String> stem(List<String> tokens) {
        tokens = tokens.stream()
            .map(token -> this.stemmer.stem(token).toString())
            .collect(Collectors.toList())
        ;
        return tokens;
    }
}
