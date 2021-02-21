package com.jamesmcguigan.nlp.tokenize;

import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import vendor.twittertokenizer.Twokenizer;

import java.util.Arrays;
import java.util.regex.Pattern;

import static opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM.ENGLISH;

public class NLPTokenizer extends AbstractTokenizer {

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

    public NLPTokenizer setStopwords(boolean useStopwords) {
        this.useStopwords = useStopwords;
        return this;
    }
    public NLPTokenizer setLowercase(boolean useLowercase) {
        this.useLowercase = useLowercase;
        return this;
    }
    public NLPTokenizer setTwitter(boolean useTwitter) {
        this.useTwitter = useTwitter;
        return this;
    }
    public NLPTokenizer setCleanTwitter(boolean useCleanTwitter) {
        this.useTwitter      = useCleanTwitter;
        this.useCleanTwitter = useCleanTwitter;
        return this;
    }
    public NLPTokenizer setStemming(boolean useStemming) {
        this.useStemming = useStemming;
        return this;
    }


    //***** Methods *****//

    public String[] tokenize(String text) {
        String[] tokens = this.split(text);
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

    public String[] split(String text) {
        String[] tokens;
        if( this.useTwitter ) {
            var tokenizer = new Twokenizer();
            tokens = tokenizer.twokenize(text).toArray(new String[0]);
        } else {
            var tokenizer = SimpleTokenizer.INSTANCE;
            tokens = tokenizer.tokenize(text);
            return tokens;
        }
        return tokens;
    }

    public String[] cleanTwitter(String[] tokens) {
        tokens = Arrays.stream(tokens)
            .filter(string -> !this.regexTwitterHandle.matcher(string).find())
            .filter(string -> !this.regexUrl.matcher(string).find())
            .map(string -> this.regexHashtag.matcher(string).replaceAll(""))
            .toArray(String[]::new)
        ;
        return tokens;
    }

    public String[] lowercase(String[] tokens) {
        tokens = Arrays.stream(tokens)
            .map(String::toLowerCase)
            .toArray(String[]::new)
        ;
        return tokens;
    }

    public String[] stem(String[] tokens) {
        tokens = Arrays.stream(tokens)
            .map(token -> this.stemmer.stem(token).toString())
            .toArray(String[]::new)
        ;
        return tokens;
    }
}
