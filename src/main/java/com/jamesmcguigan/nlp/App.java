package com.jamesmcguigan.nlp;

import com.jamesmcguigan.nlp.enricher.OpenNLPEnricher;
import com.jamesmcguigan.nlp.enricher.classifier.OpenNLPClassifierES;
import com.jamesmcguigan.nlp.enricher.classifier.OpenNLPClassifierTweet;

import java.io.IOException;

public class App {
    public static void main( String[] args ) throws IOException {
        OpenNLPClassifierTweet.main(args);
        OpenNLPClassifierES.main(args);
        OpenNLPEnricher.main(args);
    }
}
