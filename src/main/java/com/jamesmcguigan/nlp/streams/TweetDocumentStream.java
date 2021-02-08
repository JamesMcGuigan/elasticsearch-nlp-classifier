package com.jamesmcguigan.nlp.streams;

import com.jamesmcguigan.nlp.csv.Tweet;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.util.ObjectStream;

import java.io.IOException;
import java.util.List;

public class TweetDocumentStream implements ObjectStream<DocumentSample> {
    protected final List<Tweet> tweets;
    protected int index;
    protected boolean closed;

    public TweetDocumentStream(List<Tweet> tweets) {
        this.tweets = tweets;
        this.index   = 0;
        this.closed  = false;
    }

    @Override
    public DocumentSample read() throws IOException {
        if( this.closed ) {
            throw new IOException("ObjectStream is closed");
        }
        if( this.index < this.tweets.size() ) {
            var tweet = this.tweets.get(this.index);
            var document = tweet.toDocumentSampleTarget();
            this.index += 1;
            return document;
        } else {
            return null;
        }
    }

    @Override
    public void reset() throws UnsupportedOperationException {
        this.index = 0;
    }

    @Override
    public void close() {
        this.tweets.clear();
        this.closed = true;
    }

}
