package com.jamesmcguigan.nlp.streams;

import com.jamesmcguigan.nlp.csv.Entry;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.util.ObjectStream;

import java.io.IOException;
import java.util.List;

public class EntryDocumentStream implements ObjectStream<DocumentSample> {
    private final List<Entry> entries;
    private int index;
    private boolean closed;

    public EntryDocumentStream(List<Entry> entries) {
        this.entries = entries;
        this.index   = 0;
        this.closed  = false;
    }

    @Override
    public DocumentSample read() throws IOException {
        if( this.closed ) {
            throw new IOException("EntryDocumentStream is closed");
        }
        if( this.index < this.entries.size() ) {
            var entry = this.entries.get(this.index);
            var document = entry.toDocumentSampleTarget();
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
        this.closed = true;
    }

}
