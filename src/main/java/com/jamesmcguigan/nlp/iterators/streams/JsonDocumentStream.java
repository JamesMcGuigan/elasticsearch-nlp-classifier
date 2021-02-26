package com.jamesmcguigan.nlp.iterators.streams;

import com.jamesmcguigan.nlp.data.ESJsonPath;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.ObjectStream;

import java.util.Iterator;
import java.util.List;

public class JsonDocumentStream implements ObjectStream<DocumentSample> {
    protected final Iterator<String> iterator;
    protected final List<String>     fields;
    protected final String           target;
    protected Tokenizer tokenizer = ESJsonPath.getDefaultTokenizer();

    public JsonDocumentStream(Iterator<String> iterator, List<String> fields, String target) {
        this.iterator = iterator;
        this.fields   = fields;
        this.target   = target;
    }

    public Tokenizer getTokenizer() { return this.tokenizer; }
    @SuppressWarnings("unchecked")
    public <T extends JsonDocumentStream> T setTokenizer(Tokenizer tokenizer) { this.tokenizer = tokenizer; return (T) this; }


    /**
     * Returns the next object. Calling this method repeatedly until it returns
     * null will return each object from the underlying source exactly once.
     *
     * @return the next object or null to signal that the stream is exhausted
     */
    @Override
    public DocumentSample read() {
        if( this.iterator.hasNext() ) {
            return this.cast( this.iterator.next() );
        }
        return null;
    }


    protected DocumentSample cast(String json) {
        ESJsonPath jsonPath = new ESJsonPath(json).setTokenizer(this.tokenizer);
        String category     = jsonPath.get(this.target);
        String[] tokens     = jsonPath.tokenize(this.fields);
        var documentSample  = new DocumentSample(category, tokens);
        return documentSample;
    }
}
