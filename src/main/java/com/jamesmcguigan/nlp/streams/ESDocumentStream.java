package com.jamesmcguigan.nlp.streams;

import com.jamesmcguigan.nlp.csv.ESJsonPath;
import com.jamesmcguigan.nlp.elasticsearch.ScanAndScrollRequest;
import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.util.ObjectStream;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ESDocumentStream implements ObjectStream<DocumentSample> {

    private final String index;
    private final String target;
    private final List<String> fields;
    private final QueryBuilder query;
    private final ScanAndScrollRequest<String> iterator;
    private NLPTokenizer tokenizer = ESJsonPath.getDefaultTokenizer();


    public ESDocumentStream(String index, String field, String target, QueryBuilder query) throws IOException {
        this(index, Collections.singletonList(field), target, query);
    }
    public ESDocumentStream(String index, List<String> fields, String target, QueryBuilder query) throws IOException {
        this.index    = index;
        this.fields   = fields;
        this.target   = target;
        this.query    = query;   // matchQuery("title", "Elasticsearch")
        this.iterator = new ScanAndScrollRequest<>(this.index, this.query, String.class);
    }
    @Override
    public void reset()                { this.iterator.reset(); }
    public Long size()                 { return this.iterator.size(); }
    public Long getTotalHits()         { return this.iterator.getTotalHits(); }
    public NLPTokenizer getTokenizer() { return this.tokenizer; }
    public <T extends ESDocumentStream> T setTokenizer(NLPTokenizer tokenizer) { this.tokenizer = tokenizer; return (T) this; }

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
        ESJsonPath jsonPath = new ESJsonPath(json);
        String category     = jsonPath.get(this.target);
        List<String> tokens = jsonPath.tokenize(this.fields);
        var documentSample  = new DocumentSample(category, tokens.toArray(new String[0]));
        return documentSample;
    }
}
