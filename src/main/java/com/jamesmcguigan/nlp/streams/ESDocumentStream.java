package com.jamesmcguigan.nlp.streams;

import com.jamesmcguigan.nlp.elasticsearch.ScanAndScrollRequest;
import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.util.ObjectStream;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("FieldCanBeLocal")
public class ESDocumentStream implements ObjectStream<DocumentSample> {

    private final String index;
    private final String target;
    private final List<String> fields;
    private final QueryBuilder query;
    private final ScanAndScrollRequest<String> iterator;

    private NLPTokenizer tokenizer = new NLPTokenizer()
        // .setCleanTwitter(true)  // Kaggle score = 0.76248
        // .setTwitter(false)      // Kaggle score = 0.76831
        .setTwitter(true)          // Kaggle score = 0.77229
        .setLowercase(true)
        .setStopwords(true)
        .setStemming(true)
    ;



    public ESDocumentStream(String index, String field, String target, QueryBuilder query) {
        this(index, Collections.singletonList(field), target, query);
    }
    public ESDocumentStream(String index, List<String> fields, String target, QueryBuilder query) {
        this.index    = index;
        this.fields   = fields;
        this.target   = target;
        this.query    = query;   // matchQuery("title", "Elasticsearch")
        this.iterator = new ScanAndScrollRequest<>(this.index, this.query, String.class);
    }
    @Override
    public void reset() {
        this.iterator.reset();
    }

    public Long size()                 { return this.iterator.size(); }
    public Long getTotalHits()         { return this.iterator.getTotalHits(); }
    public NLPTokenizer getTokenizer() { return this.tokenizer; }
    @SuppressWarnings("unchecked")
    public <T extends ESDocumentStream> T setTokenizer(NLPTokenizer tokenizer) {
        this.tokenizer = tokenizer;
        return (T) this;
    }

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
        var jsonPath = JsonPath.parse(json);
        String category = this.readJsonPath(jsonPath, this.target);

        List<String> tokens = new ArrayList<>();
        for( String field : this.fields ) {
            String text = this.readJsonPath(jsonPath, field);
            List<String> tokenized = this.tokenizer.tokenize(text);
            tokens.addAll(tokenized);
        }

        var documentSample = new DocumentSample(category, tokens.toArray(new String[0]));
        return documentSample;
    }

    protected String readJsonPath(DocumentContext jsonPath, String path) {
        return readJsonPath(jsonPath, path, "");
    }
    @SuppressWarnings("SameParameterValue")
    protected String readJsonPath(DocumentContext jsonPath, String path, String defaultValue) {
        try {
            if( !path.startsWith("$") ) {
                path = "$." + path;
            }
            String text = jsonPath.read(path, String.class);
            return text;
        } catch ( com.jayway.jsonpath.PathNotFoundException exception ) {
            return defaultValue;
        }
    }
}
