package com.jamesmcguigan.nlp.streams;

import com.jamesmcguigan.nlp.elasticsearch.actions.ScanAndScrollIterator;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.util.ObjectStream;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ESDocumentStream extends JsonDocumentStream implements ObjectStream<DocumentSample> {

    private String index;
    private String target;
    private List<String> fields;
    private QueryBuilder query;
    private ScanAndScrollIterator<String> iterator;


    public ESDocumentStream(String index, String field, String target, QueryBuilder query) throws IOException {
        this(index, Collections.singletonList(field), target, query);
    }
    public ESDocumentStream(String index, List<String> fields, String target, QueryBuilder query) throws IOException {
        this(
            new ScanAndScrollIterator<>(index, query, String.class),
            fields,
            target
        );
        this.index = index;
        this.query = query;   // matchQuery("title", "Elasticsearch")
    }
    private ESDocumentStream(Iterator<String> iterator, List<String> fields, String target) {
        super(iterator, fields, target);
    }


    @Override
    public void reset()                { this.iterator.reset(); }
    public Long size()                 { return this.iterator.size(); }
    public Long getTotalHits()         { return this.iterator.getTotalHits(); }
}
