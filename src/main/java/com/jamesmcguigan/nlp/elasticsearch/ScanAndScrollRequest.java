package com.jamesmcguigan.nlp.elasticsearch;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class ScanAndScrollRequest implements Iterator<SearchHit> {
    private final String index;
    private final QueryBuilder query;
    private final RestHighLevelClient client;
    private final LinkedList<SearchHit> buffer;
    private String scrollId;
    private int bufferSize = 1000;
    private long ttl = 60;
    private Long totalHits = null;

    public ScanAndScrollRequest(String index, QueryBuilder query) {
        this.index    = index;
        this.query    = query;
        this.scrollId = null;
        this.client   = ESClient.client;
        this.buffer   = new LinkedList<>();
    }
    public int  getBufferSize()               { return this.bufferSize; }
    public long getTTL()                      { return this.ttl;  }
    public void setBufferSize(int bufferSize) { this.bufferSize = bufferSize; }
    public void setTTL(long ttl)              { this.ttl  = ttl;  }
    // @Override
    // protected void finalize() { this.close(); }

    public Long size() {
        if( this.totalHits == null ) {
            this.updateBuffer();
        }
        return this.totalHits;
    }

    protected SearchRequest getScanAndScrollRequest() {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search-scroll.html
        SearchRequest searchRequest = new SearchRequest(this.index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(this.query);
        searchSourceBuilder.size(this.bufferSize);

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueSeconds(this.ttl));
        return searchRequest;
    }

    protected SearchHits scanAndScroll() throws IOException {
        SearchResponse searchResponse;
        if( this.scrollId == null ) {
            SearchRequest searchRequest = this.getScanAndScrollRequest();
            searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
            this.totalHits = searchResponse.getInternalResponse().hits().getTotalHits().value;
        } else {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(this.scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(this.ttl));
            searchResponse = this.client.scroll(scrollRequest, RequestOptions.DEFAULT);
        }
        this.scrollId   = searchResponse.getScrollId();
        SearchHits hits = searchResponse.getHits();
        return hits;
    }

    protected void updateBuffer() {
        // TODO: Asynchronous loading of buffer
        try {
            SearchHits hits = this.scanAndScroll();
            this.buffer.addAll( Arrays.asList(hits.getHits()) );
        } catch( IOException ignored ) {}
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        if( this.buffer.isEmpty() ) {
            this.updateBuffer();
        }
        return !this.buffer.isEmpty();
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public SearchHit next() {
        if( this.hasNext() ) {
            return this.buffer.pop();
        } else {
            throw new NoSuchElementException();
        }
    }
}
