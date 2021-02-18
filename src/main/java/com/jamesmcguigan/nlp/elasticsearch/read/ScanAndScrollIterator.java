package com.jamesmcguigan.nlp.elasticsearch.read;

import com.google.gson.Gson;
import com.jamesmcguigan.nlp.elasticsearch.ESClient;
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
import org.json.JSONObject;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * ElasticSearch ScanAndScrollRequest implemented as an Iterator
 *
 * Caches `bufferSize` entries then makes a synchronous API call to refresh the buffer when empty.
 * .next() return type is cast to type T.
 *
 * @param <T> return type of .next()
 */
public class ScanAndScrollIterator<T> implements Iterator<T> {
    private final Class<? extends T> type;
    private final String index;
    @Nullable private final QueryBuilder query;
    @Nullable private final String[] fields;

    private int  bufferSize = 1000;     // Number of items to keep in buffer
    private long ttl        = 360;      // API timeout in seconds

    @Nullable private Long totalHits;   // Total number of results from query
    @Nullable private String scrollId;  // ScrollId of current request
    private Long pos = 0L;              // Position in the stream

    private final RestHighLevelClient client;
    private final Deque<SearchHit> buffer = new ConcurrentLinkedDeque<>();


    public ScanAndScrollIterator(Class<? extends T> type, String index)                                throws IOException { this(type, index, null, null); }
    public ScanAndScrollIterator(Class<? extends T> type, String index, @Nullable QueryBuilder query ) throws IOException { this(type, index, query, null); }
    public ScanAndScrollIterator(Class<? extends T> type, String index, @Nullable List<String> fields) throws IOException { this(type, index, null, fields); }
    public ScanAndScrollIterator(
        Class<? extends T> type,
        String index,
        @Nullable QueryBuilder query,
        @Nullable List<String> fields
    ) throws IOException {
        this.index  = index;
        this.query  = query;
        this.fields = fields != null ? fields.toArray(new String[0]) : null;
        this.type   = type;
        this.client = ESClient.getInstance();
        this.reset();
    }
    public void reset() {
        this.totalHits = null;
        this.scrollId  = null;
        this.pos       = 0L;
        this.buffer.clear();
    }


    //***** Getters / Setters *****//

    public int  getBufferSize()               { return this.bufferSize; }
    public long getTTL()                      { return this.ttl;  }
    public void setBufferSize(int bufferSize) { this.bufferSize = bufferSize; }
    public void setTTL(long ttl)              { this.ttl  = ttl;  }

    public Long size() {
        return this.getTotalHits() - this.pos;
    }
    public Long getTotalHits() {
        this.populateBuffer();
        return this.totalHits;
    }


    //***** ElasticSearch functions *****//

    protected void populateBuffer() {
        // TODO: Asynchronous loading of buffer
        try {
            if( this.totalHits == null || this.buffer.isEmpty() ) {
                SearchHits hits = this.scanAndScroll();
                this.buffer.addAll(Arrays.asList(hits.getHits()));
            }
        } catch( IOException e ) {
            e.printStackTrace();
        }
    }
    protected SearchRequest getScanAndScrollRequest() {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.11/java-rest-high-search.html
        SearchRequest searchRequest = new SearchRequest(this.index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(this.query);
        searchSourceBuilder.fetchSource(this.fields, null);
        searchSourceBuilder.size(this.bufferSize);

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueSeconds(this.ttl));
        return searchRequest;
    }
    protected SearchHits scanAndScroll() throws IOException {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search-scroll.html
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


    //***** Iterator interface *****//

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        this.populateBuffer();
        return !this.buffer.isEmpty();
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public T next() {
        if( this.hasNext() ) {
            SearchHit hit = this.buffer.pop();
            T item        = this.cast(hit);
            this.pos++;
            return item;
        } else {
            throw new NoSuchElementException();
        }
    }

    @SuppressWarnings("unchecked")
    public T cast(SearchHit hit) {
        T item;

        // NOTE: Object.class.isAssignableFrom(String.class) == true
        // NOTE: String.class.isAssignableFrom(Object.class) == false
        if( this.type.isAssignableFrom( hit.getClass() ) ) {
            item = (T) hit;
        }
        else {
            String json = hit.getSourceAsString();
            if( this.type.isAssignableFrom( String.class ) ) {
                item = (T) json;
            }
            else if( this.type.isAssignableFrom( JSONObject.class ) ) {
                item = (T) new JSONObject(json);
            }
            else {
                item = new Gson().fromJson(json, this.type);
            }
        }
        return item;
    }
}
