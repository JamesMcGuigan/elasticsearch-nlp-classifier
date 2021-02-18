package com.jamesmcguigan.nlp.elasticsearch.read;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 * ElasticSearch ScanAndScrollRequest implemented as an Iterator
 * <p/>
 * Performs synchronous HTTP request on first iteration,
 * then attempts to asynchronously keep the buffer populated with at least {@code bufferSize} entries
 *
 * @param <T> AutoCast = {@link SearchHit} | {@link String} | {@link JsonObject} | {@code JavaBean}
 */
public class ScanAndScrollIterator<T> implements Iterator<T> {
    private final Class<? extends T> type;
    private final String index;
    @Nullable private final QueryBuilder query;
    @Nullable private final String[] fields;

    private int  bufferSize = 1000;     // Number of items to load in buffer, pre-fetching may double this
    private long ttl        = 360;      // API timeout in seconds

    @Nullable private Long  totalHits;  // Total number of results from query
    @Nullable private String scrollId;  // ScrollId of current request
    private Long pos = 0L;              // Position in the stream

    private final RestHighLevelClient client;
    private final Deque<SearchHit>    buffer = new ConcurrentLinkedDeque<>();
    private CompletableFuture<Void>   future = CompletableFuture.completedFuture(null);  // Semaphore for async promises


    public ScanAndScrollIterator(Class<? extends T> type, String index)                                throws IOException { this(type, index, null, null); }
    public ScanAndScrollIterator(Class<? extends T> type, String index, @Nullable QueryBuilder query ) throws IOException { this(type, index, null, query); }
    public ScanAndScrollIterator(Class<? extends T> type, String index, @Nullable List<String> fields) throws IOException { this(type, index, fields, null); }
    public ScanAndScrollIterator(
        Class<? extends T> type,
        String index,
        @Nullable List<String> fields,
        @Nullable QueryBuilder query
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
        this.future.cancel(true);
    }


    //***** Getters / Setters *****//

    public Long getTotalHits() {
        if( this.totalHits == null ) { this.populateBuffer(true); }
        return this.totalHits;
    }
    public Long    size()                  { return this.getTotalHits() - this.pos; }
    public boolean hasMoreRequests()       { return this.getTotalHits() - this.pos - this.buffer.size() > 0; }
    public int     getBufferSize()         { return this.bufferSize; }
    public long    getTTL()                { return this.ttl;  }

    public void    setBufferSize(int size) { this.bufferSize = size; }
    public void    setTTL(long ttl)        { this.ttl  = ttl;  }



    //***** ElasticSearch functions *****//

    protected synchronized void populateBuffer() { this.populateBuffer(false); }
    protected synchronized void populateBuffer(boolean force) {
        // NOTE: synchronized due to this.buffer.isEmpty() - only have one request in flight
        try {
            // Synchronous fetching of buffer
            if( force || this.buffer.isEmpty() ) {
                this.buffer.addAll(this.fetch());
            } else {
                this.prefetchBuffer();
            }
        } catch( IOException e ) {
            e.printStackTrace();
        }
    }

    /**
     * Async prefetch of buffer, if not already pre-fetching
     */
    protected synchronized void prefetchBuffer() {
        // NOTE: synchronized due to this.future.isDone() - only have one request in flight
        if( this.buffer.size() < this.bufferSize && this.future.isDone() && this.hasMoreRequests() ) {
            this.future = CompletableFuture.runAsync(() -> this.populateBuffer(true));
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
    protected synchronized List<SearchHit> fetch() throws IOException {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search-scroll.html
        // NOTE: synchronized due to this.scrollId - only have one request in flight
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
        return Arrays.asList(hits.getHits());
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
            T item = this.cast(this.buffer.pop());
            this.pos++;
            return item;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Grab all the items currently in the buffer and async refresh
     */
    public synchronized List<T> popBuffer() {
        // NOTE: synchronized to prevent double reading of buffer
        // NOTE: Ensure this logic matches that of this.next()
        this.populateBuffer();  // synchronous reload if buffer is empty
        List<T> output = this.buffer.stream()
            .map(this::cast)
            .collect(Collectors.toList())
        ;
        this.pos += output.size();
        this.buffer.clear();    // pop() everything
        this.prefetchBuffer();  // async reload in anticipation of next call
        return output;
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
