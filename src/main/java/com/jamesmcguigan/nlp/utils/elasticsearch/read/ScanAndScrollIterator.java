package com.jamesmcguigan.nlp.utils.elasticsearch.read;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jamesmcguigan.nlp.utils.elasticsearch.ESClient;
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
import java.util.Arrays;
import java.util.List;


/**
 * ElasticSearch ScanAndScrollRequest implemented as an Iterator
 * <p/>
 * Performs synchronous HTTP request on first iteration,
 * then attempts to asynchronously keep the buffer populated with at least {@code bufferSize} entries
 *
 * @param <T> AutoCast = {@link SearchHit} | {@link String} | {@link JsonObject} | {@code JavaBean}
 */
public class ScanAndScrollIterator<T> extends BufferedIterator<T, SearchHit> {
    private final String index;
    @Nullable private final QueryBuilder query;
    @Nullable private final String[] fields;

    protected int  defaultRequestSize = 1000;  // Number of items to load in buffer, pre-fetching may double this
    protected long defaultTtl         = 360;   // API timeout in seconds

    @Nullable private String scrollId;  // ScrollId of current request
    protected final RestHighLevelClient client = ESClient.getInstance();


    //***** Constructors *****//

    public ScanAndScrollIterator(Class<? extends T> type, String index)                                throws IOException { this(type, index, null, null); }
    public ScanAndScrollIterator(Class<? extends T> type, String index, @Nullable QueryBuilder query ) throws IOException { this(type, index, null, query); }
    public ScanAndScrollIterator(Class<? extends T> type, String index, @Nullable List<String> fields) throws IOException { this(type, index, fields, null); }
    public ScanAndScrollIterator(
        Class<? extends T> type,
        String index,
        @Nullable List<String> fields,
        @Nullable QueryBuilder query
    ) throws IOException {
        super(type);
        this.index  = index;
        this.query  = query;
        this.fields = fields != null ? fields.toArray(new String[0]) : null;
        this.setRequestSize(this.defaultRequestSize);
        this.setTTL(this.defaultTtl);
        this.reset();
    }

    @Override
    public void reset() {
        super.reset();
        this.scrollId  = null;
    }


    //***** Buffer functions *****//

    @Override
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

    protected SearchRequest getScanAndScrollRequest() {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.11/java-rest-high-search.html
        SearchRequest searchRequest = new SearchRequest(this.index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(this.query);
        searchSourceBuilder.fetchSource(this.fields, null);
        searchSourceBuilder.size(this.requestSize);

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueSeconds(this.ttl));
        return searchRequest;
    }


    //***** Casting *****//

    @SuppressWarnings("unchecked")
    @Override
    public T cast(SearchHit bufferItem) {
        T item;
        // NOTE: Object.class.isAssignableFrom(String.class) == true
        // NOTE: String.class.isAssignableFrom(Object.class) == false
        if( this.type.isAssignableFrom( bufferItem.getClass() ) ) {
            item = (T) bufferItem;
        }
        else {
            String json = bufferItem.getSourceAsString();
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
