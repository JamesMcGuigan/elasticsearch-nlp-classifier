package com.jamesmcguigan.nlp.utils.elasticsearch.read;

import com.jamesmcguigan.nlp.utils.data.TermVectorDocTokens;
import com.jamesmcguigan.nlp.utils.data.TermVectorTokens;
import com.jamesmcguigan.nlp.utils.elasticsearch.ESClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * ElasticSearch MultiTermVector query implemented as an Iterator
 * <p/>
 * Uses {@link ScanAndScrollIterator} to query ids,
 * then performs HTTP {@code _mtermvectors} query to fetch termVectors
 * <p/>
 * Performs synchronous HTTP request on first iteration,
 * then attempts to asynchronously keep the buffer populated with at least {@code bufferSize} entries
 * <p/>
 *
 * @param <T> AutoCast = {@link TermVectorsResponse} | {@link TermVectorDocTokens} | {@link TermVectorTokens} | {@code String[]}
 */
public class TermVectorIterator<T> extends BufferedIterator<T, TermVectorsResponse> {
    private final String index;
    private final List<String> fields;

    protected int  defaultRequestSize = 100;  // Reduce buffer size to reduce Connection-is-Closed errors
    protected long defaultTtl         = 360;   // API timeout in seconds

    private final ScanAndScrollIterator<SearchHit> scanAndScroll;
    protected final RestHighLevelClient client = ESClient.getInstance();


    //***** Constructors *****//

    public TermVectorIterator(Class<? extends T> type, String index, List<String> fields) throws IOException { this(type, index, fields, null); }
    public TermVectorIterator(
        Class<? extends T> type,
        String index,
        List<String> fields,
        @Nullable QueryBuilder query
    ) throws IOException {
        super(type);
        this.index  = index;
        this.fields = fields;
        this.scanAndScroll = new ScanAndScrollIterator<>(SearchHit.class, index, singletonList("id"), query);
        this.scanAndScroll.setRequestSize(this.requestSize);
        this.setRequestSize(this.defaultRequestSize);
        this.setTTL(this.defaultTtl);
        this.reset();

        if( fields == null ) { throw new AssertionError("_mtermvectors returns empty results if no fields are specified"); }
    }
    @Override
    public void reset() {
        super.reset();
        if( this.scanAndScroll != null ) { this.scanAndScroll.reset(); }
    }


    //***** Getters / Setters *****//

    @Override public Long    getTotalHits()        { return this.scanAndScroll.getTotalHits();     }
    @Override public boolean hasMoreRequests()     { return this.scanAndScroll.hasMoreRequests();  }
    @Override public int     getRequestSize()      { return this.scanAndScroll.getRequestSize();   }
    @Override public long    getTTL()              { return this.scanAndScroll.getTTL();           }

    @Override public void setRequestSize(int size) { super.setRequestSize(size); this.scanAndScroll.setRequestSize(size); }
    @Override public void setTTL(long ttl)         { super.setTTL(ttl);          this.scanAndScroll.setTTL(ttl);          }



    //***** Buffer functions *****//

    @Override
    protected List<TermVectorsResponse> fetch() throws IOException {
        List<String> ids = this.getScanAndScrollIds();
        List<TermVectorsResponse> responses =
            new TermVectorQuery(this.index, this.fields).getMultiTermVectors(ids);
        return responses;
    }

    protected List<String> getScanAndScrollIds() {
        List<String> ids = this.scanAndScroll.popBuffer().stream()
            .map(SearchHit::getId)
            .collect(Collectors.toList())
        ;
        return ids;
    }


    //***** Casting *****//

    @Override
    @SuppressWarnings("unchecked")
    public T cast(TermVectorsResponse bufferItem) {
        T item = null;

        // NOTE: Object.class.isAssignableFrom(String.class) == true
        // NOTE: String.class.isAssignableFrom(Object.class) == false
        if( this.type.isAssignableFrom( bufferItem.getClass() ) ) {
            item = (T) bufferItem;
        }
        else if( this.type.equals(TermVectorDocTokens.class) ) {
            item = (T) new TermVectorDocTokens(bufferItem);
        }
        else if( this.type.equals(TermVectorTokens.class) ) {
            item = (T) new TermVectorTokens(bufferItem);
        }
        else if( this.type.equals(String[].class) ) {
            item = (T) new TermVectorTokens(bufferItem).tokenize();
        }
        else if( this.type.equals(String.class) ) {
            String[] tokens = new TermVectorTokens(bufferItem).tokenize();
            item = (T) String.join("\t", tokens);
        }
        if( item == null ) {
            throw new IllegalArgumentException("unsupported type: " + this.type.getCanonicalName());
        }
        return item;
    }
}
