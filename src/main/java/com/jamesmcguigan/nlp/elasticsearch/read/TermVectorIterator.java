package com.jamesmcguigan.nlp.elasticsearch.read;

import com.jamesmcguigan.nlp.data.TermVectorDocTokens;
import com.jamesmcguigan.nlp.data.TermVectorTokens;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
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
public class TermVectorIterator<T> implements Iterator<T> {
    private final Class<? extends T> type;
    private final String index;
    private final List<String> fields;

    private int  bufferSize = 100;  // Reduce buffer size to reduce Connection-is-Closed errors
    private Long pos = 0L;          // Position in the stream

    private final ScanAndScrollIterator<SearchHit> scanAndScroll;
    private final Deque<TermVectorsResponse>       buffer = new ConcurrentLinkedDeque<>();
    private CompletableFuture<Void>                future = CompletableFuture.completedFuture(null);  // Semaphore for async promises


    public TermVectorIterator(Class<? extends T> type, String index)                      throws IOException { this(type, index, null, null); }
    public TermVectorIterator(Class<? extends T> type, String index, QueryBuilder query ) throws IOException { this(type, index, null, query); }
    public TermVectorIterator(Class<? extends T> type, String index, List<String> fields) throws IOException { this(type, index, fields, null); }
    public TermVectorIterator(
        Class<? extends T> type,
        String index,
        @Nullable List<String> fields,
        @Nullable QueryBuilder query
    ) throws IOException {
        this.index  = index;
        this.type   = type;
        this.fields = fields;
        this.scanAndScroll = new ScanAndScrollIterator<>(SearchHit.class, index, singletonList("id"), query);
        this.scanAndScroll.setBufferSize(this.bufferSize);
        this.reset();
    }
    public void reset() {
        this.pos = 0L;
        this.buffer.clear();
        this.scanAndScroll.reset();
    }


    //***** Getters / Setters *****//

    public Long size()                  { return this.scanAndScroll.getTotalHits() - this.pos; }
    public Long getTotalHits()          { return this.scanAndScroll.getTotalHits();     }
    public boolean hasMoreRequests()    { return this.scanAndScroll.hasMoreRequests();  }
    public int  getBufferSize()         { return this.scanAndScroll.getBufferSize();    }
    public long getTTL()                { return this.scanAndScroll.getTTL();           }

    public void setBufferSize(int size) { this.scanAndScroll.setBufferSize(size); this.bufferSize = size; }
    public void setTTL(long ttl)        { this.scanAndScroll.setTTL(ttl);               }


    protected List<String> getScanAndScrollIds() {
        List<String> ids = this.scanAndScroll.popBuffer().stream()
            .map(SearchHit::getId)
            .collect(Collectors.toList())
        ;
        return ids;
    }

    protected List<TermVectorsResponse> fetch() throws IOException {
        List<String> ids = this.getScanAndScrollIds();
        List<TermVectorsResponse> responses =
            new TermVectorQuery(this.index, this.fields).getMultiTermVectors(ids);
        return responses;
    }

    //***** ElasticSearch functions *****//

    protected synchronized void populateBuffer() { this.populateBuffer(false); }
    protected synchronized void populateBuffer(boolean force) {
        // NOTE: synchronized due to this.buffer.size() - only have one request in flight
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
        if( this.buffer.size() < this.getBufferSize() && this.future.isDone() && this.hasMoreRequests() ) {
            this.future = CompletableFuture.runAsync(() -> this.populateBuffer(true));
        }
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
            TermVectorsResponse hit = this.buffer.pop();
            T item = this.cast(hit);
            this.pos++;
            return item;
        } else {
            throw new NoSuchElementException();
        }
    }


    @SuppressWarnings("unchecked")
    public T cast(TermVectorsResponse response) {
        T item = null;

        // NOTE: Object.class.isAssignableFrom(String.class) == true
        // NOTE: String.class.isAssignableFrom(Object.class) == false
        if( this.type.isAssignableFrom( response.getClass() ) ) {
            item = (T) response;
        }
        else if( this.type.equals(TermVectorDocTokens.class) ) {
            item = (T) new TermVectorDocTokens(response);
        }
        else if( this.type.equals(TermVectorTokens.class) ) {
            item = (T) new TermVectorTokens(response);
        }
        else if( this.type.equals(String[].class) ) {
            item = (T) new TermVectorTokens(response).tokenize().toArray(new String[0]);
        }
        if( item == null ) {
            throw new IllegalArgumentException("unsupported type: " + this.type.getCanonicalName());
        }
        return item;
    }
}
