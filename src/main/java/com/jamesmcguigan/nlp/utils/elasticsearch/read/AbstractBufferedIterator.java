package com.jamesmcguigan.nlp.utils.elasticsearch.read;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;


/**
 * Thread-safe base class for converting an external data feed into an Iterator.
 * <p/>
 * Performs synchronous HTTP request via {@code fetch()} on first iteration,
 * then attempts to asynchronously keep the buffer populated with at least {@code bufferSize} entries
 * <p/>
 * Subclasses should implement abstract methods: {@code fetch()} and {@code cast()}
 *
 * @param <T> autocast return type of the Iterator
 * @param <B> internal storage type for the buffer
 */
public abstract class AbstractBufferedIterator<T, B> implements Iterator<T> {
    private static final Logger logger = LogManager.getLogger();
    protected final Class<? extends T> type;

    protected int  requestSize = 1000;     // Number of items to load in buffer, pre-fetching may double this
    protected long ttl         = 360;      // API timeout in seconds

    @Nullable protected Long  totalHits;  // Total number of results from query
    protected Long pos = 0L;              // Position in the stream

    protected final Deque<B>          buffer = new ConcurrentLinkedDeque<>();
    protected CompletableFuture<Void> future = CompletableFuture.completedFuture(null);  // Semaphore for async promises


    //***** Constructors *****//

    protected AbstractBufferedIterator(Class<? extends T> type) {
        this.type = type;
        this.reset();
    }

    public void reset() {
        this.totalHits = null;
        this.pos       = 0L;
        this.buffer.clear();
        this.future.cancel(true);
    }


    //***** Getters / Setters *****//

    public Long getTotalHits() {
        if( this.totalHits == null ) { this.populateBuffer(true); }
        assert this.totalHits != null;
        return this.totalHits;
    }
    public Long    size()                    { return this.getTotalHits() - this.pos; }
    public boolean hasMoreRequests()         { return this.getTotalHits() - this.pos - this.buffer.size() > 0; }
    public int     getRequestSize()          { return this.requestSize; }
    public long    getTTL()                  { return this.ttl;  }

    public void    setRequestSize(int size)  { this.requestSize = size; }
    public void    setTTL(long ttl)          { this.ttl  = ttl;  }


    //***** Buffer functions *****//

    /**
     * Make synchronous IO request and return {@code requestSize} items of type {@code B}
     */
    protected abstract List<B> fetch() throws IOException;

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
            logger.debug(e);
        }
    }

    /**
     * Async prefetch of buffer, if not already pre-fetching
     */
    protected synchronized void prefetchBuffer() {
        // NOTE: synchronized due to this.future.isDone() - only have one request in flight
        if( this.buffer.size() < this.getRequestSize() && this.future.isDone() && this.hasMoreRequests() ) {
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


    //***** Casting *****//

    /**
     * Autocast bufferItem from B to T
     */
    public abstract T cast(B bufferItem);
}

