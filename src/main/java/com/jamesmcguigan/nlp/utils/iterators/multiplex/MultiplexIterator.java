package com.jamesmcguigan.nlp.utils.iterators.multiplex;

import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * This is the child class of MultiplexIterators
 */
public class MultiplexIterator<T> implements Iterator<T> {
    protected final MultiplexIterators<T> parent;
    protected final String name;
    protected final Deque<T> buffer = new ConcurrentLinkedDeque<>();  // all access is also synchronized or locked

    protected MultiplexIterator(MultiplexIterators<T> parent, String name) {
        this.parent = parent;
        this.name = name;
    }


    //***** Getters / Setters *****//

    public String getName() { return this.name; }

    /**
     * This allows MultiplexIterators to dynamically add items to the buffer
     *
     * @param item the item to be added
     */
    protected void add(T item) { this.buffer.add(item); }


    //***** Iterator Interface *****//

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        // Lock is shared between all child iterators
        this.parent.lock.lock();
        try {
            return !this.buffer.isEmpty() || this.parent.hasNext();
        } finally {
            this.parent.lock.unlock();
        }
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public T next() {
        // Lock is shared between all child iterators
        this.parent.lock.lock();
        try {
            if( this.buffer.isEmpty() ) {
                this.parent.next();  // adds to this.buffer as side effect
            }
            if( this.buffer.isEmpty() ) {
                throw new NoSuchElementException();
            }
            return this.buffer.pop();
        } finally {
            this.parent.lock.unlock();
        }
    }
}
