package com.jamesmcguigan.nlp.util;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * MultiplexIterators allows a single iterator to split and consumed multiple times
 * Children can either be defined by count, or defined by a list of names
 *
 * Internally the parentIterator is buffered until all children have consumed it
 * Warning: Buffer may potentially be memory intensive if the children are not consumed in parallel
 * Warning: supplied parentIterator should not be accessed after MultiplexIterators initialization
 *
 * @param <T> type of iterator being multiplexed
 */
public class MultiplexIterators<T> {
    // Shared lock between all children prevent race conditions in .stream().parallel() work
    ReentrantLock lock = new ReentrantLock();

    protected final Iterator<T> parentIterator;
    protected final Map<String, MultiplexIterator<T>> children = new HashMap<>();
    protected final List<String> names;


    /**
     * Creates a map of N numbered children capable of reading from the same iterator
     * @param parentIterator shared iterator between all children
     * @param count          number of children to create
     */
    MultiplexIterators(Iterator<T> parentIterator, int count) {
        this(
            parentIterator,
            Stream.iterate(0, n -> n + 1)
                .limit(count)
                .map(String::valueOf)
                .collect(Collectors.toList())
        );
    }

    /**
     * Creates a map of named children capable of reading from the same iterator
     * @param parentIterator shared iterator between all children
     * @param names          list of names for the children
     */
    MultiplexIterators(Iterator<T> parentIterator, List<String> names) {
        this.names = names;
        this.parentIterator = parentIterator;
        for( String name : names ) {
            this.children.put(name, new MultiplexIterator<>(this, name));
        }
    }



    /**
     * multiplex.stream().parallel().forEach(child -> {}) tested as thread-safe
     */
    public Stream<MultiplexIterator<T>> stream() {
        return this.children.values().stream();
    }

    /**
     * If initialized by count, children can be accessed via int index, which is internally stored as a string
     * @param index  numeric name of the child
     * @return       the child iterator
     */
    public MultiplexIterator<T> get(int index) {
        return this.get(String.valueOf(index));
    }

    /**
     * If initialized by a list of names, children can be accessed directly by name
     * @param name  name of the child
     * @return      the child iterator
     */
    public MultiplexIterator<T> get(String name) {
        var child = this.children.get(name);
        if( child == null ) {
            throw new IllegalArgumentException(String.format("%s not in %s", name, String.join(",", this.names)));
        }
        return child;
    }



    //***** Iterator Interface *****//

    /**
     * @return {@code true} if the all children have completed iteration
     */
    public boolean isEmpty() {
        return this.stream().noneMatch(MultiplexIterator::hasNext);
    }

    /**
     * It will return {@code true} if at least one of the children has consumed all the items in the parent iterator
     * This is a protected iterator interface only to be called by MultiplexIterator
     * @return {@code true} if the parentIterator is empty
     */
    protected boolean hasNext() {
        return parentIterator.hasNext();
    }

    /**
     * Reads the next item from parentIterator and adds it to all child buffers
     * This is a protected iterator interface only to be called by MultiplexIterator
     */
    protected synchronized void next() {
        T item = parentIterator.next();
        this.children.forEach((name, child) -> child.add(item));
    }
}


/**
 * This is the child class of MultiplexIterators
 */
class MultiplexIterator<T> implements Iterator<T> {
    protected final MultiplexIterators<T> parent;
    protected final String name;
    protected final Deque<T> buffer = new ConcurrentLinkedDeque<>();  // all access is also synchronized or locked

    protected MultiplexIterator(MultiplexIterators<T> parent, String name) {
        this.parent = parent;
        this.name   = name;
    }

    /**
     * This allows MultiplexIterators to dynamically add items to the buffer
     * @param item the item to be added
     */
    protected void add(T item) {
        this.buffer.add(item);
    }


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

