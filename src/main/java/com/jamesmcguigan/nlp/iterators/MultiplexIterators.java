package com.jamesmcguigan.nlp.iterators;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    // Shared lock between all children prevent race conditions in .stream().parallel() - tested as thread-safe
    ReentrantLock lock = new ReentrantLock();

    protected final Iterator<T> parentIterator;
    protected final Map<String, MultiplexIterator<T>> children;
    protected final List<String> names;


    //***** Constructors *****//

    /**
     * Creates a map of named children capable of reading from the same iterator
     * @param parentIterator shared iterator between all children
     * @param names          list of names for the children
     */
    public MultiplexIterators(Iterator<T> parentIterator, List<String> names) {
        this.names = names;
        this.parentIterator = parentIterator;
        this.children = // ImmutableMap.copyOf(
            names.stream().collect(Collectors.toMap(
                name -> name,
                name -> new MultiplexIterator<>(this, name)
            ));
        // );
    }

    /**
     * Creates a map of N numbered children capable of reading from the same iterator.
     * Internally stored as strings, but still accessible via {@code .get(int)} lookup
     * @param parentIterator shared iterator between all children
     * @param count          number of children to create
     */
    public MultiplexIterators(Iterator<T> parentIterator, int count) {
        this(parentIterator, namesFromCount(count));
    }

    /**
     * Converts a numeric count into a list of string names
     * @param count number of strings to generate
     * @return      list of stringified ints
     */
    public static List<String> namesFromCount(int count) {
        return Stream.iterate(0, n -> n + 1)
            .limit(count)
            .map(String::valueOf)
            .collect(Collectors.toList())
        ;
    }


    //***** Getters *****//

    public Map<String, MultiplexIterator<T>> getChildren() { return this.children; }

    /**
     * {@code multiplex.streamValues().parallel().forEach(childIterator -> {})} tested as thread-safe
     */
    public Stream<MultiplexIterator<T>> stream() {
        return this.children.values().stream();
    }
    public Stream<MultiplexIterator<T>> parallelStream() {
        return this.children.values().parallelStream();
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



