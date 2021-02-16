package com.jamesmcguigan.nlp.iterators;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MultiplexIteratorsTest {

    List<String> names = Arrays.asList("first", "second", "third");

    @ParameterizedTest()
    @ValueSource(ints = {0, 1, 2, 4, 16})
    void newCount(int count) {
        Iterator<Integer> iterator = Stream.iterate(0, n -> n + 1).limit(10).iterator();
        MultiplexIterators<Integer> multiplex = new MultiplexIterators<>(iterator, count);

        assertEquals(multiplex.children.size(), count);
        for( int i = 0; i < count; i++ ) {
            var child = multiplex.get(i);
            assertNotNull(child);
            assertTrue(child.hasNext());
        }
        assertThrows(IllegalArgumentException.class, () -> multiplex.get(count + 1));
        assertThrows(IllegalArgumentException.class, () -> multiplex.get(-1));
    }

    @Test
    void newNames() {
        Iterator<Integer> iterator = Stream.iterate(0, n -> n + 1).limit(10).iterator();
        MultiplexIterators<Integer> multiplex = new MultiplexIterators<>(iterator, names);

        assertEquals(multiplex.children.size(), names.size());
        for( String name : names ) {
            var child = multiplex.get(name);
            assertNotNull(child);
            assertTrue(child.hasNext());
        }
        for( int i = 0; i < names.size(); i++ ) {
            final int finalI = i;
            assertThrows(IllegalArgumentException.class, () -> multiplex.get(finalI));
        }
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1, 2, 16})
    void next(int limit) {
        Iterator<Integer> iterator = Stream.iterate(0, n -> n + 1).limit(limit).iterator();
        MultiplexIterators<Integer> multiplex = new MultiplexIterators<>(iterator, names);

        for( String name : names ) {
            int count = 0;
            var child = multiplex.get(name);
            while( child.hasNext() ) {
                int value = child.next();
                assertEquals(count, value);
                count++;
            }
            assertEquals(limit, count);
            assertFalse(child.hasNext());
            assertThrows(NoSuchElementException.class, child::next);
        }
        assertFalse(multiplex.hasNext());
        assertTrue(multiplex.isEmpty());
        assertThrows(NoSuchElementException.class, multiplex::next);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1, 2, 16})
    void streamNext(int limit) {
        Iterator<Integer> iterator = Stream.iterate(0, n -> n + 1).limit(limit).iterator();
        MultiplexIterators<Integer> multiplex = new MultiplexIterators<>(iterator, names);
        multiplex.stream() // .parallel()
            .forEach(child -> {
                int count = 0;
                while( child.hasNext() ) {
                    int value = child.next();
                    assertEquals(count, value);
                    count++;
                }
                assertEquals(limit, count);
                assertFalse(child.hasNext());
                assertThrows(NoSuchElementException.class, child::next);
            })
        ;
        assertFalse(multiplex.hasNext());
        assertTrue(multiplex.isEmpty());
        assertThrows(NoSuchElementException.class, multiplex::next);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1, 2, 16, 1024})
    void parallelNext(int limit) {
        // What we are really testing for here is intermittent race conditions
        for( int i = 0; i < 1000; i++ ) {
            Iterator<Integer> iterator = Stream.iterate(0, n -> n + 1).limit(limit).iterator();
            MultiplexIterators<Integer> multiplex = new MultiplexIterators<>(iterator, names);
            multiplex.stream().parallel()
                .forEach(child -> {
                    int count = 0;
                    while( child.hasNext() ) {
                        int value = child.next();
                        assertEquals(count, value);
                        count++;
                    }
                    assertEquals(limit, count);
                    assertFalse(child.hasNext());
                    assertThrows(NoSuchElementException.class, child::next);
                })
            ;
            assertFalse(multiplex.hasNext());
            assertTrue(multiplex.isEmpty());
            assertThrows(NoSuchElementException.class, multiplex::next);
        }
    }


    @Test
    void emptySingleChild() {
        int limit = 10;
        Iterator<Integer> iterator = Stream.iterate(0, n -> n + 1).limit(limit).iterator();
        MultiplexIterators<Integer> multiplex = new MultiplexIterators<>(iterator, names);
        assertTrue( multiplex.hasNext() );

        // Empty out the first child
        var firstChild = multiplex.get("first");
        for( int count = 0; count < limit; count++ ) {
            assertTrue( multiplex.hasNext() );
            assertTrue( firstChild.hasNext() );
            int value = firstChild.next();
            assertEquals(count, value);
        }
        assertFalse(firstChild.hasNext());
        assertFalse(multiplex.hasNext());

        // Test the rest of the children still have everything in the buffer
        for( String name : names ) {
            if( name.equals("first") ) { continue; }
            var child = multiplex.get(name);
            assertEquals( child.buffer.size(), limit );
            assertTrue( child.hasNext() );
            int value = child.next();
            assertEquals(0, value);
        }
    }
}
