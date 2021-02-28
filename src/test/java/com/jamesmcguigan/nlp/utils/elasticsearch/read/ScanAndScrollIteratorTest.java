package com.jamesmcguigan.nlp.utils.elasticsearch.read;

import com.jamesmcguigan.nlp.utils.data.Tweet;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.*;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.*;


class ScanAndScrollIteratorTest {
    final String           index = "twitter";
    final String           term  = "disaster".toLowerCase();
    final TermQueryBuilder query = new TermQueryBuilder("text", term);

    @Test
    void testIterator() throws IOException {
        var request = new ScanAndScrollIterator<>(SearchHit.class, index);
        var size = request.getTotalHits();
        assertTrue( size > 1000 );

        long count = 0;
        while( request.hasNext() ) {
            SearchHit hit = request.next();
            int id = Integer.parseInt( hit.getId() );
            assertTrue( id >= 0, Integer.toString(id));
            count++;
        }
        assertEquals((long) size, count);
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "text", "text,location,keyword", "text,location"})
    void testIteratorFields(String field) throws IOException {
        var fields = Arrays.asList(field.split(","));
        var request = new ScanAndScrollIterator<>(SearchHit.class, index, fields);
        request.setRequestSize(3);
        assertTrue( request.hasNext() );

        Map<String, Object> hit = request.next().getSourceAsMap();
        assertThat(hit.keySet()).containsExactlyElementsIn(fields);
    }

    @Test
    void testIteratorQuery() throws IOException {
        var request = new ScanAndScrollIterator<>(SearchHit.class, index, query);
        var size = request.getTotalHits();
        assertTrue( size > 0 );
        assertTrue( size < 1000 );

        long count = 0;
        while( request.hasNext() ) {
            SearchHit hit = request.next();

            Object object = hit.getSourceAsMap().get("text");
            assertTrue( object instanceof String );

            String text = (String) hit.getSourceAsMap().get("text");
            assertTrue( text.toLowerCase().contains(term), text );
            count++;
        }
        assertEquals((long) size, count);
    }

    @Test
    void testIteratorTypedTweet() throws IOException {
        var request = new ScanAndScrollIterator<>(Tweet.class, index, query);
        var size = request.getTotalHits();
        assertTrue( size > 0 );

        long count = 0;
        while( request.hasNext() ) {
            Tweet hit = request.next();  // JavaBean
            assertTrue( hit.text.toLowerCase().contains(term), hit.toString() );
            count++;
        }
        assertEquals((long) size, count);
    }

    @Test
    void testIteratorTypedString() throws IOException {
        var request = new ScanAndScrollIterator<>(String.class, index, query);
        var size = request.getTotalHits();
        assertTrue( size > 0 );

        long count = 0;
        while( request.hasNext() ) {
            String hit = request.next();  // Raw JSON string
            assertTrue( hit.startsWith("{"), hit );
            assertTrue( hit.toLowerCase().contains(term), hit );
            count++;
        }
        assertEquals((long) size, count);
    }

    @Test
    void testIteratorMap() throws IOException {
        var request = new ScanAndScrollIterator<>(TreeMap.class, index, query);
        var size = request.getTotalHits();
        assertTrue( size > 0 );

        long count = 0;
        while( request.hasNext() ) {
            // Beware java generic type erasure
            var hit = request.next();         // type = Map | instance = TreeMap
            assertTrue( hit.get("id")   instanceof Double );  // JsonObject casts to Integer, Map casts to Double
            assertTrue( hit.get("text") instanceof String );
            assertTrue( ((double) hit.get("id")) >= 0 );
            assertTrue( ((String) hit.get("text")).toLowerCase().contains(term), hit.toString() );
            count++;
        }
        assertEquals((long) size, count);
    }


    @Test
    void testIteratorJSONObject() throws IOException {
        var request = new ScanAndScrollIterator<>(JSONObject.class, index, query);
        var size = request.getTotalHits();
        assertTrue( size > 0 );

        long count = 0;
        while( request.hasNext() ) {
            JSONObject hit = request.next();
            assertTrue( hit.get("id")   instanceof Integer );  // JsonObject casts to Integer, Map casts to Double
            assertTrue( hit.get("text") instanceof String  );
            assertTrue( ((int) hit.get("id")) >= 0 );
            assertTrue( ((String) hit.get("text")).toLowerCase().contains(term), hit.toString() );
            count++;
        }
        assertEquals((long) size, count);
    }


    @Test
    void testIteratorReset() throws IOException {
        var request = new ScanAndScrollIterator<>(SearchHit.class, index, query);
        for( int i : new int[]{ 0, 1 } ) {
            request.reset();  // This lets us reread the ScanAndScroll request from the beginning

            var size = request.size();
            assertTrue( size > 0 );
            assertTrue( size < 1000 );
            assertTrue( request.hasNext() );

            long count = 0;
            while( request.hasNext() ) {
                SearchHit hit = request.next();
                count++;
                assertNotNull( hit );
                assertEquals(size - count, request.size());
            }
            assertEquals((long) size, count);
            assertEquals(0L, request.size());
        }
    }


    @Test
    void testPopBuffer() throws IOException {
        int bufferSize = 100;
        var request    = new ScanAndScrollIterator<>(String.class, index, query);
        request.setRequestSize(bufferSize);
        Long totalHits = request.getTotalHits();

        // First read the buffer the old fashioned way via .next()
        ArrayList<String> resultsNext      = new ArrayList<>();
        ArrayList<String> resultsPopBuffer = new ArrayList<>();
        while( request.hasNext() ) {
            resultsNext.add(request.next());
        }

        // Reset and re-read the buffer using .popBuffer()
        request.reset();
        List<String> buffer;
        for( int pos = 0; pos <= totalHits; pos += bufferSize ) {
            buffer = request.popBuffer();
            resultsPopBuffer.addAll(buffer);

            // Test we have the correct size being returned
            assertThat(buffer.isEmpty()).isFalse();
            assertThat(buffer.size()).isAtMost(bufferSize);
            assertThat(buffer.size()).isAtLeast((int) Math.min(bufferSize, totalHits-pos));
        }

        // Test we can safely read an empty buffer after
        buffer = request.popBuffer();
        assertThat(buffer.isEmpty()).isTrue();

        // Test results count matches expected
        assertThat(resultsNext.size()).isEqualTo(totalHits);
        assertThat(resultsPopBuffer.size()).isEqualTo(totalHits);

        // Test results are unique with no duplicates
        assertThat(Set.copyOf(resultsNext)).containsExactlyElementsIn(resultsNext);           // is unique
        assertThat(Set.copyOf(resultsPopBuffer)).containsExactlyElementsIn(resultsPopBuffer); // is unique

        // Test both methods returns the exact same data
        // NOTE: async results not guaranteed to be in exactly the same order
        assertThat(resultsPopBuffer).containsExactlyElementsIn(resultsNext);
    }
}
