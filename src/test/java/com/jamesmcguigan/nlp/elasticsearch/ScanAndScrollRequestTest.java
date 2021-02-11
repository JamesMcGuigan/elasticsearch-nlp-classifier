package com.jamesmcguigan.nlp.elasticsearch;

import com.jamesmcguigan.nlp.csv.Tweet;
import com.jamesmcguigan.nlp.elasticsearch.actions.ScanAndScrollRequest;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;


class ScanAndScrollRequestTest {
    @Test
    void testIterator() throws IOException {
        var request = new ScanAndScrollRequest<>("twitter", null, SearchHit.class);
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

    @Test
    void testIteratorQuery() throws IOException {
        String term = "disaster".toLowerCase();
        var query   = new TermQueryBuilder("text", term);
        var request = new ScanAndScrollRequest<>("twitter", query, SearchHit.class);
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
        String term = "disaster".toLowerCase();
        var query   = new TermQueryBuilder("text", term);
        var request = new ScanAndScrollRequest<>("twitter", query, Tweet.class);
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
        String term = "disaster".toLowerCase();
        var query   = new TermQueryBuilder("text", term);
        var request = new ScanAndScrollRequest<>("twitter", query, String.class);
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
        String term = "disaster".toLowerCase();
        var query   = new TermQueryBuilder("text", term);
        var request = new ScanAndScrollRequest<>("twitter", query, TreeMap.class);
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
        String term = "disaster".toLowerCase();
        var query   = new TermQueryBuilder("text", term);
        var request = new ScanAndScrollRequest<>("twitter", query, JSONObject.class);
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
        String term = "disaster".toLowerCase();
        var query   = new TermQueryBuilder("text", term);
        var request = new ScanAndScrollRequest<>("twitter", query, SearchHit.class);
        for( int i = 0; i <= 2; i++ ) {
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

}
