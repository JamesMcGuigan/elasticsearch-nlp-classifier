package com.jamesmcguigan.nlp.elasticsearch;

import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;



class ScanAndScrollRequestTest {
    @Test
    void testIterator() {
        ScanAndScrollRequest request = new ScanAndScrollRequest("twitter", null);
        var size = request.size();
        assertTrue( size > 1000 );

        long count = 0;
        for( ScanAndScrollRequest it = request; it.hasNext(); ) {
            SearchHit hit = it.next();
            int id = Integer.parseInt( hit.getId() );
            assertTrue( hit instanceof SearchHit);
            assertTrue( id >= 0, Integer.toString(id));
            count++;
        }
        assertEquals((long) size, count);
    }

    @Test
    void testIteratorQuery() {
        String term = "disaster".toLowerCase(Locale.ROOT);
        var query   = new TermQueryBuilder("text", term);
        var request = new ScanAndScrollRequest("twitter", query);
        var size = request.size();
        assertTrue( size > 0 );
        assertTrue( size < 1000 );

        long count = 0;
        for( ScanAndScrollRequest it = request; it.hasNext(); ) {
            SearchHit hit = it.next();

            Object object = hit.getSourceAsMap().get("text");
            assertTrue( object instanceof String );

            String text = (String) hit.getSourceAsMap().get("text");
            assertTrue( hit instanceof SearchHit );
            assertTrue( text.toLowerCase(Locale.ROOT).contains(term), text );
            count++;
        }
        assertEquals((long) size, count);
    }
}
