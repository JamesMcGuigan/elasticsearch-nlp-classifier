package com.jamesmcguigan.nlp.elasticsearch;

import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScanAndScrollRequestTest {
    @Test
    void testIterator() {
        ScanAndScrollRequest request = new ScanAndScrollRequest("twitter", null);
        var size = request.size();
        assertTrue( size > 0 );

        long count = 0;
        for( ScanAndScrollRequest it = request; it.hasNext(); ) {
            SearchHit hit = it.next();
            assertTrue( hit instanceof SearchHit);
            count++;
        }
        assertEquals((long) size, count);
    }
}
