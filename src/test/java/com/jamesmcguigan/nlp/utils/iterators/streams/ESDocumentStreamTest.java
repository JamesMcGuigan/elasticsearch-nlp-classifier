package com.jamesmcguigan.nlp.utils.iterators.streams;

import com.jamesmcguigan.nlp.utils.tokenize.ATokenizer;
import com.jamesmcguigan.nlp.utils.tokenize.NLPTokenizer;
import opennlp.tools.doccat.DocumentSample;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.jupiter.api.Assertions.*;

class ESDocumentStreamTest {

    @Test
    void getTotalHits() {
        ESDocumentStream stream = new ESDocumentStream("twitter", "text", "target", null);
        assertNotNull( stream.getTotalHits() );
        assertTrue(stream.getTotalHits() > 1000 );
    }

    @Test
    void size() {
        ESDocumentStream stream = new ESDocumentStream("twitter", "text", "target", null);
        assertNotNull( stream.size() );
        assertTrue(stream.size() > 1000 );
    }

    @Test
    void reset() {
        ESDocumentStream stream = new ESDocumentStream("twitter", "text", "target", null);
        Long size = stream.size();
        DocumentSample document = stream.read();

        assertNotNull(document);
        assertEquals(stream.size(), size-1 );

        stream.reset();
        assertEquals(stream.size(), size );
    }

    @Test
    void read() {
        String target = "1";
        String term   = "disaster";

        ATokenizer tokenizer = new NLPTokenizer().setLowercase(true).setStemming(false);
        BoolQueryBuilder query = boolQuery()
            .must(matchQuery("target", target))
            .must(termQuery("text", term));

        ESDocumentStream stream = new ESDocumentStream("twitter", "text", "target", query)
            .setTokenizer(tokenizer);

        DocumentSample document = stream.read();
        String category = document.getCategory();
        String[] tokens = document.getText();

        assertEquals( category, target);
        assertTrue( tokens.length > 1 );
        assertTrue( Arrays.asList(tokens).contains(term) );
    }
}
