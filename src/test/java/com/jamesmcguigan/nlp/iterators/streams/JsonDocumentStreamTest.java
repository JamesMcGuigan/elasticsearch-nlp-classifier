package com.jamesmcguigan.nlp.iterators.streams;

import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import opennlp.tools.doccat.DocumentSample;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonDocumentStreamTest {
    List<String> input = Arrays.asList(
        """
        { "target": 1, "text1": "hello world", "text2": "goodbye world" }
        """,
        """
        { "target": 0, "text1": "pen pineapple", "text2": "apple pen" }
        """
    );

    @Test
    void read() {
        var iterator = input.iterator();
        JsonDocumentStream stream = new JsonDocumentStream(
            iterator,
            Arrays.asList("text1", "text2"),
            "target"
        );
        DocumentSample documentSample1 = stream.read();
        DocumentSample documentSample2 = stream.read();
        DocumentSample documentSample3 = stream.read();

        assertNotNull(documentSample1);
        assertNotNull(documentSample2);
        assertNull(documentSample3);
    }

    @Test
    void setTokenizerDefault() {
        Iterator<String> iterator = input.iterator();
        JsonDocumentStream stream = new JsonDocumentStream(
            iterator,
            Arrays.asList("text1", "text2"),
            "target"
        );

        DocumentSample documentSample1 = stream.read();
        DocumentSample documentSample2 = stream.read();

        assertEquals(documentSample1.getCategory(), "1");
        assertArrayEquals(documentSample1.getText(), new String[]{"hello", "world", "goodby", "world"});

        assertEquals(documentSample2.getCategory(), "0");
        assertArrayEquals(documentSample2.getText(), new String[]{"pen", "pineappl", "appl", "pen"});
    }

    @Test
    void setTokenizerSimple() {
        Iterator<String> iterator = input.iterator();
        NLPTokenizer tokenizer    = new NLPTokenizer().setLowercase(true).setStemming(false);
        JsonDocumentStream stream = new JsonDocumentStream(
            iterator,
            Arrays.asList("text1", "text2"),
            "target"
        ).setTokenizer(tokenizer);
        assertEquals( tokenizer, stream.getTokenizer() );

        DocumentSample documentSample1 = stream.read();
        DocumentSample documentSample2 = stream.read();

        assertEquals(documentSample1.getCategory(), "1");
        assertArrayEquals(documentSample1.getText(), new String[]{"hello", "world", "goodbye", "world"});

        assertEquals(documentSample2.getCategory(), "0");
        assertArrayEquals(documentSample2.getText(), new String[]{"pen", "pineapple", "apple", "pen"});
    }
}
