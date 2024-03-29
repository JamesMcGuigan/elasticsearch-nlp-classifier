package com.jamesmcguigan.nlp.utils.iterators.streams;

import com.jamesmcguigan.nlp.utils.tokenize.ATokenizer;
import com.jamesmcguigan.nlp.utils.tokenize.NLPTokenizer;
import opennlp.tools.doccat.DocumentSample;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonDocumentStreamTest {
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

        assertEquals("1", documentSample1.getCategory());
        assertArrayEquals(new String[]{"hello", "world", "goodby", "world"}, documentSample1.getText());

        assertEquals("0", documentSample2.getCategory());
        assertArrayEquals(new String[]{"pen", "pineappl", "appl", "pen"}, documentSample2.getText());
    }

    @Test
    void setTokenizerSimple() {
        Iterator<String> iterator = input.iterator();
        ATokenizer tokenizer      = new NLPTokenizer().setLowercase(true).setStemming(false);
        JsonDocumentStream stream = new JsonDocumentStream(
            iterator,
            Arrays.asList("text1", "text2"),
            "target"
        ).setTokenizer(tokenizer);
        assertEquals( tokenizer, stream.getTokenizer() );

        DocumentSample documentSample1 = stream.read();
        DocumentSample documentSample2 = stream.read();

        assertEquals("1", documentSample1.getCategory());
        assertArrayEquals(new String[]{"hello", "world", "goodbye", "world"}, documentSample1.getText());

        assertEquals("0", documentSample2.getCategory());
        assertArrayEquals(new String[]{"pen", "pineapple", "apple", "pen"}, documentSample2.getText());
    }
}
