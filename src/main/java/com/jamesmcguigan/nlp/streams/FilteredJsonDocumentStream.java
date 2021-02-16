package com.jamesmcguigan.nlp.streams;

import com.jamesmcguigan.nlp.data.ESJsonPath;
import opennlp.tools.doccat.DocumentSample;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * This implement JsonDocumentStream, but removes any entries that lack a target category
 */
public class FilteredJsonDocumentStream extends JsonDocumentStream {
    private Predicate<ESJsonPath> predicate;

    public FilteredJsonDocumentStream(Iterator<String> iterator, List<String> fields, String target) {
        super(iterator, fields, target);
    }
    public FilteredJsonDocumentStream(Iterator<String> iterator, List<String> fields, String target, Predicate<ESJsonPath> predicate) {
        super(iterator, fields, target);
        this.predicate = predicate;
    }

    /**
     * Returns the next object. Calling this method repeatedly until it returns
     * null will return each object from the underlying source exactly once.
     *
     * @return the next object or null to signal that the stream is exhausted
     */
    @Override
    public DocumentSample read() {
        while( this.iterator.hasNext() ) {
            String json             = this.iterator.next();
            DocumentSample document = this.cast( json );
            if( document.getCategory() == null || document.getCategory().equals("") ) {
                continue;
            }
            if( this.predicate != null ) {
                ESJsonPath jsonPath = new ESJsonPath(json);
                if( this.predicate.negate().test(jsonPath) ) {
                    continue;
                }
            }
            return document;
        }
        return null;
    }
}
