package com.jamesmcguigan.nlp.elasticsearch.read;

import com.jamesmcguigan.nlp.data.TermVectorDocTokens;
import com.jamesmcguigan.nlp.data.TermVectorTokens;
import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;

class TermVectorIteratorTest {

    private final String           index  = "twitter";
    private final List<String>     fields = Arrays.asList("text", "keyword");
    private final String           term   = "disaster";
    private final TermQueryBuilder query  = new TermQueryBuilder("text", term);
    private TermVectorIterator<TermVectorsResponse> iterator;

    @BeforeEach
    void setUp() throws IOException {
        ESClient.getInstance();  // refresh connection if closed
        iterator = new TermVectorIterator<>(TermVectorsResponse.class, index, fields, query);
    }

    @Test
    void reset() {
        List<String> first  = new ArrayList<>();
        List<String> second = new ArrayList<>();
        Long totalHits = iterator.getTotalHits();

        // NOTE: TermVectorsResponse is not directly comparable
        assertThat( iterator.size() ).isEqualTo( totalHits );
        while( iterator.hasNext() ) { first.add(iterator.next().getId()); }
        assertThat( iterator.size() ).isEqualTo( 0 );

        iterator.reset();

        assertThat( iterator.size() ).isEqualTo( totalHits );
        while( iterator.hasNext() ) { second.add(iterator.next().getId()); }
        assertThat( iterator.size() ).isEqualTo( 0 );

        assertThat( first.size() ).isEqualTo( second.size() );
        assertThat( first ).containsExactlyElementsIn( second );
    }

    @Test
    void sizeTotalHits() {
        Long totalHits = iterator.getTotalHits();
        assertThat( iterator.size() ).isEqualTo( totalHits );
        for( int pos = 0; pos < totalHits; pos++ ) {
            assertThat( iterator.size() ).isEqualTo( totalHits - pos );
            assertThat( iterator.getTotalHits() ).isEqualTo( totalHits );
            iterator.next();
        }
        assertThat( iterator.size() ).isEqualTo( 0 );
        assertThat( iterator.getTotalHits() ).isEqualTo( totalHits );
    }

    @Test
    void hasMoreRequests() {
        assertThat( iterator.hasMoreRequests() ).isTrue();
        while( iterator.hasNext() ) { iterator.next(); }
        assertThat( iterator.hasMoreRequests() ).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10, 100, 1000 })
    void getSetBufferSize(int input) {
        iterator.setRequestSize(input);
        int output = iterator.getRequestSize();
        assertThat( output ).isEqualTo( input );
    }

    @ParameterizedTest
    @ValueSource(longs = { 1, 10, 100, 1000 })
    void getSetTTL(long input) {
        iterator.setTTL(input);
        long output = iterator.getTTL();
        assertThat( output ).isEqualTo( input );
    }

    @Test
    void getScanAndScrollIds() {
        List<String> ids  = iterator.getScanAndScrollIds();
        long expectedSize = Math.min( iterator.getTotalHits(), iterator.getRequestSize() );

        assertThat( Set.copyOf(ids) ).containsExactlyElementsIn(ids);
        assertThat( ids.size()      ).isEqualTo( expectedSize );
    }

    @Test
    void fetch() throws IOException {
        List<TermVectorsResponse> buffer = iterator.fetch();
        assertThat( buffer.size() ).isEqualTo( iterator.getRequestSize() );
    }

    @ParameterizedTest
    @ValueSource(classes = {
        TermVectorsResponse.class,
        TermVectorDocTokens.class,
        TermVectorTokens.class,
        String[].class
    })
    void cast(Class<?> type) throws IOException {
        var typedIterator = new TermVectorIterator<>(type, index, fields, query);
        var output = typedIterator.next();
        assertThat( output ).isNotNull();
        assertThat( type ).isEqualTo( output.getClass() );
    }
}
