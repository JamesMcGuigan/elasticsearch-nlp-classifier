package com.jamesmcguigan.nlp.data;

import com.jamesmcguigan.nlp.elasticsearch.read.TermVectorQuery;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

class TermVectorTokensTest {

    protected final String responseJson = """
    {
      "docs": [
        {
          "_index": "twitter", 
          "_type": "_doc", 
          "_id": "1", 
          "_version": 2, 
          "found": true, 
          "took": 0,
          "term_vectors": {
            "text": {
              "field_statistics": {"sum_doc_freq": 151328, "doc_count": 10000, "sum_ttf": 160554},
              "terms": {
                "all":        {"doc_freq": 326,  "ttf": 340,  "term_freq": 1, "tokens": [{"position": 12, "start_offset": 66, "end_offset": 69}]},
                "allah":      {"doc_freq": 11,   "ttf": 11,   "term_freq": 1, "tokens": [{"position": 9,  "start_offset": 49, "end_offset": 54}]},
                "are":        {"doc_freq": 529,  "ttf": 562,  "term_freq": 1, "tokens": [{"position": 2,  "start_offset": 10, "end_offset": 13}]},
                "deeds":      {"doc_freq": 2,    "ttf": 2,    "term_freq": 1, "tokens": [{"position": 1,  "start_offset": 4,  "end_offset": 9}]},
                "earthquake": {"doc_freq": 63,   "ttf": 71,   "term_freq": 1, "tokens": [{"position": 7,  "start_offset": 34, "end_offset": 44}]},
                "forgive":    {"doc_freq": 4,    "ttf": 4,    "term_freq": 1, "tokens": [{"position": 10, "start_offset": 55, "end_offset": 62}]},
                "may":        {"doc_freq": 98,   "ttf": 100,  "term_freq": 1, "tokens": [{"position": 8,  "start_offset": 45, "end_offset": 48}]},
                "of":         {"doc_freq": 2159, "ttf": 2392, "term_freq": 1, "tokens": [{"position": 5,  "start_offset": 25, "end_offset": 27}]},
                "our":        {"doc_freq": 135,  "ttf": 141,  "term_freq": 1, "tokens": [{"position": 0,  "start_offset": 0,  "end_offset": 3}]},
                "reason":     {"doc_freq": 27,   "ttf": 27,   "term_freq": 1, "tokens": [{"position": 4,  "start_offset": 18, "end_offset": 24}]},
                "the":        {"doc_freq": 3144, "ttf": 4231, "term_freq": 1, "tokens": [{"position": 3,  "start_offset": 14, "end_offset": 17}]},
                "this":       {"doc_freq": 589,  "ttf": 614,  "term_freq": 1, "tokens": [{"position": 6,  "start_offset": 28, "end_offset": 32}]},
                "us":         {"doc_freq": 151,  "ttf": 159,  "term_freq": 1, "tokens": [{"position": 11, "start_offset": 63, "end_offset": 65}]}
              }
            },
            "keyword": {
              "field_statistics": {"sum_doc_freq": 10000, "doc_count": 10000, "sum_ttf": 10000},
              "terms": {
                "": {"doc_freq": 84, "ttf": 84, "term_freq": 1, "tokens": [{"position": 0, "start_offset": 0, "end_offset": 0}]}
              }
            }
          }
        }
      ]
    }
    """;
    private TermVectorQuery termVectorQuery;
    private List<TermVectorsResponse> response;
    private TermVectorTokens termVectorTokens;

    @BeforeEach
    void setUp() throws IOException {
        termVectorQuery  = new TermVectorQuery("twitter");
        response         = termVectorQuery.castTermVectorsResponse(responseJson);
        termVectorTokens = new TermVectorTokens(response.get(0));
    }

    @Test
    void getId() {
        assertThat( termVectorTokens.getId() ).isEqualTo( "1" );
    }

    @Test
    void getFields() {
        assertThat( termVectorTokens.getFields() ).containsExactly( "text", "keyword" );
    }

    @Test
    void tokenize() {
        String[] tokens   = termVectorTokens.tokenize();
        String[] expected = new String[]{ "", "all", "allah", "are", "deeds", "earthquake", "forgive", "may", "of", "our", "reason", "the", "this", "us" };
        assertThat(tokens).isEqualTo(expected);

    }

    @Test
    void tokenizeField() {
        String[] tokensText = termVectorTokens.tokenize("text");
        String[] expected   = new String[]{ "all", "allah", "are", "deeds", "earthquake", "forgive", "may", "of", "our", "reason", "the", "this", "us" };
        assertThat(tokensText).isEqualTo(expected);

        String[] tokensKeyword = termVectorTokens.tokenize("keyword");
        assertThat(tokensKeyword).isEqualTo(new String[]{ "" });
    }
}
