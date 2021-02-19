package com.jamesmcguigan.nlp.elasticsearch.read;

import com.jayway.jsonpath.JsonPath;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.CartesianProductTest;
import org.junitpioneer.jupiter.CartesianValueSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;


class TermVectorQueryTest {

    private TermVectorQuery termVectorQuery;
    private final String       index  = "twitter";
    private final List<String> ids    = Arrays.asList("1", "2", "9961");
    private final List<String> fields = Arrays.asList("text", "location", "keyword");

    @BeforeEach
    void setUp() {
        this.termVectorQuery = new TermVectorQuery(this.index, this.fields);
    }

    @Test
    void getMultiTermVectors() throws IOException {
        List<TermVectorsResponse> responses = this.termVectorQuery.getMultiTermVectors(this.ids);
        var responseIds = responses.stream()
            .map(TermVectorsResponse::getId)
            .collect(Collectors.toList())
        ;
        var responseFields = responses.stream()
            .map(TermVectorsResponse::getTermVectorsList)
            .flatMap(Collection::stream)
            .map(TermVectorsResponse.TermVector::getFieldName)
            .collect(Collectors.toList())
        ;
        assertThat(responses.size()).isEqualTo(this.ids.size());
        assertThat(responseIds).containsExactlyElementsIn(this.ids);

        // NOTE: ids += [9961] required to pass this test
        //       ids == [1,2] -> responseFields == ["text", "keywords"]
        //       database [1,2] have blank { "location": "" }
        // schema.json
        //  "text":     { "type": "text", fielddata: true },
        //  "location": { "type": "text", fielddata: true, "fields": { "keyword": { "type":  "keyword" } } },
        //  "keyword":  { "type": "keyword" },
        assertThat(Set.copyOf(responseFields)).containsExactlyElementsIn(this.fields);
        assertThat(responseFields)
            .containsExactlyElementsIn(Arrays.asList(
                "keyword", "text",             // id = 1
                "keyword", "text",             // id = 2
                "keyword", "location", "text"  // id = 9961
            ))
            .inOrder()
        ;
    }


    @CartesianProductTest
    @CartesianValueSource(booleans = { true, false })
    @CartesianValueSource(booleans = { true, false })
    @CartesianValueSource(booleans = { true, false })
    @CartesianValueSource(booleans = { true, false })
    void getMultiTermVectorRequestJson(boolean offset, boolean payload, boolean position, boolean termStatistic) {
        this.termVectorQuery.setOffsets(offset);
        this.termVectorQuery.setPayloads(payload);
        this.termVectorQuery.setPositions(position);
        this.termVectorQuery.setTermStatistics(termStatistic);

        String requestJson = this.termVectorQuery.getMultiTermVectorsRequestJson(ids);
        List<String>  ids       = JsonPath.read(requestJson, "docs[*]._id");
        List<String>  fields    = JsonPath.read(requestJson, "docs[*].fields[*]");
        List<Boolean> offsets   = JsonPath.read(requestJson, "docs[*].offsets");
        List<Boolean> payloads  = JsonPath.read(requestJson, "docs[*].payloads");
        List<Boolean> positions = JsonPath.read(requestJson, "docs[*].positions");
        List<Boolean> termStats = JsonPath.read(requestJson, "docs[*].term_statistics");

        assertThat(ids).containsExactlyElementsIn(this.ids);
        assertThat(Set.copyOf(fields)).containsExactlyElementsIn(this.fields);
        assertThat(Set.copyOf(offsets)).containsExactly(offset);
        assertThat(Set.copyOf(payloads)).containsExactly(payload);
        assertThat(Set.copyOf(positions)).containsExactly(position);
        assertThat(Set.copyOf(termStats)).containsExactly(termStatistic);
    }


    @Test
    void getMultiTermVectorRequest() throws IOException {
        String  requestJson = this.termVectorQuery.getMultiTermVectorsRequestJson(ids);
        Request request     = this.termVectorQuery.getMultiTermVectorsRequest(requestJson);
        String  content     = new BufferedReader(new InputStreamReader(
            request.getEntity().getContent()
        )).lines().collect(Collectors.joining("\n"));

        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getEndpoint()).startsWith("/"+this.index+"/_mtermvectors");
        assertThat(content).isEqualTo(requestJson);
    }

    @Test
    void getMultiTermVectorRequestJson() throws IOException {
        String requestJson  = this.termVectorQuery.getMultiTermVectorsRequestJson(ids);
        String responseJson = this.termVectorQuery.getMultiTermVectorsResponseJson(requestJson);
        List<String>  ids   = JsonPath.read(responseJson, "docs[*]._id");
        assertThat(ids).containsExactlyElementsIn(this.ids);
    }

    @Test
    void castTermVectorsResponse() throws IOException {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-termvectors.html
        String responseJson = """
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
        List<TermVectorsResponse> responses = this.termVectorQuery.castTermVectorsResponse(responseJson);
        List<String> terms = responses.get(0).getTermVectorsList().stream()
            .map(TermVectorsResponse.TermVector::getFieldName)
            .collect(Collectors.toList())
        ;
        List<String> tokens = responses.get(0).getTermVectorsList().stream()
            .map(TermVectorsResponse.TermVector::getTerms)
            .flatMap(Collection::stream)
            .map(TermVectorsResponse.TermVector.Term::getTerm)
            .collect(Collectors.toList())
        ;
        assertThat(responses).isNotNull();
        assertThat(responses.size()).isEqualTo(1);
        assertThat(responses.get(0).getId()).isEqualTo("1");
        assertThat(responses.get(0).getIndex()).isEqualTo("twitter");
        assertThat(terms).containsExactly("text", "keyword");
        assertThat(tokens).containsExactly("", "all", "allah", "are", "deeds", "earthquake", "forgive", "may", "of", "our", "reason", "the", "this", "us");
    }
}
