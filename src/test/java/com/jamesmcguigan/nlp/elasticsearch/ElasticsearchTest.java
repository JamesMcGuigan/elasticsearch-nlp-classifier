package com.jamesmcguigan.nlp.elasticsearch;

import com.github.underscore.lodash.U;
import com.google.gson.Gson;
import com.jamesmcguigan.nlp.data.ESJsonPath;
import com.jamesmcguigan.nlp.data.Tweet;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.elasticsearch.index.query.QueryBuilders.*;



public class ElasticsearchTest {
    ESClient client  = ESClient.getInstance();
    String index     = "twitter";
    List<String> ids = Arrays.asList("1", "2", "3", "4");
    String[] fields  = Arrays.asList("text", "location").toArray(new String[0]);

    public ElasticsearchTest() throws IOException {}


    @Test
    void searchRequest() throws IOException {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search.html
        SearchRequest searchRequest = new SearchRequest("twitter");
        searchRequest.source(new SearchSourceBuilder()
            .query(boolQuery()
                .must(matchQuery("target", "1"))
                .must(termQuery("text", "disaster"))
            )
            .from(0)
            .size(5)
            .timeout(new TimeValue(60, TimeUnit.SECONDS))
        );
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits  = response.getHits().getHits();


        assertThat(searchHits.length).isEqualTo(5);
        for( SearchHit searchHit : searchHits ) {
            Map<String, Object> source = searchHit.getSourceAsMap();

            assertThat(source.get("id")    ).isInstanceOf(Integer.class);
            assertThat(source.get("target")).isInstanceOf(Integer.class);
            assertThat(source.get("text")  ).isInstanceOf(String.class);

            assertThat((Integer) source.get("id")    ).isGreaterThan(0);
            assertThat((Integer) source.get("target")).isEqualTo(1);
            assertThat((String)  source.get("text")  ).contains("disaster");
        }


        List<ESJsonPath> jsonPaths = Arrays.stream(searchHits)
            .map(hit -> new ESJsonPath(hit.getSourceAsString()))
            .collect(Collectors.toList())
        ;
        for( ESJsonPath jsonPath : jsonPaths ) {
            assertThat(jsonPath.get("id")    ).isInstanceOf(String.class);
            assertThat(jsonPath.get("target")).isInstanceOf(String.class);
            assertThat(jsonPath.get("text")  ).isInstanceOf(String.class);

            assertThat(jsonPath.get("target")).isEqualTo("1");
            assertThat(jsonPath.get("text")  ).contains("disaster");
        }


        List<Tweet> tweets = Arrays.stream(searchHits)
            // .map(hit -> new JSONObject(hit.getSourceAsString()))
            .map(hit -> new Gson().fromJson(hit.getSourceAsString(), Tweet.class))
            .collect(Collectors.toList())
        ;
        for( Tweet tweet : tweets ) {
            assertThat(tweet.target).isEqualTo("1");
            assertThat(tweet.text).contains("disaster");
        }
    }


    @Test
    void termVector() throws IOException {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-term-vectors.html
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-termvectors.html
        // NOTE: use org.elasticsearch.client.core.TermVectorsRequest not org.elasticsearch.action.termvectors.TermVectorsRequest

        TermVectorsRequest request = new TermVectorsRequest("twitter", "1614");
        request.setFields("text");
        request.setFieldStatistics(true);
        request.setTermStatistics(true);
        request.setPositions(false);
        request.setOffsets(false);
        request.setPayloads(false);

        TermVectorsResponse response                         = client.termvectors(request, RequestOptions.DEFAULT);
        List<TermVectorsResponse.TermVector> searchHits      = response.getTermVectorsList();
        List<TermVectorsResponse.TermVector.Term> terms      = searchHits.get(0).getTerms();
        TermVectorsResponse.TermVector.FieldStatistics stats = searchHits.get(0).getFieldStatistics();

        Map<String, Integer> termVectors = terms.stream().collect(Collectors.toMap(
            TermVectorsResponse.TermVector.Term::getTerm,
            TermVectorsResponse.TermVector.Term::getTermFreq
        ));
        Map<String, Long> termTTF = terms.stream().collect(Collectors.toMap(
            TermVectorsResponse.TermVector.Term::getTerm,
            TermVectorsResponse.TermVector.Term::getTotalTermFreq
        ));
        Map<String, Integer> termDocFreq = terms.stream().collect(Collectors.toMap(
            TermVectorsResponse.TermVector.Term::getTerm,
            TermVectorsResponse.TermVector.Term::getDocFreq
        ));

        assertThat( termVectors.keySet() ).isEqualTo( termTTF.keySet() );
        assertThat( termVectors.keySet() ).isEqualTo( termDocFreq.keySet() );
        for( String token : termVectors.keySet() ) {
            assertThat( termVectors.get(token) ).isAtLeast(1);
            assertThat( termDocFreq.get(token) ).isAtLeast(termVectors.get(token));
            assertThat( termTTF.get(token)     ).isAtLeast(termDocFreq.get(token));
        }
        assertThat( stats.getDocCount()         ).isAtLeast( 1 );
        assertThat( stats.getSumDocFreq()       ).isAtLeast( 1 );
        assertThat( stats.getSumTotalTermFreq() ).isAtLeast( 1 );
    }


    @Ignore
    //@Test
    void brokenMultiTermVector1() throws IOException {
        // This is the version taken directly from the ElasticSearch docs
        // Bonsai - BROKEN: URI [_mtermvectors], status line [HTTP/1.1 400 Bad Request
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-multi-term-vectors.html

        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        TermVectorsRequest termRequest = new TermVectorsRequest(index, ids.get(0));
        termRequest.setFields("text", "location");
        request.add(termRequest);

        for( String id : ids.subList(1,ids.size()) ) {
            XContentBuilder docBuilder = XContentFactory.jsonBuilder();
            docBuilder.startObject().field("user", "guest-user").endObject();
            TermVectorsRequest termRequest2 = new TermVectorsRequest("authors", docBuilder);
            request.add(termRequest2);
        }
        MultiTermVectorsResponse response       = client.mtermvectors(request, RequestOptions.DEFAULT);
        List<TermVectorsResponse> termResponses = response.getTermVectorsResponses();

        for( TermVectorsResponse termResponse : termResponses ) {
            List<TermVectorsResponse.TermVector> termVectors = termResponse.getTermVectorsList();
            assertThat(termVectors).isNotNull();
            assertThat(termVectors).isNotEmpty();
        }
    }


    @Ignore
    //@Test
    void brokenMultiTermVector2() {
        // This is the version taken directly from a github example
        // Bonsai - BROKEN: URI [_mtermvectors], status line [HTTP/1.1 400 Bad Request
        // DOCS: https://github.com/guanjunlinger/es-study/blob/65a7bf49c9191f6d37151ee72e2678b46d1e96fd/src/main/java/com/study/service/impl/BulkDocumentServiceImpl.java

        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        for( String id : ids ) {
            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(index, id);
            termVectorsRequest.setFields(fields);
            multiTermVectorsRequest.add(termVectorsRequest);
        }
        try {
            MultiTermVectorsResponse multiTermVectorsResponse = client.mtermvectors(multiTermVectorsRequest, RequestOptions.DEFAULT);
            for( TermVectorsResponse termVectorsResponse : multiTermVectorsResponse.getTermVectorsResponses() ) {
                for( TermVectorsResponse.TermVector termVector : termVectorsResponse.getTermVectorsList() ) {
                    String id = termVectorsResponse.getId();
                    List<TermVectorsResponse.TermVector.Term> terms = termVector.getTerms();
                    assertThat(id).isNotNull();
                    assertThat(terms).isNotNull();
                    assertThat(terms).isNotEmpty();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void multiTermVectorRawJSON() throws IOException {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-low-usage-requests.html
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-low-usage-responses.html
        Request request = new Request("POST", "/"+index+"/_mtermvectors");
        request.setJsonEntity("""
        {
           "docs": [
              {
                 "fields" : ["text","keyword"],
                 "_id": "1",
                 "offsets":   false,
                 "payloads":  false,
                 "positions": false,
                 "term_statistics": true              
              },
              {
                 "fields" : ["text","keyword"],
                 "_id": "2",
                 "offsets":   false,
                 "payloads":  false,
                 "positions": false,
                 "term_statistics": true                 
              }
           ]
        }
        """);
        Response response = client.getLowLevelClient().performRequest(request);
        RequestLine requestLine = response.getRequestLine();
        HttpHost host           = response.getHost();
        int statusCode          = response.getStatusLine().getStatusCode();
        Header[] headers        = response.getHeaders();
        String responseBody     = EntityUtils.toString(response.getEntity());

        //// NOTE: This doesn't work!
        // TermVectorsResponse.fromXContent(response.getEntity());
        // TermVectorsResponse termResponse = new Gson().fromJson(responseBody, TermVectorsResponse.class);

        assertThat(requestLine.toString()).isEqualTo("POST /twitter/_mtermvectors HTTP/1.1");
        assertThat(host.getSchemeName()).isEqualTo("https");
        assertThat(host.toString()).startsWith("https://");
        assertThat(statusCode).isEqualTo(200);

        HashMap<String, Integer> termVectors = new HashMap<>();             // termVectors[field][token] = count
        HashMap<String, Integer> termTTF     = new HashMap<>();             // termVectors[field][token] = count
        HashMap<String, Integer> termDocFreq = new HashMap<>();             // termVectors[field][token] = count
        HashMap<Integer, Map<String, Integer>> docStats = new HashMap<>();  // docStats[id][statName]   = count

        JSONArray docs = new JSONObject(responseBody).getJSONArray("docs");
        for( int i = 0; i < docs.length(); i++ ) {
            JSONObject doc = docs.getJSONObject(i);
            JSONObject term_vectors = doc.getJSONObject("term_vectors");
            for( var field : term_vectors.keySet() ) {
                JSONObject terms = term_vectors.getJSONObject(field).getJSONObject("terms");
                for( var token : terms.keySet() ) {
                    int current  = terms.getJSONObject(token).getInt("term_freq");
                    int previous = termVectors.getOrDefault(token, 0);
                    termVectors.put(token, current + previous);
                }
                for( var token : terms.keySet() ) {
                    int current  = terms.getJSONObject(token).getInt("ttf");
                    int previous = termVectors.getOrDefault(token, 0);
                    termTTF.put(token, current + previous);
                }
                for( var token : terms.keySet() ) {
                    int current  = terms.getJSONObject(token).getInt("doc_freq");
                    int previous = termVectors.getOrDefault(token, 0);
                    termDocFreq.put(token, current + previous);
                }


                JSONObject stats = term_vectors.getJSONObject(field).getJSONObject("field_statistics");
                for( var statName : stats.keySet() ) {
                    Integer id = doc.getInt("_id");
                    docStats.computeIfAbsent(id, k -> new HashMap<>());
                    docStats.get(id).put(statName, stats.getInt(statName));
                }
            }
        }
        assertThat(termVectors.size()).isGreaterThan(2);
        assertThat(termVectors.keySet()).isEqualTo(termTTF.keySet());
        assertThat(termVectors.keySet()).isEqualTo(termDocFreq.keySet());

        for( int id : docStats.keySet() ) {
            assertThat(docStats.get(id).keySet()).containsExactly("sum_doc_freq", "doc_count", "sum_ttf");
        }
    }


    @Test
    void multiTermVectorUnderscore() throws IOException {
        // WORKAROUND: client.mtermvectors(new MultiTermVectorsRequest()) on Bonsai throws 400 Bad Request

        // DOCS: https://github.com/javadev/underscore-java
        String requestJson = U.objectBuilder()
            .add("docs", U.reduce(ids, (arrayBuilder, id) -> arrayBuilder.add(U.objectBuilder()
                .add("_id", id)
                .add("fields", fields)
                .add("term_statistics", true)
                .add("offsets",   true)
                .add("payloads",  true)
                .add("positions", true)
            ), U.arrayBuilder())
        ).toJson();

        Request request = new Request("POST", "/"+index+"/_mtermvectors");
        request.setJsonEntity(requestJson);

        Response response   = client.getLowLevelClient().performRequest(request);
        String responseJson = EntityUtils.toString(response.getEntity());

        HashMap<String, Integer> termVectors = new HashMap<>();             // termVectors[token] = count
        HashMap<String, Integer> termTTF     = new HashMap<>();             // termVectors[token] = count
        HashMap<String, Long>    termDocFreq = new HashMap<>();             // termVectors[token] = count
        HashMap<String, Map<String, Integer>> docStats = new HashMap<>();   // docStats[id][statName]   = count

        try(
            // Manually generate MultiTermVectorsResponse() from JSON to reuse ES object model
            XContentParser xContentParser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseJson
            )
        ) {
            // Now we have somewhat clean looking code!
            List<TermVectorsResponse> termVectorsResponses = MultiTermVectorsResponse
                .fromXContent(xContentParser)
                .getTermVectorsResponses()
            ;
            for( TermVectorsResponse termVectorsResponse : termVectorsResponses ) {
                String id = termVectorsResponse.getId();
                if( !docStats.containsKey(id) ) { docStats.put(id, new HashMap<>()); }
                for( TermVectorsResponse.TermVector termVectorItem : termVectorsResponse.getTermVectorsList() ) {
                    String fieldName      = termVectorItem.getFieldName();
                    int  docCount         = termVectorItem.getFieldStatistics().getDocCount();
                    long sumDocFreq       = termVectorItem.getFieldStatistics().getSumDocFreq();
                    long sumTotalTermFreq = termVectorItem.getFieldStatistics().getSumTotalTermFreq();

                    assertThat(fieldName).isIn(Arrays.asList(this.fields));
                    assertThat(docCount).isAtLeast(1);
                    assertThat(sumDocFreq).isAtLeast(docCount);
                    assertThat(sumTotalTermFreq).isAtLeast(sumTotalTermFreq);

                    for( TermVectorsResponse.TermVector.Term term : termVectorItem.getTerms() ) {
                        String  termName      = term.getTerm();
                        int     termFreq      = term.getTermFreq();
                        int     docFreq       = term.getDocFreq();
                        long    totalTermFreq = term.getTotalTermFreq();

                        termVectors.put(termName, termVectors.getOrDefault(termName, 0)  + termFreq);
                        termTTF.put(    termName, termTTF.getOrDefault(    termName, 0)  + docFreq);
                        termDocFreq.put(termName, termDocFreq.getOrDefault(termName, 0L) + totalTermFreq);
                    }
                }
            }
        }
        assertThat(termVectors.size()).isGreaterThan(2);
        assertThat(termVectors.keySet()).isEqualTo(termTTF.keySet());
        assertThat(termVectors.keySet()).isEqualTo(termDocFreq.keySet());
    }
}
