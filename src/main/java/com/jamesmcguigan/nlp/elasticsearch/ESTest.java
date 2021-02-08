package com.jamesmcguigan.nlp.elasticsearch;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runtime sandbox test demonstrating ElasticSearch API functionality
 */
public class ESTest {

    public static void main( String[] args ) throws IOException {
        var client = new ESClient().client;

        //***** Search *****//

        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = response.getHits().getHits();
        Map<String, Object> first = searchHits[0].getSourceAsMap();

        var searchResults =
            Arrays.stream(searchHits)
                .map(hit -> new JSONObject(hit.getSourceAsString()))
                // .map(hit -> JSON.parseObject(hit.getSourceAsString()))
                // .map(hit -> JSON.parseObject(hit.getSourceAsString(), Person.class))
                .collect(Collectors.toList());


        //***** Raw Request *****//
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-low-usage-requests.html
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-low-usage-responses.html
        Request llRequest = new Request("POST", "/twitter/_mtermvectors");
        llRequest.setJsonEntity("""
        {
           "docs": [
              {
                 "fields" : ["text","keyword"],
                 "_id": "1",
                 "offsets": false,
                 "payloads": false,
                 "positions": false,
                 "term_statistics": false              
              },
              {
                 "fields" : ["text","keyword"],
                 "_id": "2",
                 "offsets": false,
                 "payloads": false,
                 "positions": false,
                 "term_statistics": false                 
              }
           ]
        }
        """);
        Response llResponse = client.getLowLevelClient().performRequest(llRequest);
        RequestLine llRequestLine = llResponse.getRequestLine();
        HttpHost llHost = llResponse.getHost();
        int llStatusCode = llResponse.getStatusLine().getStatusCode();
        Header[] llHeaders = llResponse.getHeaders();
        String llResponseBody = EntityUtils.toString(llResponse.getEntity());
        JSONObject llJson = new JSONObject(llResponseBody);
        JSONArray llDocs   = llJson.getJSONArray("docs");
        HashMap<String, Integer> llTermVectors = new HashMap<>();
        for( int i = 0; i < llDocs.length(); i++ ) {
            JSONObject doc = llDocs.getJSONObject(i);
            for( var field : doc.getJSONObject("term_vectors").keySet() ) {
                var terms = doc.getJSONObject("term_vectors").getJSONObject(field).getJSONObject("terms");
                for( var token : terms.keySet() ) {
                    var current  = terms.getJSONObject(token).getInt("term_freq");
                    var previous = llTermVectors.getOrDefault(token, 0);
                    llTermVectors.put(token, current + previous );
                }
            }
        }
        System.out.print("llTermVectors");
        System.out.print(llTermVectors);


        //***** Term Vector Request *****//

        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-term-vectors.html
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-termvectors.html
        // NOTE: use org.elasticsearch.client.core.TermVectorsRequest not org.elasticsearch.action.termvectors.TermVectorsRequest
        TermVectorsRequest tvRequest = new TermVectorsRequest("twitter", "1614");
        tvRequest.setFields("text");
        tvRequest.setFieldStatistics(true);
        tvRequest.setTermStatistics(true);
//        tvRequest.setPositions(true);
//        tvRequest.setOffsets(true);
//        tvRequest.setPayloads(true);

        TermVectorsResponse tvResponse = client.termvectors(tvRequest, RequestOptions.DEFAULT);
        List<TermVectorsResponse.TermVector> tvSearchHits = tvResponse.getTermVectorsList();
        List<TermVectorsResponse.TermVector.Term> tvTerms = tvSearchHits.get(0).getTerms();
        TermVectorsResponse.TermVector.FieldStatistics tvStats = tvSearchHits.get(0).getFieldStatistics();



        //***** Multi Term Vector Request - BROKEN *****//

//        TermVectorsRequest tvrequestTemplate =
//            new TermVectorsRequest("twitter", first.get("id"));
//        // tvrequestTemplate.setFields("user");
//        String[] ids = {"1", "2"};
//        MultiTermVectorsRequest request =
//            new MultiTermVectorsRequest(ids, tvrequestTemplate);

//
//        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-multi-term-vectors.html
//        TermVectorsRequest mtvRequestTemplate =
//            new TermVectorsRequest("twitter", "");
//        mtvRequestTemplate.setFields("text");
//        mtvRequestTemplate.setFieldStatistics(true);
//        mtvRequestTemplate.setTermStatistics(true);
//        mtvRequestTemplate.setPayloads(true);
//
//
//        Map<String, Integer> filterSettings = new HashMap<>();
//        filterSettings.put("max_num_terms", 3);
//        filterSettings.put("min_term_freq", 1);
//        filterSettings.put("max_term_freq", 10);
//        filterSettings.put("min_doc_freq", 1);
//        filterSettings.put("max_doc_freq", 100);
//        filterSettings.put("min_word_length", 1);
//        filterSettings.put("max_word_length", 10);
//
//        mtvRequestTemplate.setFilterSettings(filterSettings);
//
//        String[] ids = {"1614"};
//        MultiTermVectorsRequest mtvRequest = new MultiTermVectorsRequest(ids, mtvRequestTemplate);
//
////        MultiTermVectorsRequest mtvRequest = new MultiTermVectorsRequest();
////        mtvRequest.add(tvRequest);
//
//        Optional<ValidationException> mtvError = mtvRequest.validate();
//        MultiTermVectorsResponse mtvResponse = client.mtermvectors(mtvRequest, RequestOptions.DEFAULT);
//        List<TermVectorsResponse> mtvResults = mtvResponse.getTermVectorsResponses();
//        List<TermVectorsResponse.TermVector> mtvVectors = mtvResults.get(0).getTermVectorsList();
//        TermVectorsResponse.TermVector.FieldStatistics mtvStats = mtvVectors.get(0).getFieldStatistics();

        client.close();
    }
}
