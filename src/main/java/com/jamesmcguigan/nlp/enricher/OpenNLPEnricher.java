package com.jamesmcguigan.nlp.enricher;

import com.jamesmcguigan.nlp.classifier.OpenNLPClassifierES;
import com.jamesmcguigan.nlp.data.ESJsonPath;
import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.elasticsearch.actions.ScanAndScrollRequest;
import com.jamesmcguigan.nlp.streams.ESDocumentStream;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;

/**
 *  OpenNLPEnricher streams from ElasticSearch via `ESDocumentStream()`
 *  trains on data extracted via `target` field,
 *  predicts using `OpenNLPClassifierES()`
 *  updates ElasticSearch with new prediction metadata
 */
@SuppressWarnings("unchecked")
public class OpenNLPEnricher {

    private final String       index;
    private final List<String> fields;
    private final String       target;
    private final String       prefix;

    private final int minRequestsInFlight = 5;
    private int maxRequestsInFlight = 25;
    private int requestsInFlight    = 0;

    private double accuracy = 0.0;
    private final OpenNLPClassifierES classifier = new OpenNLPClassifierES();


    public OpenNLPEnricher(String index, List<String> fields, String target) { this(index, fields, target, null); }
    public OpenNLPEnricher(String index, List<String> fields, String target, @Nullable String prefix) {
        this.index  = index;
        this.fields = fields;
        this.target = target;
        this.prefix = (prefix != null) ? prefix : getClass().getSimpleName();
    }
    public <T extends OpenNLPEnricher> T load(Path filepath) throws IOException { this.classifier.load(filepath); return (T) this; }
    public <T extends OpenNLPEnricher> T save(Path filepath) throws IOException { this.classifier.save(filepath); return (T) this; }
    public double getAccuracy() { return this.accuracy; }


    public <T extends OpenNLPEnricher> T train() throws IOException { return train(null); }
    public <T extends OpenNLPEnricher> T train(@Nullable QueryBuilder query) throws IOException {
        BoolQueryBuilder streamQuery = new BoolQueryBuilder();
        streamQuery.must(existsQuery(target));
        if( query != null ) { streamQuery.must(query); }

        try (
            ESDocumentStream stream = new ESDocumentStream(
                index, fields, target, streamQuery
            ).setTokenizer(classifier.tokenizer)
        ) {
            classifier.train(stream);
        }
        return (T) this;
    }


    public <T extends OpenNLPEnricher> T enrich() throws IOException { return enrich(null); }
    public <T extends OpenNLPEnricher> T enrich(@Nullable QueryBuilder query) throws IOException {
        ScanAndScrollRequest<String> request = new ScanAndScrollRequest<>(index, query, String.class);

        int correct = 0;
        int count   = 0;
        while( request.hasNext() ) {
            String json       = request.next();
            var jsonPath      = new ESJsonPath(json);
            String id         = jsonPath.get("id");
            String category   = jsonPath.get(this.target);
            String[] tokens   = jsonPath.tokenize(this.fields).toArray(new String[0]);
            String prediction = this.classifier.predict(tokens);

            if( this.isUpdateRequired(jsonPath, target, prediction) ) {
                this.asyncUpdate(id, this.target, prediction);
            }

            if( !category.isEmpty() ) {
                correct += category.equals(prediction) ? 1 : 0;
                count   += 1;
            }
        }
        this.accuracy = (count > 0) ? (double) correct / count : 0.0;
        return (T) this;
    }
    private String getPredictionPath(String target) {
        return this.prefix.isEmpty() ? target : this.prefix+'.'+target;
    }
    private boolean isUpdateRequired(ESJsonPath jsonPath, String target, String prediction) {
        String path     = this.getPredictionPath(target);
        String existing = jsonPath.get(path);
        return !prediction.equals(existing);
    }


    @SuppressWarnings("BusyWait")
    private void waitForQueue() {
        while( this.requestsInFlight > Math.max(this.maxRequestsInFlight, this.minRequestsInFlight) ) {
            try {
                Thread.sleep(10);
            } catch( InterruptedException e ) {
                Thread.currentThread().interrupt();
            }
        }
    }
    private UpdateRequest getUpdateRequest(String id, String target, String value) {
        Map<String, Object> jsonMap   = new HashMap<>();
        Map<String, Object> targetMap = new HashMap<>();
        jsonMap.put(this.prefix, targetMap);
        targetMap.put(target, value);
        if( this.prefix.isEmpty() ) {
            jsonMap = targetMap;
        }
        UpdateRequest request = new UpdateRequest(this.index, id).doc(jsonMap);
        return request;
    }
    private void asyncUpdate(String id, String target, String value) throws IOException {
        // TODO: implement bulk update
        this.waitForQueue();

        this.requestsInFlight += 1;
        UpdateRequest request = this.getUpdateRequest(id, target, value);
        ESClient.getInstance().updateAsync(
            request,
            RequestOptions.DEFAULT,
            new ActionListener<>() {
                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    OpenNLPEnricher.this.requestsInFlight -= 1;

                    var result = updateResponse.getResult();
                    System.out.printf("%s %s(%s) | %s.%s = %s%n",
                        result.toString(), index, id, OpenNLPEnricher.this.prefix, target, value
                    );
                }
                @Override
                public void onFailure(Exception e) {
                    OpenNLPEnricher.this.requestsInFlight -= 1;
                    boolean retry  = false;
                    String status  = "ERROR";
                    String message = e.toString();

                    // ERROR: Concurrent request limit exceeded. Please consider batching your requests
                    // WORKAROUND: Retry and reduce maxRequestsInFlight
                    if( e instanceof ElasticsearchStatusException ) {
                        var exception = (ElasticsearchStatusException) e;
                        if( exception.status().toString().equals("TOO_MANY_REQUESTS") ) {
                            OpenNLPEnricher.this.maxRequestsInFlight *= 0.9;  // race condition is desirable here
                            retry   = true;
                            status  = exception.status().toString();
                            message = "reducing maxRequestsInFlight = " + OpenNLPEnricher.this.maxRequestsInFlight;
                        }
                    }

                    System.out.printf("%s %s(%s) | %s%n", status, index, id, message);
                    if( retry ) {
                        try {
                            OpenNLPEnricher.this.asyncUpdate(id, target, value);
                        } catch( IOException ioException ) {
                            ioException.printStackTrace();
                        }
                    }
                }
            }
        );
    }


    public static void main(String[] args) throws IOException {
        var targets = Arrays.asList("target", "keyword");
        var accuracies = new TreeMap<String, Double>();
        for( String target : targets ) {
            var enricher = new OpenNLPEnricher("twitter", Arrays.asList("text", "location"), target);
            enricher.train();
            enricher.enrich();
            Double accuracy = enricher.getAccuracy();
            accuracies.put(target, accuracy);
        }
        String className = MethodHandles.lookup().lookupClass().getSimpleName();
        System.out.printf("%s() accuracy for on training data: %s%n", className, accuracies.toString());
        ESClient.getInstance().close();
    }
}
