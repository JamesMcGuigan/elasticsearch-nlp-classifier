package com.jamesmcguigan.nlp.enricher;

import com.jamesmcguigan.nlp.classifier.OpenNLPClassifierES;
import com.jamesmcguigan.nlp.data.ESJsonPath;
import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.elasticsearch.actions.AsyncUpdateQueue;
import com.jamesmcguigan.nlp.elasticsearch.actions.ScanAndScrollRequest;
import com.jamesmcguigan.nlp.streams.ESDocumentStream;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

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
    private String             prefix = "_opennlp";

    private double accuracy = 0.0;
    private final OpenNLPClassifierES classifier = new OpenNLPClassifierES();


    public OpenNLPEnricher(String index, List<String> fields, String target) { this(index, fields, target, null); }
    public OpenNLPEnricher(String index, List<String> fields, String target, @Nullable String prefix) {
        this.index  = index;
        this.fields = fields;
        this.target = target;
        this.prefix = (prefix != null) ? prefix : this.prefix;
    }

    public <T extends OpenNLPEnricher> T load(Path filepath) throws IOException { this.classifier.load(filepath); return (T) this; }
    public <T extends OpenNLPEnricher> T save(Path filepath) throws IOException { this.classifier.save(filepath); return (T) this; }

    public double getAccuracy()               { return this.accuracy; }
    public String getUpdateKey(String target) { return this.prefix.isEmpty() ? target : this.prefix+'.'+target; }



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
        var request     = new ScanAndScrollRequest<>(index, query, String.class);
        var updateQueue = new AsyncUpdateQueue(this.index);

        int correct = 0;
        int count   = 0;
        while( request.hasNext() ) {
            String json       = request.next();
            var jsonPath      = new ESJsonPath(json);
            String id         = jsonPath.get("id");
            String category   = jsonPath.get(this.target);
            String[] tokens   = jsonPath.tokenize(this.fields).toArray(new String[0]);
            String prediction = this.classifier.predict(tokens);
            String updateKey  = this.getUpdateKey(this.target);

            if( this.isUpdateRequired(jsonPath, updateKey, prediction) ) {
                updateQueue.add(id, updateKey, prediction);
            }

            if( !category.isEmpty() ) {
                correct += category.equals(prediction) ? 1 : 0;
                count   += 1;
            }
        }
        this.accuracy = (count > 0) ? (double) correct / count : 0.0;
        return (T) this;
    }
    private boolean isUpdateRequired(ESJsonPath jsonPath, String updateKey, String prediction) {
        String existing = jsonPath.get(updateKey);
        return !prediction.equals(existing);
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
