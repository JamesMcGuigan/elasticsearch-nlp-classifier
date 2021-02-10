package com.jamesmcguigan.nlp.enricher;

import com.jamesmcguigan.nlp.classifier.OpenNLPClassifierES;
import com.jamesmcguigan.nlp.csv.ESJsonPath;
import com.jamesmcguigan.nlp.elasticsearch.ScanAndScrollRequest;
import com.jamesmcguigan.nlp.streams.ESDocumentStream;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;

@SuppressWarnings("unchecked")
public class OpenNLPEnricher {

    private final String       index;
    private final List<String> fields;
    private final String       target;
    private final String       prefix;

    private double accuracy = 0.0;
    private final OpenNLPClassifierES classifier = new OpenNLPClassifierES();


    public OpenNLPEnricher(String index, List<String> fields, String target) { this(index, fields, target, null); }
    public OpenNLPEnricher(String index, List<String> fields, String target, String prefix) {
        this.index  = index;
        this.fields = fields;
        this.target = target;
        this.prefix = (prefix != null) ? prefix : getClass().getSimpleName();
    }
    public <T extends OpenNLPEnricher> T load(Path filepath) throws IOException { this.classifier.load(filepath); return (T) this; }
    public <T extends OpenNLPEnricher> T save(Path filepath) throws IOException { this.classifier.save(filepath); return (T) this; }
    public double getAccuracy() { return this.accuracy; }


    public <T extends OpenNLPEnricher> T train() throws IOException {
        try (
            ESDocumentStream stream = new ESDocumentStream(
                index, fields, target,
                boolQuery().must(existsQuery(target))
            ).setTokenizer(classifier.tokenizer)
        ) {
            classifier.train(stream);
        }
        return (T) this;
    }


    public <T extends OpenNLPEnricher> T enrich() throws IOException { return enrich(null); }
    public <T extends OpenNLPEnricher> T enrich(QueryBuilder query) throws IOException {
        ScanAndScrollRequest<String> request = new ScanAndScrollRequest<>(index, query, String.class);

        int correct = 0;
        int count   = 0;
        while( request.hasNext() ) {
            String json       = request.next();
            var jsonPath      = new ESJsonPath(json);
            String id         = jsonPath.get("_id");
            String category   = jsonPath.get(this.target);
            String[] tokens   = jsonPath.tokenize(this.fields).toArray(new String[0]);
            String prediction = this.classifier.predict(tokens);

            this.queueUpdate(id, this.target, prediction);

            if( !category.isEmpty() ) {
                correct += category.equals(prediction) ? 1 : 0;
                count   += 1;
            }
        }
        this.accuracy = (count > 0) ? (double) correct / count : 0.0;
        return (T) this;
    }


    private void queueUpdate(String id, String target, String value) {
        // TODO: implement
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
        System.out.printf("%s() accuracy for on training data: %s", className, accuracies.toString());
    }
}
