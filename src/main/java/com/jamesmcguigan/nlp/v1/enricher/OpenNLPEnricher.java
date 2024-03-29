package com.jamesmcguigan.nlp.v1.enricher;

import com.jamesmcguigan.nlp.utils.data.ESJsonPath;
import com.jamesmcguigan.nlp.utils.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.utils.elasticsearch.read.ScanAndScrollIterator;
import com.jamesmcguigan.nlp.utils.elasticsearch.update.BulkUpdateQueue;
import com.jamesmcguigan.nlp.utils.iterators.streams.ESDocumentStream;
import com.jamesmcguigan.nlp.utils.tokenize.ATokenizer;
import com.jamesmcguigan.nlp.utils.tokenize.NLPTokenizer;
import com.jamesmcguigan.nlp.v1.classifier.OpenNLPClassifierES;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;

/**
 *  OpenNLPEnricher streams from ElasticSearch via `ESDocumentStream()`
 *  trains on data extracted via `target` field,
 *  predicts using `OpenNLPClassifierES()`
 *  updates ElasticSearch with new prediction metadata
 */
@SuppressWarnings("unchecked")
public class OpenNLPEnricher {
    private ATokenizer tokenizer = NLPTokenizer.getDefaultTokenizer();

    private final String       index;
    private final List<String> fields;
    private final String       target;
    private String             prefix = "_opennlp";
    private final OpenNLPClassifierES classifier = new OpenNLPClassifierES();



    //***** Constructors *****//

    public OpenNLPEnricher(String index, List<String> fields, String target) { this(index, fields, target, null); }
    public OpenNLPEnricher(String index, List<String> fields, String target, @Nullable String prefix) {
        this.index  = index;
        this.fields = new ArrayList<>(fields);
        this.target = target;
        this.prefix = (prefix != null) ? prefix : this.prefix;
    }



    //***** Getters / Setters *****//

    public <T extends OpenNLPEnricher> T load(Path filepath) throws IOException { this.classifier.load(filepath); return (T) this; }
    public <T extends OpenNLPEnricher> T save(Path filepath) throws IOException { this.classifier.save(filepath); return (T) this; }

    public ATokenizer getTokenizer() { return this.tokenizer; }
    public <T extends OpenNLPEnricher> T setTokenizer(ATokenizer tokenizer) { this.tokenizer = tokenizer; return (T) this; }

    public String getUpdateKey(String target) { return this.prefix.isEmpty() ? target : (this.prefix+'.'+target); }



    //***** Train *****//

    public <T extends OpenNLPEnricher> T train() throws IOException { return train(null); }
    public <T extends OpenNLPEnricher> T train(@Nullable QueryBuilder query) throws IOException {
        BoolQueryBuilder streamQuery = new BoolQueryBuilder();
        streamQuery.must(existsQuery(target));
        if( query != null ) { streamQuery.must(query); }

        try (
            ESDocumentStream stream = new ESDocumentStream(
                index, fields, target, streamQuery
            ).setTokenizer(classifier.getTokenizer())
        ) {
            classifier.train(stream);
        }
        return (T) this;
    }



    //***** Enrich *****//

    public <T extends OpenNLPEnricher> T enrich() { return enrich(null); }
    public <T extends OpenNLPEnricher> T enrich(@Nullable QueryBuilder query) {
        try(
            var updateQueue = new BulkUpdateQueue(this.index)
        ) {
            var request = new ScanAndScrollIterator<>(String.class, index, query);
            while( request.hasNext() ) {
                String json         = request.next();
                ESJsonPath jsonPath = new ESJsonPath(json);
                String id           = jsonPath.get("id");
                String[] tokens     = this.tokenizer.tokenize(jsonPath.get(this.fields));
                String prediction   = this.classifier.predict(tokens);
                String updateKey    = this.getUpdateKey(this.target);

                if( isUpdateRequired(jsonPath, updateKey, prediction) ) {
                    updateQueue.update(id, updateKey, prediction);
                }
            }
        }
        return (T) this;
    }

    private static boolean isUpdateRequired(ESJsonPath jsonPath, String updateKey, String prediction) {
        String existing = jsonPath.get(updateKey);
        return !prediction.equals(existing);
    }



    //***** Main *****//

    public static void main(String[] args) throws IOException {
        var targets = Arrays.asList("target", "keyword");
        for( String target : targets ) {
            var enricher = new OpenNLPEnricher("twitter", Arrays.asList("text", "location"), target);
            enricher.train();
            enricher.enrich();
        }
        ESClient.getInstance().close();
    }
}
