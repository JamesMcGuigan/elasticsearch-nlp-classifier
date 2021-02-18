package com.jamesmcguigan.nlp.enricher;

import com.jamesmcguigan.nlp.data.ESJsonPath;
import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.elasticsearch.read.ScanAndScrollIterator;
import com.jamesmcguigan.nlp.elasticsearch.update.BulkUpdateQueue;
import com.jamesmcguigan.nlp.enricher.classifier.OpenNLPClassifierES;
import com.jamesmcguigan.nlp.iterators.streams.ESDocumentStream;
import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
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
    private static final Logger logger = LogManager.getLogger();
    private NLPTokenizer tokenizer     = ESJsonPath.getDefaultTokenizer();

    private final String       index;
    private final List<String> fields;
    private final String       target;  // TODO: convert to List<String> targets
    private String             prefix = "_opennlp";
    private final OpenNLPClassifierES classifier = new OpenNLPClassifierES();  // TODO: convert to multi-field Map



    //***** Constructors *****//

    public OpenNLPEnricher(String index, List<String> fields, String target) { this(index, fields, target, null); }
    public OpenNLPEnricher(String index, List<String> fields, String target, @Nullable String prefix) {
        this.index  = index;
        this.fields = fields;
        this.target = target;
        this.prefix = (prefix != null) ? prefix : this.prefix;
    }



    //***** Getters / Setters *****//

    public <T extends OpenNLPEnricher> T load(Path filepath) throws IOException { this.classifier.load(filepath); return (T) this; }
    public <T extends OpenNLPEnricher> T save(Path filepath) throws IOException { this.classifier.save(filepath); return (T) this; }

    public NLPTokenizer getTokenizer() { return this.tokenizer; }
    public <T extends OpenNLPEnricher> T setTokenizer(NLPTokenizer tokenizer) { this.tokenizer = tokenizer; return (T) this; }

    public String getUpdateKey(String target) { return this.prefix.isEmpty() ? target : this.prefix+'.'+target; }



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

    public <T extends OpenNLPEnricher> T enrich() throws IOException { return enrich(null); }
    public <T extends OpenNLPEnricher> T enrich(@Nullable QueryBuilder query) throws IOException {
        try(
            var updateQueue = new BulkUpdateQueue(this.index)
        ) {
            var request = new ScanAndScrollIterator<>(String.class, index, query);
            while( request.hasNext() ) {
                String json         = request.next();
                ESJsonPath jsonPath = new ESJsonPath(json).setTokenizer(this.tokenizer);
                String id           = jsonPath.get("id");
                String[] tokens     = jsonPath.tokenize(this.fields).toArray(new String[0]);
                String prediction   = this.classifier.predict(tokens);
                String updateKey    = this.getUpdateKey(this.target);

                if( this.isUpdateRequired(jsonPath, updateKey, prediction) ) {
                    updateQueue.update(id, updateKey, prediction);
                }
            }
        }
        return (T) this;
    }

    private boolean isUpdateRequired(ESJsonPath jsonPath, String updateKey, String prediction) {
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
