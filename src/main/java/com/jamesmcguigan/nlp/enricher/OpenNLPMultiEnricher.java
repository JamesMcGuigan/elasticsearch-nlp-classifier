package com.jamesmcguigan.nlp.enricher;

import com.google.common.collect.Streams;
import com.jamesmcguigan.nlp.data.ESJsonPath;
import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.elasticsearch.actions.BulkUpdateQueue;
import com.jamesmcguigan.nlp.elasticsearch.actions.ScanAndScrollIterator;
import com.jamesmcguigan.nlp.enricher.classifier.OpenNLPClassifier;
import com.jamesmcguigan.nlp.iterators.MultiplexIterator;
import com.jamesmcguigan.nlp.iterators.MultiplexIterators;
import com.jamesmcguigan.nlp.streams.FilteredJsonDocumentStream;
import com.jamesmcguigan.nlp.streams.JsonDocumentStream;
import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.QueryBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;

/**
 *  OpenNLPMultiEnricher streams from ElasticSearch via {@code }ScanAndScrollIterator()}
 *  trains multiple {@code OpenNLPClassifier}s in parallel, one for each {@code this.targets}
 *  predicts multiple fields in parallel
 *  updates ElasticSearch with combined prediction metadata
 */
@SuppressWarnings("unchecked")
public class OpenNLPMultiEnricher {
    private static final Logger logger = LogManager.getLogger();
    private NLPTokenizer tokenizer     = ESJsonPath.getDefaultTokenizer();

    private final String       index;
    private final List<String> fields;
    private final List<String> targets;
    private String             prefix = "_opennlp";

    private final Map<String, OpenNLPClassifier> classifiers;


    //***** Constructors *****//

    public OpenNLPMultiEnricher(String index, List<String> fields, List<String> targets) { this(index, fields, targets, null); }
    public OpenNLPMultiEnricher(String index, List<String> fields, List<String> targets, @Nullable String prefix) {
        this.index   = index;
        this.fields  = fields;
        this.targets = targets;
        this.prefix  = (prefix != null) ? prefix : this.prefix;
        this.classifiers = targets.stream().collect(Collectors.toMap(
            target -> target,
            target -> new OpenNLPClassifier()
        ));
    }



    //***** Getters / Setters *****//

    public String getUpdateKey(String target) { return this.prefix.isEmpty() ? target : this.prefix+'.'+target; }

    public NLPTokenizer getTokenizer() { return this.tokenizer; }
    public <T extends OpenNLPMultiEnricher> T setTokenizer(NLPTokenizer tokenizer) { this.tokenizer = tokenizer; return (T) this; }



    //***** Train *****//

    public <T extends OpenNLPMultiEnricher> T train() throws IOException { return train(null); }
    public <T extends OpenNLPMultiEnricher> T train(@Nullable QueryBuilder query) throws IOException {
        var scanAndScroll = new ScanAndScrollIterator<>(this.index, this.getTargetQuery(query), String.class);
        var multiplexer   = new MultiplexIterators<>(scanAndScroll, this.targets);
        multiplexer
            .parallelStream()
            .forEach(this::trainClassifier)
        ;
        return (T) this;
    }

    protected QueryBuilder getTargetQuery(@Nullable QueryBuilder query) {
        var targetQuery = boolQuery();
        if( query != null ) { targetQuery = targetQuery.must(query); }
        for( String target : this.targets ) { targetQuery = targetQuery.must(existsQuery(target)); }
        return targetQuery;
    }

    protected void trainClassifier(MultiplexIterator<String> iterator) {
        String target                = iterator.getName();
        OpenNLPClassifier classifier = this.classifiers.get(target).setTokenizer(this.tokenizer);
        JsonDocumentStream stream    = new FilteredJsonDocumentStream(iterator, this.fields, target);
        try {
            classifier.train(stream);
        } catch( IOException e ) {
            e.printStackTrace();
        }
    }



    //***** Enrich *****//

    public <T extends OpenNLPMultiEnricher> T enrich() throws IOException { return enrich(null); }
    public <T extends OpenNLPMultiEnricher> T enrich(@Nullable QueryBuilder query) throws IOException {
        try(
            BulkUpdateQueue updateQueue = new BulkUpdateQueue(this.index)
        ) {
            // Read the items from scanAndScroll one at a time
            // TODO: Streams.stream(scanAndScroll).parallel()
            var scanAndScroll = new ScanAndScrollIterator<>(this.index, query, String.class);
            Streams.stream(scanAndScroll)
                .parallel()
                .map(this::predictUpdatePairFromJson)
                .filter(Objects::nonNull)  // remove empty updateMaps
                .forEachOrdered(pair -> {
                    // Send the combined result from all targets back to ElasticSearch
                    // Do this synchronously to prevent ConnectionClosedException
                    String id                     = pair.getLeft();
                    Map<String, Object> updateMap = pair.getRight();
                    updateQueue.update(id, updateMap);
                })
            ;
        }
        return (T) this;
    }

    private @Nullable ImmutablePair<String, Map<String, Object>> predictUpdatePairFromJson(String json) {
        var jsonPath    = new ESJsonPath(json);
        String id       = jsonPath.get("id");
        String[] tokens = jsonPath.tokenize(this.fields).toArray(new String[0]);

        // Loop over each of the target fields
        HashMap<String, Object> updateMap = new HashMap<>();
        for( String target : this.targets ) {
            OpenNLPClassifier classifier = this.classifiers.get(target);
            String prediction = classifier.predict(tokens);
            String updateKey  = this.getUpdateKey(target);

            if( this.isUpdateRequired(jsonPath, updateKey, prediction) ) {
                // NOTE: hardcoded use of "top.level.keys"
                //       Would require a flag to correctly put() into nested objects
                updateMap.put(updateKey, prediction);
            }
        }
        return ( !updateMap.isEmpty() )
            ? new ImmutablePair<>(id, updateMap)
            : null;
    }

    private boolean isUpdateRequired(ESJsonPath jsonPath, String updateKey, String prediction) {
        String existing = jsonPath.get(updateKey);
        return !prediction.equals(existing);
    }



    //***** Main *****//

    public static void main(String[] args) throws IOException {
        var index   = "twitter";
        var fields  = Arrays.asList("text", "location");
        var targets = Arrays.asList("target", "keyword");
        var enricher = new OpenNLPMultiEnricher(index, fields, targets);
        enricher.train();
        enricher.enrich();
        ESClient.getInstance().close();
    }
}
