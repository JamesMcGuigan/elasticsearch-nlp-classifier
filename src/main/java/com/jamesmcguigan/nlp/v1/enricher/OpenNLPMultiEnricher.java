package com.jamesmcguigan.nlp.v1.enricher;

import com.google.common.collect.Streams;
import com.jamesmcguigan.nlp.utils.data.ESJsonPath;
import com.jamesmcguigan.nlp.utils.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.utils.elasticsearch.read.ScanAndScrollIterator;
import com.jamesmcguigan.nlp.utils.elasticsearch.update.BulkUpdateQueue;
import com.jamesmcguigan.nlp.utils.iterators.multiplex.MultiplexIterator;
import com.jamesmcguigan.nlp.utils.iterators.multiplex.MultiplexIterators;
import com.jamesmcguigan.nlp.utils.iterators.streams.FilteredJsonDocumentStream;
import com.jamesmcguigan.nlp.utils.iterators.streams.JsonDocumentStream;
import com.jamesmcguigan.nlp.v1.classifier.OpenNLPClassifier;
import opennlp.tools.tokenize.Tokenizer;
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
    protected Tokenizer tokenizer = ESJsonPath.getDefaultTokenizer();

    protected final String       index;
    protected final List<String> fields;
    protected final List<String> targets;
    protected String             prefix = "_opennlp";

    protected final Map<String, OpenNLPClassifier> classifiers;


    //***** Constructors *****//

    public OpenNLPMultiEnricher(String index, List<String> fields, List<String> targets) {
        this.index   = index;
        this.fields  = fields;
        this.targets = targets;
        this.classifiers = targets.stream().collect(Collectors.toMap(
            (String target) -> target,
            (String target) -> new OpenNLPClassifier()
        ));
    }
    public OpenNLPMultiEnricher(String index, List<String> fields, List<String> targets, String prefix) {
        this(index, fields, targets);
        this.prefix = prefix;
    }




    //***** Getters / Setters *****//

    public String getUpdateKey(String target) { return this.prefix.isEmpty() ? target : (this.prefix+'.'+target); }

    public Tokenizer getTokenizer() { return this.tokenizer; }
    public <T extends OpenNLPMultiEnricher> T setTokenizer(Tokenizer tokenizer) { this.tokenizer = tokenizer; return (T) this; }



    //***** Iterators *****//

    public Iterator<String> getIterator(@Nullable QueryBuilder query) throws IOException {
        return new ScanAndScrollIterator<>(String.class, this.index, query);
    }

    protected QueryBuilder getTargetQuery(@Nullable QueryBuilder query) {
        var targetQuery = boolQuery();
        if( query != null ) { targetQuery = targetQuery.must(query); }
        for( String target : this.targets ) { targetQuery = targetQuery.must(existsQuery(target)); }
        return targetQuery;
    }


    //***** Train *****//

    public <T extends OpenNLPMultiEnricher> T train() throws IOException { return train(null); }
    public <T extends OpenNLPMultiEnricher> T train(@Nullable QueryBuilder query) throws IOException {
        var targetQuery = this.getTargetQuery(query);
        var scanAndScroll = this.getIterator(targetQuery);
        var multiplexer   = new MultiplexIterators<>(scanAndScroll, this.targets);
        multiplexer
            .parallelStream()
            .forEach(this::trainClassifier)
        ;
        return (T) this;
    }

    protected void trainClassifier(MultiplexIterator<String> iterator) {
        String target                = iterator.getName();
        OpenNLPClassifier classifier = this.classifiers.get(target).setTokenizer(this.tokenizer);
        JsonDocumentStream stream    = new FilteredJsonDocumentStream(iterator, this.fields, target);
        try {
            classifier.train(stream);
        } catch( IOException e ) {
            logger.error(e);
        }
    }



    //***** Enrich *****//

    public <T extends OpenNLPMultiEnricher> T enrich() throws IOException { return enrich(null); }
    public <T extends OpenNLPMultiEnricher> T enrich(@Nullable QueryBuilder query) throws IOException {
        try(
            BulkUpdateQueue updateQueue = new BulkUpdateQueue(this.index)
        ) {
            // Read the items from scanAndScroll one at a time
            var scanAndScroll = new ScanAndScrollIterator<>(String.class, this.index, query);
            Streams.stream(scanAndScroll)
                .parallel()
                .map(this::predictUpdatePairFromJson)
                .filter(Objects::nonNull)  // remove empty updateMaps
                .forEachOrdered((ImmutablePair<String, Map<String, Object>> pair) -> {
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
        String[] tokens = jsonPath.tokenize(this.fields);

        // Loop over each of the target fields
        HashMap<String, Object> updateMap = new HashMap<>();
        for( String target : this.targets ) {
            OpenNLPClassifier classifier = this.classifiers.get(target);
            String prediction = classifier.predict(tokens);
            String updateKey  = this.getUpdateKey(target);

            if( isUpdateRequired(jsonPath, updateKey, prediction) ) {
                // NOTE: hardcoded use of "top.level.keys"
                //       Would require a flag to correctly put() into nested objects
                updateMap.put(updateKey, prediction);
            }
        }
        return ( !updateMap.isEmpty() )
            ? new ImmutablePair<>(id, updateMap)
            : null;
    }

    private static boolean isUpdateRequired(ESJsonPath jsonPath, String updateKey, String prediction) {
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
