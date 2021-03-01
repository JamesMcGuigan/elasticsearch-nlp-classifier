package com.jamesmcguigan.nlp.v2.datasets;

import com.jamesmcguigan.nlp.utils.elasticsearch.read.ScanAndScrollIterator;
import com.jamesmcguigan.nlp.v2.config.DatasetConfig;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.*;

// TODO: write unit tests
public class ElasticsearchDataset extends AbstractDataset {
    private final String index;
    private final List<String> fields;

    public ElasticsearchDataset(DatasetConfig config, String condition) {
        super(config, condition);
        this.index  = config.getIndex();
        this.fields = config.getIdFieldLabels();
    }


    //***** Queries *****//

    private QueryBuilder getConditionQuery() {
        return (condition != null && !condition.isEmpty()) ? QueryBuilders.queryStringQuery(condition) : matchAllQuery();
    }
    private QueryBuilder getTestQuery() {
        return getConditionQuery();
    }
    private QueryBuilder getTrainQuery() {
        BoolQueryBuilder query = boolQuery();
        query.must(getConditionQuery());
        for( String field : this.config.getLabels().keySet() ) {
            query.must(existsQuery(field));             // missing field is not a training match
            query.mustNot(termQuery(field, ""));  // empty string is not training match
        }
        return query;
    }
    private QueryBuilder getKFoldQuery(int fold, int folds) {
        if(!( 0 <= fold && fold < folds)) { throw new IllegalArgumentException("PRECONDITION: 0 <= fold="+fold+" < folds="+folds); }

        BoolQueryBuilder query = boolQuery();
        query.must( this.getTrainQuery() );
        query.must( scriptQuery(new Script(
            "Integer.parseInt(doc['_id'].value) % "+folds+" == "+fold))
        );
        return query;
    }

    //***** Streams *****//

    public Stream<DataRow> getStream(List<String> fields, QueryBuilder query) {
        var scanAndScroll = new ScanAndScrollIterator<>(JSONObject.class, this.index, fields, query);
        return Stream.iterate(
            scanAndScroll.next(),
            i -> scanAndScroll.hasNext(),
            i -> scanAndScroll.next()
        ).map(json -> new DataRow(json.toMap(), this.config));
    }

    @Override
    public Stream<DataRow> getTrainStream() {
        return this.getStream(this.fields, this.getTrainQuery());
    }

    @Override
    public Stream<DataRow> getTestStream() {
        return this.getStream(this.fields, this.getTestQuery());
    }

    @Override
    public List<Stream<DataRow>> getKFoldStreams(int folds) {
        if(!( folds >= 1 )) { throw new IllegalArgumentException("PRECONDITION: folds="+folds+" >= 1"); }

        List<Stream<DataRow>> streams = new ArrayList<>();
        for( int fold = 0; fold < folds; fold++ ) {
            streams.add( this.getStream(this.fields, this.getKFoldQuery(fold, folds)) );
        }
        return streams;
    }
}
