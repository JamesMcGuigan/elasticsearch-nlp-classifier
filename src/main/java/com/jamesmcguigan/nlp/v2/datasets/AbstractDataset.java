package com.jamesmcguigan.nlp.v2.datasets;

import com.jamesmcguigan.nlp.v2.config.DatasetConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


// TODO: write unit tests
public abstract class AbstractDataset implements Dataset {
    protected DatasetConfig config;
    protected String condition;

    protected AbstractDataset(DatasetConfig config, String condition) {
        this.config    = config;
        this.condition = condition;
    }

    public List<Map<TestTrain,Stream<DataRow>>> getKFoldTestTrainStreams(int folds) {
        if(!( folds >= 1 )) { throw new IllegalArgumentException("PRECONDITION: folds="+folds+" >= 1"); }

        List<Map<TestTrain,Stream<DataRow>>> streamPairs = new ArrayList<>();
        for( int fold = 0; fold < folds; fold++ ) {
            // Merge [1,2,3] stream folds into { test: (1), train: (2,3) } test/train pairs
            var streams = this.getKFoldStreams(folds);
            Stream<DataRow> test  = streams.get(fold);
            Stream<DataRow> train = Stream.empty();
            for( int i = 0; i < folds; i++ ) {
                if( i == fold ) { continue; }
                train = Stream.concat(train, streams.get(i));
            }
            streamPairs.add(Map.of(
                TestTrain.train, train,
                TestTrain.test,  test
            ));
        }
        return streamPairs;
    }
}
