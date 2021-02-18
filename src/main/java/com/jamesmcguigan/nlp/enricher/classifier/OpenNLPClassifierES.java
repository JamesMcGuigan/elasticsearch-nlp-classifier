package com.jamesmcguigan.nlp.enricher.classifier;

import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.iterators.streams.ESDocumentStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.logging.log4j.Level.INFO;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class OpenNLPClassifierES extends OpenNLPClassifier {
    private static final Logger logger = LogManager.getLogger();


    public double kFoldValidation(String index, List<String> fields, String target, int folds) throws IOException {
        double[] accuracies = new double[folds];
        long[]   trainHits  = new long[folds];
        long[]   testHits   = new long[folds];

        for( int fold = 0; fold < folds; fold++ ) {
            try(
                ESDocumentStream trainStream = new ESDocumentStream(
                    index, fields, target,
                    boolQuery()
                        .must(existsQuery(target))
                        .mustNot(scriptQuery(new Script(
                            "Integer.parseInt(doc['_id'].value) % "+folds+" == "+fold))
                        )
                ).setTokenizer(this.tokenizer);

                ESDocumentStream testStream = new ESDocumentStream(
                    index, fields, target,
                    boolQuery()
                        .must(existsQuery(target))
                        .must(scriptQuery(new Script(
                            "Integer.parseInt(doc['_id'].value) % "+folds+" == "+fold))
                        )
                ).setTokenizer(this.tokenizer)
            ) {
                this.train(trainStream);
                double accuracy = this.accuracy(testStream);

                accuracies[fold] = accuracy;
                trainHits[fold]  = trainStream.getTotalHits();
                testHits[fold]   = testStream.getTotalHits();
            }
        }

        double accuracy    = Arrays.stream(accuracies).average().orElse(0);
        long meanTrainHits = (long) Arrays.stream(trainHits).average().orElse(0);
        long meanTestHits  = (long) Arrays.stream(testHits).average().orElse(0);
        logger.printf(INFO,"accuracy on %d folds (%d/%d split) is %.3f",
            folds, meanTestHits, meanTrainHits, accuracy);
        return accuracy;
    }

    public static void main(String[] args) throws IOException {
        var classifier = new OpenNLPClassifierES();
        classifier.kFoldValidation("twitter", Arrays.asList("text", "location"), "target", 3);
        ESClient.getInstance().close();
    }
}
