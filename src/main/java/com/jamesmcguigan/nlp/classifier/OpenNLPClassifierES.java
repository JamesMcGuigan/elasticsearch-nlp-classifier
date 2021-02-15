package com.jamesmcguigan.nlp.classifier;

import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import com.jamesmcguigan.nlp.streams.ESDocumentStream;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class OpenNLPClassifierES extends OpenNLPClassifier {

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
                // TODO: multi-target classification
                this.train(trainStream);
                accuracies[fold] = this.accuracy(testStream);
                trainHits[fold]  = trainStream.getTotalHits();
                testHits[fold]   = testStream.getTotalHits();
            }
        }

        double accuracy    = Arrays.stream(accuracies).average().orElse(0);
        long meanTrainHits = (long) Arrays.stream(trainHits).average().orElse(0);
        long meanTestHits  = (long) Arrays.stream(testHits).average().orElse(0);
        String className = MethodHandles.lookup().lookupClass().getSimpleName();
        System.out.printf("%s() accuracy on %d folds (%d/%d split) is %.3f%n",
            className, folds, meanTestHits, meanTrainHits, accuracy);

        return accuracy;
    }

    public static void main(String[] args) throws IOException {
        var classifier = new OpenNLPClassifierES();
        classifier.kFoldValidation("twitter", Arrays.asList("text", "location"), "target", 3);
        ESClient.getInstance().close();
    }
}
