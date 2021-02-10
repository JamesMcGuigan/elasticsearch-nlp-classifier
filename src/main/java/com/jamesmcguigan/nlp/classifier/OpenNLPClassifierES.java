package com.jamesmcguigan.nlp.classifier;

import com.jamesmcguigan.nlp.streams.ESDocumentStream;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class OpenNLPClassifierES extends OpenNLPClassifier {

    public static void main(String[] args) throws IOException {
        var classifier = new OpenNLPClassifierES();
        long split     = Math.round(1 / 0.2);
        String index   = "twitter";
        String target  = "target";
        List<String> fields = Arrays.asList("text", "location");

        try(
            ESDocumentStream trainStream = new ESDocumentStream(
                index, fields, target,
                boolQuery()
                    .must(existsQuery(target))
                    .mustNot(scriptQuery(new Script("Integer.parseInt(doc['_id'].value) % "+split+" == 0")))
            ).setTokenizer(classifier.tokenizer);

            ESDocumentStream testStream = new ESDocumentStream(
                index, fields, target,
                boolQuery()
                    .must(existsQuery(target))
                    .must(scriptQuery(new Script("Integer.parseInt(doc['_id'].value) % "+split+" == 0")))
            ).setTokenizer(classifier.tokenizer);
        ) {
            // TODO: multi-target classification
            classifier.train(trainStream);
            double accuracy = classifier.accuracy(testStream);

            String className = MethodHandles.lookup().lookupClass().getSimpleName();
            System.out.printf(
                "%s() accuracy on %d/%d test-train-split is %.3f%n",
                className, testStream.getTotalHits(), trainStream.getTotalHits(), accuracy
            );
        }
    }
}
