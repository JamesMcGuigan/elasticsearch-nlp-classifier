// Example: https://github.com/mmm0469/apache-opennlp-examples/blob/master/DocumentCategorizerMaxentExample.java
package com.jamesmcguigan.nlp.classifier;

import com.google.common.collect.Lists;
import com.jamesmcguigan.nlp.csv.Tweet;
import com.jamesmcguigan.nlp.csv.Tweets;
import com.jamesmcguigan.nlp.streams.TweetDocumentStream;
import opennlp.tools.doccat.DoccatFactory;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.TrainingParameters;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class OpenNLPClassifierTweet {

    private final TrainingParameters params;
    private DoccatModel model;
    private DocumentCategorizerME doccat;



    //***** Constructor *****//

    public OpenNLPClassifierTweet() {
        this.params = new TrainingParameters();

        // Settings for: MAXENT_VALUE
        params.put(TrainingParameters.ITERATIONS_PARAM, "1000");       // accuracy = 0.750
        // params.put(TrainingParameters.ITERATIONS_PARAM, "2500");    // accuracy = 0.750
        // params.put(TrainingParameters.ITERATIONS_PARAM, "10000");   // accuracy = 0.742
        // params.put(TrainingParameters.ITERATIONS_PARAM, "20000");   // accuracy = 0.734
        // params.put(TrainingParameters.ITERATIONS_PARAM, "100000");  // accuracy = 0.727
        params.put(TrainingParameters.CUTOFF_PARAM, "0");

        // TrainerFactory.BUILTIN_TRAINERS = [MAXENT_QN, MAXENT, PERCEPTRON, NAIVEBAYES, PERCEPTRON_SEQUENCE]
        params.put(TrainingParameters.ALGORITHM_PARAM, "NAIVEBAYES");  // Kaggle Score = 0.78026
    }
    public OpenNLPClassifierTweet(Path filename) throws IOException {
        this();
        this.model  = new DoccatModel(filename);
        this.doccat = new DocumentCategorizerME(this.model);
    }

    public void save(Path file) throws IOException {
        this.model.serialize(file);
    }



    //***** Training and Prediction *****//

    public void train(List<Tweet> tweets) throws IOException {
        var objectStream = new TweetDocumentStream(tweets);
        this.train(objectStream);
    }
    public void train(ObjectStream<DocumentSample> objectStream) throws IOException {
        this.model = DocumentCategorizerME.train(
            "en",
            objectStream,
            this.params,
            new DoccatFactory()
        );
        this.doccat = new DocumentCategorizerME(model);
    }


    public String predict(Tweet tweet) {
        if( this.model == null ) {
            throw new UnsupportedOperationException("model not trained yet");
        }
        var tokens = tweet.tokenize().toArray(new String[0]);
        double[] probabilities = this.doccat.categorize(tokens);
        var category = doccat.getBestCategory(probabilities);
        return category;
    }
    public List<String> predict(List<Tweet> tweets) {
        return tweets.parallelStream()
            .map(this::predict)
            .collect(Collectors.toList())
        ;
    }



    //***** Validation *****//

    /**
     * Split a list of objects into N groups of Pair(train, test) tuples for kFold validation
     *
     * @param tweets list of tweets to split
     * @param folds number of folds to generate
     * @return list of Pair(train, test) tuples with N=fold tweets
     */
    public static List<Pair<List<Tweet>, List<Tweet>>> kFold(List<Tweet> tweets, int folds) {
        List<Pair<List<Tweet>, List<Tweet>>> kFolds = new ArrayList<>();

        var shuffled = new ArrayList<>(tweets);
        Collections.shuffle(shuffled);
        int partitionSize = (int) Math.ceil( (double) shuffled.size() / folds );
        var partitions = Lists.partition(shuffled, partitionSize);
        for( int fold = 0; fold < folds; fold++ ) {
            List<Tweet> test  = new ArrayList<>();
            List<Tweet> train = new ArrayList<>();
            for( int i = 0; i < folds; i++ ) {
                if( i == fold ) {
                    test.addAll(partitions.get(i));
                } else {
                    train.addAll(partitions.get(i));
                }
            }
            var kFold = new ImmutablePair<>(train, test);
            kFolds.add(kFold);
        }
        return kFolds;
    }

    public static double kFoldValidation(List<Tweet> tweets, int folds) throws IOException {
        var correct = new double[folds];
        var counts  = new double[folds];
        var kFolds = OpenNLPClassifierTweet.kFold(tweets, folds);
        for( int fold = 0; fold < kFolds.size(); fold++ ) {
            var kFold = kFolds.get(fold);
            var train = kFold.getLeft();
            var test  = kFold.getRight();

            var classifier = new OpenNLPClassifierTweet();
            classifier.train(train);
            var predictions = classifier.predict(test);
            for( int i = 0; i < predictions.size(); i++ ) {
                if( test.get(i).target.equals(predictions.get(i)) ) {
                    correct[fold] += 1;
                }
                counts[fold] += 1;
            }
        }
        double accuracy = Arrays.stream(correct).sum() / Arrays.stream(counts).sum();
        return accuracy;
    }


    public static void main(String[] args) throws IOException {
        String className    = MethodHandles.lookup().lookupClass().getSimpleName();
        Path trainPath      = Paths.get("input/train.csv");
        Path testPath       = Paths.get("input/test.csv");
        Path submissionPath = Paths.get("output/submission-"+className+".csv");
        List<Tweet> train   = Tweets.fromCSV(trainPath);
        List<Tweet> test    = Tweets.fromCSV(testPath);

        var folds = 3;
        var accuracy = OpenNLPClassifierTweet.kFoldValidation(train, folds);

        var classifier = new OpenNLPClassifierTweet();
        classifier.train(train);
        var predictions = classifier.predict(test);
        Tweets.toSubmissionCSV(submissionPath, test, predictions);

        System.out.printf(
             "%s() accuracy with kFolds=%d is %.3f%n",
            className, folds, accuracy
        );
    }
}
