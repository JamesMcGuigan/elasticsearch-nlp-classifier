// Example: https://github.com/mmm0469/apache-opennlp-examples/blob/master/DocumentCategorizerMaxentExample.java
package com.jamesmcguigan.nlp.enricher.classifier;

import com.google.common.collect.Lists;
import com.jamesmcguigan.nlp.data.Tweet;
import com.jamesmcguigan.nlp.data.Tweets;
import com.jamesmcguigan.nlp.iterators.streams.TweetDocumentStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.Level.INFO;


public class OpenNLPClassifierTweet extends OpenNLPClassifier {
    private static final Logger logger = LogManager.getLogger();

    //***** Training and Prediction *****//

    public void train(List<Tweet> tweets) throws IOException {
        var objectStream = new TweetDocumentStream(tweets);
        this.train(objectStream);
    }


    public String predict(Tweet tweet) {
        var tokens = tweet.tokenize();
        return this.predict(tokens);
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

        logger.printf(INFO, "accuracy with kFolds=%d is %.3f", folds, accuracy);
    }
}
