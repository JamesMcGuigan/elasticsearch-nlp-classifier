#!/usr/bin/env bash
set -x
kaggle competitions download -c nlp-getting-started                               -p input/nlp-getting-started
kaggle competitions download -c jigsaw-unintended-bias-in-toxicity-classification -p input/jigsaw-unintended-bias-in-toxicity-classification
kaggle competitions download -c jigsaw-toxic-comment-classification-challenge     -p input/jigsaw-toxic-comment-classification-challenge
kaggle competitions download -c quora-insincere-questions-classification          -p input/quora-insincere-questions-classification
kaggle competitions download -c google-quest-challenge                            -p input/google-quest-challenge
kaggle competitions download -c tradeshift-text-classification                    -p input/tradeshift-text-classification
kaggle competitions download -c whats-cooking                                     -p input/whats-cooking
kaggle competitions download -c sentiment-analysis-on-movie-reviews               -p input/sentiment-analysis-on-movie-reviews
kaggle competitions download -c random-acts-of-pizza                              -p input/random-acts-of-pizza
kaggle competitions download -c word2vec-nlp-tutorial                             -p input/word2vec-nlp-tutorial
kaggle competitions download -c stumbleupon                                       -p input/stumbleupon

# Unzip needs to be run twice as we have nested zip files
for FILE in `find input/ -name '*.zip'`; do unzip -u $FILE -d `dirname $FILE`; done;
for FILE in `find input/ -name '*.zip'`; do unzip -u $FILE -d `dirname $FILE`; done;
for FILE in `find input/ -name '*.gz'`;  do gzip  -d $FILE; done;
