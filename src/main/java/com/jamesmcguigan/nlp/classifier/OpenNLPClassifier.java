package com.jamesmcguigan.nlp.classifier;

import com.jamesmcguigan.nlp.tokenize.NLPTokenizer;
import opennlp.tools.doccat.DoccatFactory;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.TrainingParameters;

import java.io.IOException;
import java.nio.file.Path;

public class OpenNLPClassifier {

    protected final TrainingParameters params;
    protected DoccatModel model;
    protected DocumentCategorizerME doccat;

    public final NLPTokenizer tokenizer = new NLPTokenizer()
        // .setCleanTwitter(true)  // Kaggle score = 0.76248
        // .setTwitter(false)      // Kaggle score = 0.76831
        .setTwitter(true)          // Kaggle score = 0.77229
        .setLowercase(true)
        .setStopwords(true)
        .setStemming(true)
    ;


    //***** Constructor *****//

    public OpenNLPClassifier() {
        // TrainerFactory.BUILTIN_TRAINERS = [MAXENT_QN, MAXENT, PERCEPTRON, NAIVEBAYES, PERCEPTRON_SEQUENCE]
        // 0.779 NAIVEBAYES
        // 0.776 MAXENT     | 100 iter + 0 cutoff
        // 0.769 MAXENT_QN  | 100 iter + 0 cutoff
        // 0.750 PERCEPTRON | 100 iter + 0 cutoff
        // NaN PERCEPTRON_SEQUENCE

        this.params = new TrainingParameters();
        params.put(TrainingParameters.ALGORITHM_PARAM, "NAIVEBAYES");    // Kaggle Score = 0.78026
        // params.put(TrainingParameters.ALGORITHM_PARAM, "MAXENT");     // Kaggle Score = 0.78026

        // Settings for: MAXENT / MAXENT_QN / PERCEPTRON
        params.put(TrainingParameters.ITERATIONS_PARAM, "100");
        params.put(TrainingParameters.CUTOFF_PARAM, "0");
    }
    public OpenNLPClassifier(Path filename) throws IOException {
        this();
        this.model  = new DoccatModel(filename);
        this.doccat = new DocumentCategorizerME(this.model);
    }

    public void save(Path file) throws IOException {
        this.model.serialize(file);
    }



    //***** Training and Prediction *****//

    public void train(ObjectStream<DocumentSample> objectStream) throws IOException {
        this.model = DocumentCategorizerME.train(
            "en",
            objectStream,
            this.params,
            new DoccatFactory()
        );
        this.doccat = new DocumentCategorizerME(model);
    }


    public String predict(String text) {
        String[] tokens = this.tokenizer.tokenize(text).toArray(new String[0]);
        return this.predict(tokens);
    }
    public String predict(String[] tokens) {
        if( this.model == null || this.doccat == null ) {
            throw new UnsupportedOperationException("model not trained yet");
        }
        double[] probabilities = this.doccat.categorize(tokens);
        String category        = doccat.getBestCategory(probabilities);
        return category;
    }


    /**
     * This can be used to predict accuracy based on a training dataset
     * @param stream        stream of DocumentSamples
     * @return              percentage of correct predictions
     * @throws IOException  stream.read() exception
     */
    public double accuracy(ObjectStream<DocumentSample> stream) throws IOException {
        int correct = 0;
        int count   = 0;

        DocumentSample document = stream.read();
        while( document != null ) {
            String category   = document.getCategory();
            String[] tokens   = document.getText();
            String prediction = this.predict(tokens);
            if( category.equals(prediction) ) {
                correct++;
            }
            count++;
            document = stream.read();
        }
        if( count == 0 ) { return 0.0; }
        return (double) correct / count;
    }

}
