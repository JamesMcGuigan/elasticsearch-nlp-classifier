package com.jamesmcguigan.nlp.classifier;

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



    //***** Constructor *****//

    public OpenNLPClassifier() {
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


}
