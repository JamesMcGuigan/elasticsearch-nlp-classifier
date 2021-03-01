package com.jamesmcguigan.nlp.v2.controller;

import com.jamesmcguigan.nlp.v2.config.ControllerConfig;
import com.jamesmcguigan.nlp.v2.datasets.Dataset;
import com.jamesmcguigan.nlp.v2.datasets.Datasets;

import java.util.List;

public class Controller {
    private final ControllerConfig config;
    private final List<Dataset>    datasets;

    public Controller(ControllerConfig config) {
        this.config = config;
        this.datasets = Datasets.from(config.getDatasets(), config.getCondition());
    }

    @SuppressWarnings("EmptyMethod")
    public void run() {
        // TODO: implement
    }
}
