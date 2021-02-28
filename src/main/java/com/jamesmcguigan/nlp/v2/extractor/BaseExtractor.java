package com.jamesmcguigan.nlp.v2.extractor;

import com.jamesmcguigan.nlp.v2.config.ControllerConfig;

import java.util.Map;

public abstract class BaseExtractor implements Extractor {
    private final ControllerConfig parentConfig;
    private final Map<String, Object> configuration;

    protected BaseExtractor(ControllerConfig parentConfig) {
        this.parentConfig  = parentConfig;
        this.configuration = parentConfig.getConfiguration();
    }

    public String getContext() { return parentConfig.getContext(); }
    public Map<String, Object> getConfiguration() { return configuration; }
}
