package com.jamesmcguigan.nlp.v2.config;

import com.jamesmcguigan.nlp.v2.extractor.Extractor;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Map;


@SuppressWarnings("java:S1104")
class ExtractorConfigYaml {
    public String condition;
    public String extractor;
    public String context;
    public Map<String, Object> configuration;
}

@SuppressWarnings("FieldCanBeLocal")
public class ExtractorConfig {
    private final Path   path;
    private final String name;
    private final String condition;
    private final String extractorName;
    private final Class<? extends Extractor> extractorClass;
    private final Extractor extractor;
    private final Map<String, Object> configuration;
    private final String context;


    @SuppressWarnings("unchecked")
    public ExtractorConfig(ExtractorConfigYaml config, @Nullable String name, @Nullable Path path) throws
        ClassNotFoundException, NoSuchMethodException, ClassCastException,
        IllegalAccessException, InvocationTargetException, InstantiationException
    {
        this.path           = path;
        this.name           = name;
        this.condition      = config.condition;
        this.configuration  = config.configuration;
        this.context        = config.context;
        this.extractorName  = config.extractor;
        this.extractorClass = (Class<? extends Extractor>) Class.forName(config.extractor);
        this.extractor      = this.extractorClass
            .getConstructor(ExtractorConfig.class)
            .newInstance(this)
        ;
    }

    public Path   getPath()          { return path; }
    public String getName()          { return name; }
    public String getCondition()     { return condition; }
    public String getContext()       { return context; }
    public String getExtractorName() { return extractorName; }
    public Extractor getExtractor()  { return extractor; }
    public Map<String, Object> getConfiguration() { return configuration; }
}
