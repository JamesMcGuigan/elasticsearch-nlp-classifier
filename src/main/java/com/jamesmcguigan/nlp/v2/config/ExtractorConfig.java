package com.jamesmcguigan.nlp.v2.config;

import com.jamesmcguigan.nlp.v2.extractor.Extractor;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;


@SuppressWarnings("java:S1104")
class ExtractorConfigYaml {
    public String condition;
    public String extractor;
    public String context;
    public List<Path> datasets;
    public Map<String, Object> configuration;
}


@SuppressWarnings("FieldCanBeLocal")
public class ExtractorConfig {
    private final String name;
    private final Class<? extends Extractor> extractorClass;
    private final Extractor extractor;
    private final ExtractorConfigYaml config;
    private final List<DatasetConfig> datasets;

    @SuppressWarnings("unchecked")
    public ExtractorConfig(ExtractorConfigYaml config, @Nullable String name) {
        try {
            this.name = name;
            this.config = config;
            this.extractorClass = (Class<? extends Extractor>) Class.forName(config.extractor);
            this.extractor = this.extractorClass
                .getConstructor(ExtractorConfig.class)
                .newInstance(this)
            ;
            this.datasets = YamlParser.getDatasetConfigs(config.datasets);
        } catch(
            ClassNotFoundException | NoSuchMethodException | ClassCastException |
            IllegalAccessException | InvocationTargetException | InstantiationException exception
        ) {
            throw new InvalidConfigurationException(name, exception);
        }
    }

    public String getName()          { return name; }
    public String getCondition()     { return config.condition; }
    public String getContext()       { return config.context; }
    public String getExtractorName() { return config.extractor; }
    public Extractor getExtractor()  { return extractor; }
    public Map<String, Object> getConfiguration() { return config.configuration; }
    public List<DatasetConfig> getDatasets()      { return datasets; }
}
