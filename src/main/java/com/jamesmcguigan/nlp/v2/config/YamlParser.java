package com.jamesmcguigan.nlp.v2.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class YamlParser {
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    static { mapper.findAndRegisterModules(); }  // Add YAML plugin

    private YamlParser() {}



    //***** Extractor Config *****//

    public static List<DatasetConfig> getExtractorConfigs(List<Path> paths) {
        return paths.stream()
            .map(YamlParser::getDatasetConfigs)
            .flatMap(Collection::stream)
            .collect(Collectors.toList())
        ;
    }
    public static List<ExtractorConfig> getExtractorConfigs(Path path) {
        try {
            String yaml = Files.readString(path);
            return getExtractorConfigs(yaml);
        } catch( InvalidConfigurationException | IOException exception ) {
            throw new InvalidConfigurationException(path.toString(), exception);
        }
    }
    @SuppressWarnings("unchecked")
    public static List<ExtractorConfig> getExtractorConfigs(String yaml) {
        try {
            Map<String, Object> yamlMap  = mapper.readValue(yaml.getBytes(), Map.class);
            List<ExtractorConfig> output = new ArrayList<>();
            for( Map.Entry<String, Object> entry : yamlMap.entrySet() ) {
                ExtractorConfigYaml yamlConfig = mapper.convertValue(entry.getValue(), ExtractorConfigYaml.class);
                ExtractorConfig     config     = new ExtractorConfig(yamlConfig, entry.getKey());
                output.add(config);
            }
            return output;
        } catch( InvalidConfigurationException | IllegalArgumentException | IOException exception ) {
            throw new InvalidConfigurationException(yaml, exception);
        }
    }



    //***** Dataset Config *****//

    public static List<DatasetConfig> getDatasetConfigs(List<Path> paths) {
        return paths.stream()
            .map(YamlParser::getDatasetConfigs)
            .flatMap(Collection::stream)
            .collect(Collectors.toList())
        ;
    }
    public static List<DatasetConfig> getDatasetConfigs(Path path) {
        try {
            String yaml = Files.readString(path);
            return getDatasetConfigs(yaml);
        } catch( InvalidConfigurationException | IOException exception ) {
            throw new InvalidConfigurationException(path.toString(), exception);
        }
    }
    @SuppressWarnings("unchecked")
    public static List<DatasetConfig> getDatasetConfigs(String yaml) {
        try {
            Map<String, Object> yamlMap = mapper.readValue(yaml.getBytes(), Map.class);
            List<DatasetConfig> output = new ArrayList<>();
            for( Map.Entry<String, Object> entry : yamlMap.entrySet() ) {
                DatasetConfigYaml yamlConfig = mapper.convertValue(entry.getValue(), DatasetConfigYaml.class);
                DatasetConfig     config     = new DatasetConfig(yamlConfig, entry.getKey());
                output.add(config);
            }
            return output;
        } catch( InvalidConfigurationException | IllegalArgumentException | IOException exception ) {
            throw new InvalidConfigurationException(yaml, exception);
        }
    }

}
