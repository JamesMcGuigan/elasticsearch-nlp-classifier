package com.jamesmcguigan.nlp.v2.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jamesmcguigan.nlp.v2.config.yaml.ExtractorConfigYaml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class YamlParser {
    private static final Logger logger = LogManager.getLogger();

    private YamlParser() {}

    @SuppressWarnings("unchecked")
    public static List<ExtractorConfig> getExtractorConfigs(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();   // Add YAML plugin

        Map<String, Object> yaml = mapper.readValue(new File(path.toUri()), Map.class);
        List<ExtractorConfig> rules   = new ArrayList<>();
        for( Map.Entry<String, Object> entry : yaml.entrySet() ) {
            try {
                ExtractorConfigYaml config = mapper.convertValue(entry.getValue(), ExtractorConfigYaml.class);
                ExtractorConfig rule       = new ExtractorConfig(config, entry.getKey(), path);
                rules.add(rule);
            } catch(
                InstantiationException | InvocationTargetException | NoSuchMethodException |
                IllegalAccessException | ClassNotFoundException e
            ) {
                logger.error(e);
                e.printStackTrace();
            }
        }
        return rules;
    }
}
