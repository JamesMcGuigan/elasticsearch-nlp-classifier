package com.jamesmcguigan.nlp.v2.config;

import com.google.common.collect.Sets;
import com.jamesmcguigan.nlp.v2.exceptions.InvalidConfigurationException;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({"java:S1104", "java:S116"})
class DatasetConfigYaml {
    public DatasetType type;
    @Nullable public String index;
    @Nullable public Files  files;
    public String id = "id";
    public Map<String, DatasetField> fields;
    public Map<String, DatasetField> labels;
    @Nullable public Map<String,String> output_mapping;

    public static class Files {
        public Path train;
        public Path test;
        public Path output;
    }
}

public class DatasetConfig {
    private final DatasetConfigYaml config;
    private final String name;

    public DatasetConfig(DatasetConfigYaml config, @Nullable String name) {
        this.config = config;
        this.name   = name;
        this.validateConfig(config);
    }
    void validateConfig(DatasetConfigYaml config) {
        if( config.id == null || config.id.isEmpty() ) {
            throw new InvalidConfigurationException(this.name + " | 'id' is required");
        }
        if( config.type == DatasetType.elasticsearch ) {
            if( config.index == null ) { throw new InvalidConfigurationException(this.name + " | type:elasticsearch requires 'index' field"); }
            if( config.files != null ) { throw new InvalidConfigurationException(this.name + " | type:elasticsearch must not have 'files' field"); }
        }
        if( config.type == DatasetType.csv ) {
            if( config.index != null ) { throw new InvalidConfigurationException(this.name + " | type:csv must not have 'index' field"); }
            if( config.files == null ) { throw new InvalidConfigurationException(this.name + " | type:csv requires 'files' field"); }
        }
        this.validateOutputMapping();
    }
    void validateOutputMapping() {
        if( config.output_mapping != null ) {
            Set<String> validFieldNames = Sets.union(
                Set.of(this.config.id),
                Sets.union(
                    this.config.fields.keySet(),
                    this.config.labels.keySet()
                )
            );
            Set<String> proposedFieldNames  = Set.copyOf(config.output_mapping.values());
            Set<String> invalidOutputFields = Sets.difference(proposedFieldNames, validFieldNames);
            if( !invalidOutputFields.isEmpty() ) {
                throw new InvalidConfigurationException(this.name + " | output_mapping contains invalid fields: " + invalidOutputFields.toString());
            }
        }
    }

    public String                    getName()    { return name; }
    public DatasetType getType()                  { return config.type; }
    public String                    getIndex()   { return config.index; }
    public DatasetConfigYaml.Files   getFiles()   { return config.files; }
    public String getIdField()                    { return config.id; }
    public Map<String, DatasetField> getFields()  { return config.fields; }
    public Map<String, DatasetField> getLabels()  { return config.labels; }
    public Map<String, String> getOutputMapping() { return config.output_mapping; }

    public List<String> getIdFieldLabels() {
        ArrayList<String> fields = new ArrayList<>();
        fields.add(    this.getIdField() );
        fields.addAll( this.getFields().keySet() );
        fields.addAll( this.getLabels().keySet() );
        return fields;
    }
}

