package com.jamesmcguigan.nlp.v2.config;

import com.jamesmcguigan.nlp.v2.exceptions.InvalidConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;


@SuppressWarnings({"java:S5976"})
class DatasetConfigTest {

    @Test
    void invalid_type() {
        String yaml = """
            invalid-config:
                type: invalid
        """;
        assertThrows(InvalidConfigurationException.class, () -> YamlParser.getDatasetConfigs(yaml) );
    }

    @Test
    void empty_id() {
        String yaml = """
            invalid-config:
                type:  elasticsearch
                index: twitter
                id:    ""
        """;
        assertThrows(InvalidConfigurationException.class, () -> YamlParser.getDatasetConfigs(yaml) );
    }

    @Test
    void type_elasticsearch_missing_index() {
        String yaml = """
            invalid-config:
                type: elasticsearch
        """;
        assertThrows(InvalidConfigurationException.class, () -> YamlParser.getDatasetConfigs(yaml) );
    }

    @Test
    void type_elasticsearch_with_index() {
        String yaml = """
            valid-elasticsearch-config:
                type:  elasticsearch
                index: twitter
        """;
        List<DatasetConfig> configs = YamlParser.getDatasetConfigs(yaml);
        DatasetConfig config = configs.get(0);
        assertThat(configs.size()).isEqualTo(1);
        assertThat(config.getName()).isEqualTo("valid-elasticsearch-config");
        assertThat(config.getType()).isEqualTo(DatasetType.elasticsearch);
        assertThat(config.getIndex()).isEqualTo("twitter");
    }

    @Test
    void type_csv_missing_files() {
        String yaml = """
            invalid-csv-config:
                type: csv
        """;
        assertThrows(InvalidConfigurationException.class, () -> YamlParser.getDatasetConfigs(yaml) );
    }

    @Test
    void type_csv_with_files() {
        String yaml = """
            valid-csv-config:
                type:  csv
                files:
                    train:  input/nlp-getting-started/train.csv
                    test:   input/nlp-getting-started/test.csv
                    output: output/nlp-getting-started.csv
            """;
        List<DatasetConfig> configs = YamlParser.getDatasetConfigs(yaml);
        DatasetConfig config = configs.get(0);
        assertThat(configs.size()).isEqualTo(1);
        assertThat(config.getName()).isEqualTo("valid-csv-config");
        assertThat(config.getType()).isEqualTo(DatasetType.csv);
        assertThat(config.getFiles()).isNotNull();
        assertThat(config.getFiles().train.toString() ).isEqualTo(Path.of("input/nlp-getting-started/train.csv").toString());
        assertThat(config.getFiles().test.toString()  ).isEqualTo(Path.of("input/nlp-getting-started/test.csv" ).toString());
        assertThat(config.getFiles().output.toString()).isEqualTo(Path.of("output/nlp-getting-started.csv"     ).toString());
    }

    @Test
    void type_csv_invalid_output_mapping() {
        String yaml = """
            invalid-csv-config:
                type: csv
                files:
                    train:  input/nlp-getting-started/train.csv
                    test:   input/nlp-getting-started/test.csv
                    output: output/nlp-getting-started.csv
                fields:
                    text: string
                labels:
                    target: string
                output_mapping:
                    predict: unknown
        """;
        assertThrows(InvalidConfigurationException.class, () -> YamlParser.getDatasetConfigs(yaml) );
    }
    @Test
    void type_csv_valid_output_mapping() {
        String yaml = """
            valid-csv-config:
                type: csv
                files:
                    train:  input/nlp-getting-started/train.csv
                    test:   input/nlp-getting-started/test.csv
                    output: output/nlp-getting-started.csv
                # id: "id"  # is implicit
                fields:
                    text: text
                labels:
                    target: binary
                output_mapping:
                    Id:     id
                    Text:   text
                    Target: target
        """;
        List<DatasetConfig> configs = YamlParser.getDatasetConfigs(yaml);
        DatasetConfig config = configs.get(0);
        assertThat(configs.size()).isEqualTo(1);
        assertThat( config.getIdField() ).isEqualTo("id");
        assertThat( config.getFields().get("text").toString() ).isEqualTo("text");
        assertThat( config.getLabels().get("target").toString() ).isEqualTo("binary");
    }



    private DatasetConfig config;
    private Path path;

    @Nested
    class CSV {
        @BeforeEach
        void setUp() {
            path   = Paths.get("src/test/resources/nlp-getting-started-csv.yaml");
            config = YamlParser.getDatasetConfigs(path).get(0);
        }

        @Test
        void can_load_yaml() {
            assertThat(config).isNotNull();
            assertThat(config).isInstanceOf(DatasetConfig.class);
        }
    }

    @Nested
    class ElasticSearch {
        @BeforeEach
        void setUp() {
            path   = Paths.get("src/test/resources/nlp-getting-started-elasticsearch.yaml");
            config = YamlParser.getDatasetConfigs(path).get(0);
        }

        @Test
        void can_load_yaml() {
            assertThat(config).isNotNull();
            assertThat(config).isInstanceOf(DatasetConfig.class);
        }
    }

}
