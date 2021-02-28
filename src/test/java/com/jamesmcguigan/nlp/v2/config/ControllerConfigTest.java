package com.jamesmcguigan.nlp.v2.config;

import com.jamesmcguigan.nlp.v2.extractor.Extractor;
import com.jamesmcguigan.nlp.v2.extractor.Pipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;


@SuppressWarnings("FieldCanBeLocal")
class ControllerConfigTest {
    private ControllerConfig config;
    private Path path;
    private String expectedContext;
    private Class<?> expectedClass;
    private String expectedExtractorName;

    @BeforeEach
    void setUp() {
        path    = Paths.get("src/test/resources/ExtractorConfigTest.yaml");
        config  = YamlParser.getExtractorConfigs(path).get(0);
        expectedContext       = "test";
        expectedExtractorName = "com.jamesmcguigan.nlp.v2.extractor.Pipeline";
        expectedClass         = Pipeline.class;
    }

    @Test
    void can_read_name() {
        String name = config.getName();
        assertThat(name).isEqualTo("pipeline-config-test");
    }

    @Test
    void can_read_context() {
        assertThat(config.getContext()).isEqualTo(this.expectedContext);
    }

    @Test
    void can_read_extractor_name() {
        assertThat(config.getExtractorName()).isEqualTo(this.expectedExtractorName);
    }

    @Test
    void can_load_extractor() {
        assertThat(config.getExtractor()).isInstanceOf(this.expectedClass);
        assertThat(config.getExtractor().getClass().getCanonicalName()).isEqualTo(this.expectedExtractorName);
    }

    @Test
    void can_passthrough_config() {
        Extractor extractor                 = config.getExtractor();
        Map<String, Object> childConfig     = config.getConfiguration();
        Map<String, Object> extractorConfig = extractor.getConfiguration();
        assertThat(extractorConfig).isEqualTo(childConfig);
    }

    @Test
    void can_passthrough_context() {
        assertThat(config.getExtractor().getContext()).isEqualTo(this.expectedContext);
    }
}
