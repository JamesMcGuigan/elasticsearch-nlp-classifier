package com.jamesmcguigan.nlp.v2.datasets;

import com.jamesmcguigan.nlp.v2.config.DatasetConfig;

import java.util.List;
import java.util.stream.Collectors;

public class Datasets {
    private Datasets() {}

    public static List<Dataset> from(List<DatasetConfig> datasetConfigs, String condition) {
        return datasetConfigs.stream()
            .map(datasetConfig -> Datasets.from(datasetConfig, condition))
            .collect(Collectors.toList())
        ;
    }
    public static Dataset from(DatasetConfig datasetConfig, String condition) {
        return switch( datasetConfig.getType() ) {
            case elasticsearch -> new ElasticsearchDataset(datasetConfig, condition);
            case csv           -> new CSVDataset(datasetConfig, condition);
        };
    }
}
