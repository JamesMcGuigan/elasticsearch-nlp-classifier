package com.jamesmcguigan.nlp.v2.datasets;

import com.jamesmcguigan.nlp.v2.config.DatasetConfig;

import java.util.Map;

public class DataRow {
    private final Map<String, Object> data;
    private final DatasetConfig config;

    public DataRow(Map<String, Object> data, DatasetConfig config) {
        this.data   = data;
        this.config = config;
    }

    public Map<String, Object> toMap() { return this.data;   }
    public DatasetConfig getConfig()   { return this.config; }
}
