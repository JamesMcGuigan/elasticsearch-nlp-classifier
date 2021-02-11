package com.jamesmcguigan.nlp.elasticsearch.actions;

import java.io.IOException;
import java.util.Map;

public interface UpdateQueue {

    default void add(String id, String updateKey, String value) throws IOException {
        this.add(id, Map.of(updateKey,value));
    }

    void add(String id, Map<Object, Object> updateKeyValues) throws IOException;
}
