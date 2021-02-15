package com.jamesmcguigan.nlp.elasticsearch.actions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface UpdateQueue extends Closeable {

    default void update(String id, String updateKey, String value) throws IOException {
        this.update(id, Map.of(updateKey,value));
    }

    void update(String id, Map<String, Object> updateKeyValues) throws IOException;
}
