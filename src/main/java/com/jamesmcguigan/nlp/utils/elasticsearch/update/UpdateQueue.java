package com.jamesmcguigan.nlp.utils.elasticsearch.update;

import java.io.Closeable;
import java.util.Map;

public interface UpdateQueue extends Closeable {

    default void update(String id, String updateKey, String value) {
        this.update(id, Map.of(updateKey,value));
    }

    void update(String id, Map<String, Object> updateKeyValues);
}
