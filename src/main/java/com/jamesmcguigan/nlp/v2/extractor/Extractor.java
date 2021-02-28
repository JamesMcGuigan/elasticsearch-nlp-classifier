package com.jamesmcguigan.nlp.v2.extractor;

import java.util.Map;

public interface Extractor {
    String getContext();
    Map<String, Object> getConfiguration();
}
