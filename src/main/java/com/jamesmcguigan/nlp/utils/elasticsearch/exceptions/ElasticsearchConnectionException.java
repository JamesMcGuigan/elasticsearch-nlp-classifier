package com.jamesmcguigan.nlp.utils.elasticsearch.exceptions;

public class ElasticsearchConnectionException extends RuntimeException {
    public ElasticsearchConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
