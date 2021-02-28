package com.jamesmcguigan.nlp.v2.exceptions;

public class ElasticsearchConnectionException extends RuntimeException {
    public ElasticsearchConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
