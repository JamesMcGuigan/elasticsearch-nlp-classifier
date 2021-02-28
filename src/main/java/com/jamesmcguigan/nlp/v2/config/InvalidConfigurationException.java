package com.jamesmcguigan.nlp.v2.config;

class InvalidConfigurationException extends RuntimeException {
    InvalidConfigurationException(String message) {
        super(message);
    }
    InvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
