package com.jamesmcguigan.kdt.data;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Entry {

    public final int id;
    public final String keyword;
    public final String location;
    public final String text;
    public final Boolean target;


    public Entry(int id, String keyword, String location, String text, Boolean target) {
        this.id       = id;
        this.keyword  = keyword;
        this.location = location;
        this.text     = text;
        this.target   = target;
    }

    public String toString() {
        return String.format("%s(%s, %s, %s, %s, %s)",
                this.getClass().getSimpleName(),
                this.id,
                this.keyword,
                this.location,
                this.text,
                this.target
        );
    }

    public List<String> tokenized() {
        return Stream.of(
                this.keyword,
                this.location,
                this.text
            )
            .map(StringUtils::splitPreserveAllTokens)
            .map(Arrays::asList)
            .flatMap(List<String>::stream)
            .collect(Collectors.toList())
        ;
    }
}
