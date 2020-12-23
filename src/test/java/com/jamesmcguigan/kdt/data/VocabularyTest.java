package com.jamesmcguigan.kdt.data;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class VocabularyTest {
    @Test
    void testFromFiles() {
        Vocabulary train = Vocabulary.fromFiles("input/train.csv");
        Vocabulary test  = Vocabulary.fromFiles("input/test.csv");
        Vocabulary both  = Vocabulary.fromFiles("input/train.csv", "input/test.csv");

        assertTrue( train.size() > 0 );
        assertTrue( test.size() > 0  );
        assertTrue( both.size() > 0  );
        assertTrue( both.size() >= test.size()  );
        assertTrue( both.size() >= train.size() );
    }
}
