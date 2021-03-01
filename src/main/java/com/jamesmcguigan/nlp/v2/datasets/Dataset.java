package com.jamesmcguigan.nlp.v2.datasets;

import java.util.List;
import java.util.stream.Stream;

public interface Dataset {
    Stream<DataRow> getTrainStream();
    Stream<DataRow> getTestStream();
    List<Stream<DataRow>> getKFoldStreams(int folds);
}
