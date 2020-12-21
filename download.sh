#!/usr/bin/env bash
# Source: https://www.kaggle.com/c/nlp-getting-started/data

mkdir -p input/
kaggle c download nlp-getting-started -p input/
unzip input/nlp-getting-started.zip -d input/
rm -f input/nlp-getting-started.zip
