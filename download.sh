#!/usr/bin/env bash
# Source: https://www.kaggle.com/c/nlp-getting-started/data

mkdir -p input/
kaggle c download nlp-getting-started -p input/
unzip input/nlp-getting-started.zip -d input/
unzip input/nlp-getting-started.zip -d input/nlp-getting-started/  # v1 + v2 assume different paths
rm -f input/nlp-getting-started.zip
