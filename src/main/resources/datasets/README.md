# Dataset Specifications

NOTE: draft specifications have not yet been implemented
```
<name>:
    type: elasticsearch | csv | tsv | json
    source:  # elasticsearch
        index:  URL 
    source:  # csv | tsv | json
        train:  filename
        test:   filename
        output: filename
    fields:
        <field>: binary | numeric | text | categorical | list_categorical 
    labels:
        <field>: binary | numeric | text | categorical | list_categorical 
    output_mapping:
        <target_field>: <source_field>
```
