# Pipeline Specifications

NOTE: draft specifications have not yet been implemented

Enricher
```
<name>:
  extractor: <java.classpath>
  condition: None | <lucene:query>
  configuration:
    task:    train | statistics | enrich | csv
    context: <string>
    predict:  # default: datasets[].labels || output_mapping
      - <field>
      - <field>
    datasets:
      - <directory/filepath.yaml>
```

Pipeline
```
<label>:
  extractor: com.jamesmcguigan.nlp.enricher.PipelineEnricher
  condition: <lucene:query>
  configuration:
    context: <context>   # inherits
    predict:             # inherits
      - target
      - keyword
    datasets:            # inherits
      - <directory/filepath.yaml>
    tasks:
      - <task>:
        - extractor: <java.classpath>
          # context:  <context>   # implied
          # task:     <task>      # implied
          # predict:  <predict>   # implied
          # datasets: <datasets>  # implied

        - extractor: <java.classpath>
          # context:  <context>   # implied
          # task:     <task>      # implied
          # predict:  <predict>   # implied
          # datasets: <datasets>  # implied
```
