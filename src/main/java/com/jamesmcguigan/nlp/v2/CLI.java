package com.jamesmcguigan.nlp.v2;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import picocli.CommandLine;

import java.io.File;
import java.util.concurrent.Callable;

// Usage:
// java -cp target/classes:`cat classpath` com.jamesmcguigan.nlp.v2.CLI
// mvn package -q; java -jar target/elasticsearch-nlp-classifier-1.0-SNAPSHOT.jar -c config/pipelines/elasticsearch/0_pipeline_elasticsearch.yaml
// mvn compile exec:java -q -Dexec.args="-c config/pipelines/elasticsearch/0_pipeline_elasticsearch.yaml"
@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal", "unused"})
@CommandLine.Command(
    name = "Enricher",
    // mixinStandardHelpOptions = true,
    version = "v0.1",
    description = "NLP Enricher"
)
public class CLI implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CLI.class);

    @CommandLine.Option(names={"-c", "--config"}, required=true, description="YAML configuration file")
    private File config;

    @CommandLine.Option(names={"-v", "--verbose"}, description="Verbose Logging")
    private boolean verbose = false;

    @CommandLine.Option(names={"-q", "--quiet"}, description="Quiet Logging")
    private boolean quiet = false;


    public void setQuiet()   { Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.ERROR); }
    public void setVerbose() {
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
    }


    @Override
    public Integer call() {
        if( this.verbose ) { this.setVerbose(); }
        if( this.quiet   ) { this.setQuiet();   }
        logger.info(this.config.getAbsolutePath());
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CLI()).execute(args);
        System.exit(exitCode);
    }

}
