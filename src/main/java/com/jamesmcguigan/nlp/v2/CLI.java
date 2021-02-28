package com.jamesmcguigan.nlp.v2;

import com.jamesmcguigan.nlp.v2.config.ControllerConfig;
import com.jamesmcguigan.nlp.v2.config.YamlParser;
import com.jamesmcguigan.nlp.v2.controller.Controller;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
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
    private Path path;

    @CommandLine.Option(names={"-v", "--verbose"}, description="Verbose Logging")
    private boolean verbose = false;

    @CommandLine.Option(names={"-q", "--quiet"}, description="Quiet Logging")
    private boolean quiet = false;


    public static void main(String[] args) {
        int exitCode = new CommandLine(new CLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        if( this.verbose ) { this.setVerbose(); }
        if( this.quiet   ) { this.setQuiet();   }
        if( !this.path.toFile().exists()  ) {
            logger.error("path {} does not exist", this.path);
            return 126;  // unix exitcode 126 = Command invoked cannot execute
        }
        try {
            this.parseConfig();
            return 0;    // unix exitcode 0 = success
        } catch( Exception e ) {
            logger.error(e);
            e.printStackTrace();
            return 1;    // unix exitcode 1 = Catchall for general errors
        }
    }

    public void setQuiet()   { Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.ERROR); }
    public void setVerbose() { Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG); }

    private void parseConfig() {
        List<ControllerConfig> configs = YamlParser.getExtractorConfigs(this.path);
        for( ControllerConfig config : configs ) {
            Controller controller = new Controller(config);
            controller.run();
        }
    }
}
