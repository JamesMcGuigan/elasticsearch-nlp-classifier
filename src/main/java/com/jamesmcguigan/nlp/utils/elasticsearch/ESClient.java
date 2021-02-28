package com.jamesmcguigan.nlp.utils.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;

import java.io.IOException;
import java.util.Properties;

public final class ESClient extends RestHighLevelClient {
    private static final Logger logger = LogManager.getLogger();
    private static final boolean useSniffer = false;  // Sniffer breaks HTTPS connectivity with Bonsai
    private Sniffer sniffer;



    //***** Thread-safe Singleton *****//

    private static ESClient instance;
    public static synchronized ESClient getInstance() throws IOException {
        // BUGFIX: reopen connection if `.close()` has been called to early
        if( instance == null || !instance.getLowLevelClient().isRunning() ) {
            instance = new ESClient();
        }
        return instance;
    }



    //***** Constructor *****//

    private ESClient() throws IOException {
        super( getBuilder() );
        this.loadSniffer();
        this.registerShutdownHook();
    }

    private static Properties getProperties() throws IOException {
        String propertiesFile = "elasticsearch.properties";
        Properties properties = new Properties();
        properties.load(
            Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(propertiesFile)
        );
        return properties;
    }

    private static RestClientBuilder getBuilder() throws IOException {
        Properties properties = getProperties();

        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high.html
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_basic_authentication.html
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(
                properties.getProperty("es.username"),
                properties.getProperty("es.password")
            )
        );

        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-initialization.html
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(
                properties.getProperty("es.hostname"),
                Integer.parseInt( properties.getProperty("es.port") ),
                properties.getProperty("es.scheme")
            )
        )
            .setHttpClientConfigCallback((HttpAsyncClientBuilder httpClientBuilder) -> {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            })
            .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(Integer.parseInt(
                    properties.getProperty("es.settings.connectTimeout")
                ))
                .setSocketTimeout(Integer.parseInt(
                    properties.getProperty("es.settings.socketTimeout" )
                ))
            )
            .setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    logger.error("setFailureListener() {}", node);
                }
            })
        ;
        return builder;
    }
    private void loadSniffer() {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/_usage.html
        // BUG:  Sniffer breaks HTTPS connectivity with Bonsai
        if( useSniffer ) {
            this.sniffer = Sniffer.builder(this.getLowLevelClient()).build();
        }
    }
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if( ESClient.this.sniffer != null ) {
                    ESClient.this.sniffer.close();
                }
                ESClient.this.close();
            } catch( IOException e ) {
                logger.error(e);
            }
        }, "Shutdown-thread"));
    }


}
