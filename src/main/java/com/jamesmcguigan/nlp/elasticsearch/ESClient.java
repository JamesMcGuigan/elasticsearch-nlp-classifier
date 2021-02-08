package com.jamesmcguigan.nlp.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

public class ESClient {

    public static RestHighLevelClient client;
    static { try { client = connect(); } catch( IOException e ) { e.printStackTrace(); } }

    public ESClient() {
    }

    public static RestHighLevelClient connect() throws IOException {
        Properties properties = new Properties();
        properties.load(
            MethodHandles.lookup().lookupClass().getClassLoader()
                .getResourceAsStream("elasticsearch.properties")
        );

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
            .setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        httpClientBuilder.disableAuthCaching();
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }
            )
            .setRequestConfigCallback(
                new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder
                            .setConnectTimeout(Integer.parseInt(
                                properties.getProperty("es.settings.connectTimeout")
                            ))
                            .setSocketTimeout(Integer.parseInt(
                                properties.getProperty("es.settings.socketTimeout" )
                            ))
                        ;
                    }
                }
            )
            .setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    System.out.print(node.toString());
                }
            })
        ;

        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.10/java-rest-high.html
        RestHighLevelClient client = new RestHighLevelClient(builder);

        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/_usage.html
        // Sniffer sniffer = Sniffer.builder(client.getLowLevelClient()).build();

        return client;
    }


}
