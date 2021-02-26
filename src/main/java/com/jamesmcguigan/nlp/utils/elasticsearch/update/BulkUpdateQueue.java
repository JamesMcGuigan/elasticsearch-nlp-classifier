package com.jamesmcguigan.nlp.utils.elasticsearch.update;

import com.google.gson.Gson;
import com.jamesmcguigan.nlp.utils.elasticsearch.ESClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;


public class BulkUpdateQueue implements UpdateQueue {
    private static final Logger logger = LogManager.getLogger();

    private int batchSize           = 1000;
    private int maxRequestsInFlight = 2;  // Keep this low
    private int maxRetries          = 5;
    private int flushSeconds        = 5;

    private final BulkProcessor.Listener listener;
    private final BulkProcessor bulkProcessor;
    private final ESClient      client;
    private final String        index;


    //***** Constructor *****//

    public BulkUpdateQueue(String index) throws IOException {
        this.index         = index;
        this.client        = ESClient.getInstance();
        this.listener      = this.getListener();
        this.bulkProcessor = this.getBuilder(this.listener).build();
    }
    public BulkUpdateQueue(String index, int batchSize, int maxRequestsInFlight, int maxRetries, int flushSeconds) throws IOException {
        this.index               = index;
        this.batchSize           = batchSize;
        this.maxRequestsInFlight = maxRequestsInFlight;
        this.maxRetries          = maxRetries;
        this.flushSeconds        = flushSeconds;

        this.client              = ESClient.getInstance();
        this.listener            = this.getListener();
        this.bulkProcessor       = this.getBuilder(this.listener).build();
    }

    private BulkProcessor.Builder getBuilder(BulkProcessor.Listener listener) {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-bulk.html
        BulkProcessor.Builder builder = BulkProcessor.builder(
            (request, bulkListener) -> this.client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
            listener
        );
        builder.setBulkActions(this.batchSize);                         // default = 1000
        builder.setBulkSize(new ByteSizeValue(5L, MB));            // default = 5Mb
        builder.setConcurrentRequests(this.maxRequestsInFlight);        // default = 0
        builder.setFlushInterval(timeValueSeconds(this.flushSeconds));  // default = 0
        builder.setBackoffPolicy(exponentialBackoff(timeValueSeconds(1L), maxRetries)); // default = 1s * 3
        return builder;
    }

    private BulkProcessor.Listener getListener() {
        return new BulkProcessor.Listener() {
            /**
             * Callback before the bulk is executed.
             */
            @Override
            public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                // Do Nothing
            }

            /**
             * Callback after a failed execution of bulk request.
             *
             * Note that in case an instance of <code>InterruptedException</code> is passed, which means that request processing has been
             * cancelled externally, the thread's interruption status has been restored prior to calling this method.
             */
            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable failure) {
                // intermittent BUG: Bulk Request FAILURE: org.apache.http.ConnectionClosedException: Connection is closed
                logger.error("Bulk Request FAILURE: {} | {}", BulkUpdateQueue.this.index, failure);

                // Add any failed requests back onto the queue after ConnectionClosedException
                BulkUpdateQueue.this.update(bulkRequest);
            }

            /**
             * Callback after a successful execution of bulk request.
             */
            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                BulkUpdateQueue.this.logBulkResponse(bulkRequest, bulkResponse);
            }
        };
    }

    protected void logBulkResponse(BulkRequest bulkRequest, BulkResponse bulkResponse) {
        Level level = Level.INFO;
        if( !logger.isEnabled(level) ) { return; }
        for( int i = 0; i < Math.min(bulkRequest.requests().size(), bulkResponse.getItems().length); i++ ) {
            var request = bulkRequest.requests().get(i);
            var response  = bulkResponse.getItems()[i];
            String id = response.getId();
            String source = this.requestToString(request);
            String action = response.getResponse().getResult().toString();  // "UPDATE"
            var message = source;
            if( response.isFailed() ) {
                BulkItemResponse.Failure failure = response.getFailure();
                message += " | " + failure.getCause() + " | " + failure.getMessage();
            }
            logger.log(level, "{} {}({}) | {}", action, BulkUpdateQueue.this.index, id, message);
        }
    }
    protected String requestToString(DocWriteRequest<?> request) {
        if( request instanceof UpdateRequest ) { return ((UpdateRequest) request).doc().sourceAsMap().toString(); }
        if( request instanceof IndexRequest  ) { return ((IndexRequest)  request).sourceAsMap().toString();       }
        return request.toString();
    }


    //***** Public Interface *****//

    @Override
    public void update(String id, Map<String, Object> updateKeyValues) {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.11/java-rest-high-document-update.html
        if( updateKeyValues.isEmpty() ) { return; }
        String json           = new Gson().toJson(updateKeyValues);
        UpdateRequest request = new UpdateRequest(this.index, id).doc(json, XContentType.JSON);
        this.bulkProcessor.add(request);  // HighLevelRESTClient is thread-safe
    }

    /**
     * This allows a raw BulkRequest to be re-added to the queue on ConnectionClosedException
     * @param bulkRequest raw BulkRequest object from BulkProcessor.Listener
     */
    public void update(BulkRequest bulkRequest) {
        bulkRequest.requests().stream()
            .filter(item -> item instanceof UpdateRequest)
            .forEach(item -> {
                String id  = item.id();
                Map<String, Object> source = ((UpdateRequest) item).doc().sourceAsMap();
                this.update(id, source);
            })
        ;
    }

    public void close() {
        // intermittent BUG: Bulk Request FAILURE: org.apache.http.ConnectionClosedException: Connection is closed
        try {
            this.bulkProcessor.flush();
            this.bulkProcessor.awaitClose(60L, TimeUnit.SECONDS);
        } catch( InterruptedException e ) {
            Thread.currentThread().interrupt();
        }
    }
}
