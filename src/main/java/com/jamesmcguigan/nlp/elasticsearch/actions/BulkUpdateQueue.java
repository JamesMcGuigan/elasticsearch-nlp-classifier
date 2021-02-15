package com.jamesmcguigan.nlp.elasticsearch.actions;

import com.google.gson.Gson;
import com.jamesmcguigan.nlp.elasticsearch.ESClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;


public class BulkUpdateQueue implements UpdateQueue {
    private static final Logger logger = LogManager.getLogger();

    private final int batchSize           = 1000;
    private final int maxRequestsInFlight = 2;  // Keep this low
    private final int maxRetries          = 5;
    private final int flushSeconds        = 5;

    private final BulkProcessor.Listener listener;
    private final BulkProcessor bulkProcessor;
    private final ESClient      client;
    private final String        index;


    public BulkUpdateQueue(String index) throws IOException {
        this.index         = index;
        this.client        = ESClient.getInstance();
        this.listener      = this.getListener();
        this.bulkProcessor = this.getBuilder(this.listener).build();
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
            public void beforeBulk(long executionId, BulkRequest request) {
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
        // TODO: Handle IndexRequest / DeleteRequest
        Map<String, Map<String, Object>> requestMap = bulkRequest.requests().stream()
            .filter(item -> item instanceof UpdateRequest)
            .collect(Collectors.toMap(
                DocWriteRequest::id,
                item -> ((UpdateRequest) item).doc().sourceAsMap()
            ))
        ;
        for( BulkItemResponse bulkItemResponse : bulkResponse ) {
            DocWriteResponse response = bulkItemResponse.getResponse();  // instanceof UpdateResponse
            String id     = response.getId();
            String source = requestMap.getOrDefault(id, new HashMap<>()).toString();
            String action = response.getResult().toString();  // "UPDATE"

            var message = source;
            if( bulkItemResponse.isFailed() ) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                message += " | " + failure.getCause() + " | " + failure.getMessage();
            }
            logger.trace("{} {}({}) | {}", action, BulkUpdateQueue.this.index, id, message);
        }
    }



    @Override
    public void update(String id, Map<String, Object> updateKeyValues) {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.11/java-rest-high-document-update.html
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
