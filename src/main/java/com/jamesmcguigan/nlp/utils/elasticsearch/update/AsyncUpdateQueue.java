package com.jamesmcguigan.nlp.utils.elasticsearch.update;

import com.jamesmcguigan.nlp.utils.elasticsearch.ESClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;
import java.util.Map;

import static org.apache.logging.log4j.Level.TRACE;


public class AsyncUpdateQueue implements UpdateQueue {
    private static final Logger logger = LogManager.getLogger();

    private int requestsInFlight    = 0;
    private int minRequestsInFlight = 5;
    private int maxRequestsInFlight = 25;

    private final String index;


    public AsyncUpdateQueue(String index) {
        this.index = index;
    }

    @SuppressWarnings("unchecked")
    public <T extends AsyncUpdateQueue> T setMinRequestsInFlight(int minRequestsInFlight) {
        this.minRequestsInFlight = minRequestsInFlight;
        return (T) this;
    }
    @SuppressWarnings("unchecked")
    public <T extends AsyncUpdateQueue> T setMaxRequestsInFlight(int maxRequestsInFlight) {
        this.maxRequestsInFlight = maxRequestsInFlight;
        return (T) this;
    }



    @Override
    public void update(String id, Map<String, Object> updateKeyValues) throws IOException {
        this.waitForQueue();

        this.requestsInFlight += 1;
        var request = new UpdateRequest(this.index, id).doc(updateKeyValues);
        ESClient.getInstance().updateAsync(
            request,
            RequestOptions.DEFAULT,
            new ActionListener<>() {
                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    AsyncUpdateQueue.this.requestsInFlight -= 1;

                    var result = updateResponse.getResult();
                    logger.info("{} {}({}) | {}", result, index, id, updateKeyValues);
                }
                @Override
                public void onFailure(Exception e) {
                    AsyncUpdateQueue.this.requestsInFlight -= 1;
                    boolean retry  = false;
                    String action  = "ERROR";
                    String message = e.toString();

                    // ERROR: Concurrent request limit exceeded. Please consider batching your requests
                    // WORKAROUND: Retry and reduce maxRequestsInFlight
                    if( e instanceof ElasticsearchStatusException ) {
                        var exception = (ElasticsearchStatusException) e;
                        if( "TOO_MANY_REQUESTS".equals(exception.status().toString()) ) {
                            AsyncUpdateQueue.this.maxRequestsInFlight *= 0.9;  // race condition is desirable here
                            retry   = true;
                            action  = exception.status().toString();
                            message = "reducing maxRequestsInFlight = " + AsyncUpdateQueue.this.maxRequestsInFlight;
                        }
                    }

                    logger.printf(TRACE, "%s %s(%s) | %s", action, index, id, message);
                    if( retry ) {
                        try {
                            AsyncUpdateQueue.this.update(id, updateKeyValues);
                        } catch( IOException ioException ) {
                            logger.debug(ioException);
                        }
                    }
                }
            }
        );
    }

    @SuppressWarnings("BusyWait")
    private void waitForQueue() {
        while( this.requestsInFlight > Math.max(this.maxRequestsInFlight, this.minRequestsInFlight) ) {
            try {
                Thread.sleep(10);
            } catch( InterruptedException e ) {
                Thread.currentThread().interrupt();
            }
        }
    }


    public void close() {
        // Do Nothing
    }
}
