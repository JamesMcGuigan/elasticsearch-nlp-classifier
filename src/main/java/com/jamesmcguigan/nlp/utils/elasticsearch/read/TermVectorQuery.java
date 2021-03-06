package com.jamesmcguigan.nlp.utils.elasticsearch.read;

import com.github.underscore.lodash.U;
import com.jamesmcguigan.nlp.utils.elasticsearch.ESClient;
import org.apache.http.ConnectionClosedException;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility wrapper for making ElasticSearch _mtermvectors requests
 */
@SuppressWarnings("unchecked")
public class TermVectorQuery {
    private static final Logger logger = LogManager.getLogger();
    private static final int retries = 5;  // number of times to retry connection before throwing exception

    private final String       index;
    private final List<String> fields;
    private boolean termStatistics = true;
    private boolean offsets        = true;
    private boolean payloads       = true;
    private boolean positions      = true;

    private final ESClient client  = ESClient.getInstance();


    public TermVectorQuery(String index, List<String> fields) {
        if( fields == null || fields.isEmpty() ) { throw new AssertionError("_mtermvectors returns empty results if no fields are specified"); }

        this.index  = index;
        this.fields = new ArrayList<>(fields);
    }
    public <T extends TermVectorQuery> T setTermStatistics(boolean termStatistics) { this.termStatistics = termStatistics; return (T) this; }
    public <T extends TermVectorQuery> T setOffsets(boolean offsets)     { this.offsets = offsets;     return (T) this; }
    public <T extends TermVectorQuery> T setPayloads(boolean payloads)   { this.payloads = payloads;   return (T) this; }
    public <T extends TermVectorQuery> T setPositions(boolean positions) { this.positions = positions; return (T) this; }


    public List<TermVectorsResponse> getMultiTermVectors(List<String> ids) throws IOException {
        if( ids.isEmpty() ) { return new ArrayList<>(); }
        String requestJson  = this.getMultiTermVectorsRequestJson(ids);
        String responseJson = this.getMultiTermVectorsResponseJson(requestJson);
        return this.castTermVectorsResponse(responseJson);
    }

    
    public String getMultiTermVectorsRequestJson(List<String> ids) {
        // WORKAROUND: client.mtermvectors(new MultiTermVectorsRequest()) on Bonsai throws 400 Bad Request
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-termvectors.html
        // DOCS: https://github.com/javadev/underscore-java
        String requestJson = U.objectBuilder()
            .add("docs", U.reduce(ids, (arrayBuilder, id) -> arrayBuilder.add(U.objectBuilder()
                    .add("_id",       id)
                    .add("fields",    this.fields)
                    .add("offsets",   this.offsets)
                    .add("payloads",  this.payloads)
                    .add("positions", this.positions)
                    .add("term_statistics", this.termStatistics)
                ), U.arrayBuilder())
            ).toJson()
        ;
        return requestJson;
    }


    public Request getMultiTermVectorsRequest(String requestJson) {
        Request request = new Request("POST", "/"+this.index+"/_mtermvectors");
        request.setJsonEntity(requestJson);
        return request;
    }


    protected String getMultiTermVectorsResponseJson(String requestJson) throws IOException {
        // DOCS: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.11/java-rest-high-document-multi-term-vectors.html
        // If we get a ConnectionClosedException then try again - happens intermittently
        IOException exception = new IOException("retry limit exceeded");
        for( int i = 0; i < retries; i++ ) {
            try {
                Request request     = this.getMultiTermVectorsRequest(requestJson);
                Response response   = client.getLowLevelClient().performRequest(request);
                String responseJson = EntityUtils.toString(response.getEntity());
                return responseJson;
            } catch( ConnectionClosedException e ) {
                logger.debug(e.getCause());
                exception = e;
            }
        }
        throw exception;
    }


    public List<TermVectorsResponse> castTermVectorsResponse(String responseJson) throws IOException {
        try(
            // Manually generate MultiTermVectorsResponse() from JSON to reuse ES object model
            XContentParser xContentParser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseJson
            )
        ) {
            // Cast back to the ElasticSearch object model
            List<TermVectorsResponse> termVectorsResponses = MultiTermVectorsResponse
                .fromXContent(xContentParser)
                .getTermVectorsResponses()
            ;
            return termVectorsResponses;
        }
    }
}
