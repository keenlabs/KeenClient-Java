package io.keen.client.java;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.List;

import io.keen.client.java.exceptions.KeenQueryClientException;

import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.OutputSource;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;
import io.keen.client.java.http.UrlConnectionHttpHandler;

import io.keen.client.java.result.Group;
import io.keen.client.java.result.QueryResult;
import io.keen.client.java.result.DoubleResult;
import io.keen.client.java.result.LongResult;
import io.keen.client.java.result.StringResult;
import io.keen.client.java.result.ListResult;
import io.keen.client.java.result.IntervalResult;
import io.keen.client.java.result.GroupByResult;

/**
 * <p>
 * KeenQueryClient provides all of the functionality required to execute the basic queries
 * supported by the Data Analysis API: https://keen.io/docs/data-analysis/
 * </p>
 * <p> This include Count, Count Unique, Sum, Average, Maxiumum, Minimum, Median,
 * Percentile, and Select Unique. It does not include Extractions, Multi-Analysis, and Funnels.</p>
 * */
public class KeenQueryClient {

    private static final String ENCODING = "UTF-8";
    private final KeenJsonHandler jsonHandler;
    private final String baseUrl;
    private final KeenProject project;
    private final HttpHandler httpHandler;

    /**
     * Gets the default project that this {@link KeenQueryClient} is using.
     *
     * @return The {@link KeenProject}.
     */
    public KeenProject getProject() {
        return this.project;
    }

    /**
     * Count query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/#count
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return  the count query response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Long count(String eventCollection, Timeframe timeframe) throws IOException {
        Query queryParams = new Query.Builder(QueryType.COUNT)
                .withEventCollection(eventCollection)
                .withTimeframe(timeframe)
                .build();
        QueryResult result = execute(queryParams);
        return queryResultToLong(result);
    }

    /**
     * Count Unique query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/#count-unique
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The count unique query response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Long countUnique(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException {
        Query queryParams = new Query.Builder(QueryType.COUNT_UNIQUE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withTimeframe(timeframe)
                .build();
        QueryResult result = execute(queryParams);
        return queryResultToLong(result);
    }

    /**
     * Minimum query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#minimum-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The minimum query response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double minimum(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException {
        Query queryParams = new Query.Builder(QueryType.MINIMUM)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withTimeframe(timeframe)
                .build();
        QueryResult result =  execute(queryParams);
        return queryResultToDouble(result);
    }

    /**
     * Maximum query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#maximum-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double maximum(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.Builder(QueryType.MAXIMUM)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withTimeframe(timeframe)
                .build();
        QueryResult result = execute(queryParams);
        return queryResultToDouble(result);
    }

    /**
     * Average query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#average-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The average query response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double average(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.Builder(QueryType.AVERAGE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withTimeframe(timeframe)
                .build();
        QueryResult result = execute(queryParams);
        return queryResultToDouble(result);
    }

    /**
     * Median query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#median-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The median query response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double median(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.Builder(QueryType.MEDIAN)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withTimeframe(timeframe)
                .build();
        QueryResult result =  execute(queryParams);
        return queryResultToDouble(result);
    }

    /**
     * Percentile query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#percentile-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param percentile     The percentile.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The percentile query response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double percentile(String eventCollection, String targetProperty, Double percentile, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.Builder(QueryType.PERCENTILE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withPercentile(percentile)
                .withTimeframe(timeframe)
                .build();
        QueryResult result = execute(queryParams);
        return queryResultToDouble(result);
    }

    /**
     * Sum query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#sum-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The sum response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double sum(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.Builder(QueryType.SUM)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withTimeframe(timeframe)
                .build();
        QueryResult result =  execute(queryParams);
        return queryResultToDouble(result);
    }

    /**
     * Select Unique query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#select-unique-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param timeframe     The {@link RelativeTimeframe} or {@link AbsoluteTimeframe}.
     * @return The select unique query response.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public QueryResult selectUnique(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.Builder(QueryType.SELECT_UNIQUE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withTimeframe(timeframe)
                .build();
        QueryResult result = execute(queryParams);
        return result;
    }

    /**
     * This is the most flexible way to run a query. Use {@link Builder} to
     * build all the query arguments to run the query.
     *
     * @param params     The {@link Query} information, including {@link QueryType}, required args, and any optional args.
     * @return The {@link QueryResult} result.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public QueryResult execute(Query params) throws IOException {

        // check parameters are valid
        if (false == params.areParamsValid()) {
            throw new IllegalArgumentException("Keen Query parameters are insufficient. Please check Query API docs for required arguments.");
        }

        // Construct Query parameter args and URL string.
        Map<String, Object> allQueryArgs = params.constructQueryArgs();
        String urlString = formatBaseURL(params.getQueryType().toString());
        URL url = new URL(urlString);

        // post request and construct QueryResult.
        Object postResult = postRequest(project, url, allQueryArgs);
        QueryResult result = constructQueryResult(postResult, params.hasGroupBy(), params.hasInterval());
        return result;
    }


    private static QueryResult constructQueryResult(Object input, boolean isGroupBy, boolean isInterval) {
        QueryResult result = null;

        // below code determines what type of object QueryResult holds.
        if (input instanceof Integer) {
            Integer intValue = (Integer) input;
            result = new LongResult(intValue.longValue());
        } else if (input instanceof Long) {
            result = new LongResult((Long) input);
        } else if (input instanceof Double) {
            result = new DoubleResult((Double) input);
        } else if (input instanceof String) {
            result = new StringResult((String) input);
        } else if (input instanceof List) {

            // recursively construct the children of this...
            List<Object> listInput = (ArrayList<Object>)input;

            // if this is an IntervalResult, construct the IntervalResult object.
            if (isInterval) {
                result = constructIntervalResult(listInput, isGroupBy);
            } else if (isGroupBy) {
                // if this is a GroupByResult, construct the GroupByResult object.
                // Note that if this is both an Interval and GroupBy, the GroupBy
                // code will be called later from within constructIntervalResult()
                result = constructGroupByResult(listInput);
            } else {

                // else if this is just a List of QueryResult objects - for example,
                // Select Unique query returns a list of unique objects.
                List<QueryResult> listOutput = new ArrayList<QueryResult>();
                for (Object child : listInput) {
                    QueryResult resultItem = constructQueryResult(child, false, false);
                    listOutput.add(resultItem);
                }
                result = new ListResult(listOutput);
            }
        }

        return result;
    }

    private static IntervalResult constructIntervalResult(List<Object> intervals, boolean isGroupBy) {
        Map<AbsoluteTimeframe, QueryResult> intervalResult = new HashMap<AbsoluteTimeframe, QueryResult>();

        for (Object child : intervals) {
            if (child instanceof HashMap) {
                Map<String, Object> inputMap = (HashMap<String, Object>) child;
                // If this is an interval, it should have keys "timeframe" and "value"
                if (inputMap.containsKey(KeenQueryConstants.TIMEFRAME) && (inputMap.containsKey(KeenQueryConstants.VALUE))) {
                    AbsoluteTimeframe absoluteTimeframe = null;
                    Object timeframe = inputMap.get(KeenQueryConstants.TIMEFRAME);
                    if (timeframe instanceof Map) {
                        Map<String, String> hashTimeframe = (HashMap<String, String>) timeframe;
                        String start = hashTimeframe.get(KeenQueryConstants.START);
                        String end = hashTimeframe.get(KeenQueryConstants.END);
                        absoluteTimeframe = new AbsoluteTimeframe(start, end);
                    } else {
                        throw new IllegalStateException();
                    }

                    Object value = inputMap.get(KeenQueryConstants.VALUE);
                    QueryResult queryResultValue = constructQueryResult(value, isGroupBy, false);
                    intervalResult.put(absoluteTimeframe, queryResultValue);
                } else {
                    throw new IllegalStateException();
                }
            } else {
                throw new IllegalStateException();
            }
        }

        return new IntervalResult(intervalResult);
    }

    private static GroupByResult constructGroupByResult(List<Object> groups) {
        Map<Group, QueryResult> groupByResult = new HashMap<Group, QueryResult>();

        for (Object child : groups) {
            if (child instanceof HashMap) {
                Map<String, Object> inputMap = (HashMap<String, Object>) child;

                // If this is a GroupByResult, it should have key "result", along with properties to group by.
                if (inputMap.containsKey(KeenQueryConstants.RESULT)) {
                    QueryResult result = null;
                    Map<String, Object> properties = new HashMap<String, Object>();
                    for (String key : inputMap.keySet()) {
                        if (key.equals(KeenQueryConstants.RESULT)) {
                            // there should not be intervals nested inside GroupByResult's; only
                            // the other way around.
                            result = constructQueryResult(inputMap.get(key), false, false);
                        } else {
                            properties.put(key, inputMap.get(key));
                        }
                    }

                    Group groupBy = new Group(properties);
                    groupByResult.put(groupBy, result);
                } else {
                    throw new IllegalStateException();
                }
            } else {
                throw new IllegalStateException();
            }
        }

        return new GroupByResult(groupByResult);
    }

    /**
     * Posts a request to the server in the specified project, using the given URL and request data.
     * The request data will be serialized into JSON using the client's
     * {@link io.keen.client.java.KeenJsonHandler}.
     *
     * @param project     The project in which the event(s) will be published; this is used to
     *                    determine the read key to use for authentication.
     * @param url         The URL to which the POST should be sent.
     * @param requestData The request data, which will be serialized into JSON and sent in the
     *                    request body.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server.
     */
    private Object postRequest(KeenProject project, URL url,
                               final Map<String, ?> requestData) throws IOException {

        // Build an output source which simply writes the serialized JSON to the output.
        OutputSource source = new OutputSource() {
            @Override
            public void writeTo(OutputStream out) throws IOException {
                OutputStreamWriter writer = new OutputStreamWriter(out, ENCODING);

                // in queries, requestData may be null.
                if (requestData != null && requestData.size() != 0) {
                    jsonHandler.writeJson(writer, requestData);
                }
            }
        };

        // If logging is enabled, log the request being sent.
        if (KeenLogging.isLoggingEnabled()) {
            try {
                StringWriter writer = new StringWriter();
                jsonHandler.writeJson(writer, requestData);
                String request = writer.toString();
                KeenLogging.log(String.format(Locale.US, "Sent request '%s' to URL '%s'",
                        request, url.toString()));
            } catch (IOException e) {
                KeenLogging.log("Couldn't log event written to file: ");
                e.printStackTrace();
            }
        }

        // Send the request.
        String readkey = project.getReadKey();
        Request request = new Request(url, "POST", readkey, source, null);
        Response response = httpHandler.execute(request);

        if (response.isSuccess() == false) {
            throw new ServerException(response.body);
        }

        // Parse the response into a map.
        StringReader reader = new StringReader(response.body);
        Map<String, Object> responseMap;
        responseMap = this.jsonHandler.readJson(reader);

        // Get the result object.
        Object result = responseMap.get(KeenQueryConstants.RESULT);
        // for successful query, we should get a Result object. But just in case we don't...
        if (result == null) {
            String errorCode = responseMap.get(KeenQueryConstants.ERROR_CODE).toString();
            String message = responseMap.get(KeenQueryConstants.MESSAGE).toString();

            String errorMessage = "Error response received from server";
            if (errorCode != null) {errorMessage += " " + errorCode;}
            if (message != null) {errorMessage += ": " + message;}

            throw new KeenQueryClientException(errorMessage);
        }

        return result;
    }

    private String formatBaseURL(String queryName) {
        return String.format(Locale.US, "%s/%s/projects/%s/queries/%s",
                baseUrl,
                KeenConstants.API_VERSION,
                project.getProjectId(),
                queryName        // query name
        );
    }

    private Long queryResultToLong(QueryResult result) throws KeenQueryClientException {
        if (result.isLong()) {
            return result.longValue();
        } else {
            throw new KeenQueryClientException("Count Query Error: expected Long response type.");
        }
    }

    private Double queryResultToDouble(QueryResult result) throws KeenQueryClientException {
        if (result.isDouble()) {
            return result.doubleValue();
        } else if (result.isLong()) {
            return Long.valueOf(result.longValue()).doubleValue();
        } else {
            throw new KeenQueryClientException("Sum Query Error: expected Double response type.");
        }
    }

    /**
     * Constructs a Keen Query client using a builder.
     *
     * @param builder The {@link Builder} from which to retrieve this client's interfaces and settings.
     */
    protected KeenQueryClient(Builder builder) {
        // Initialize final properties using the builder.
        this.httpHandler = builder.httpHandler;
        this.jsonHandler = builder.jsonHandler;
        this.baseUrl = builder.baseUrl;
        this.project = builder.project;
    }

    /**
     * <p>
     * Builder class for instantiating Keen Query clients.
     * </p>
     * <p> This builder defaults to using HttpURLConnection to handle HTTP requests.</p>
     * <p> This builder defaults to using JacksonJsonHandler for JSON handler.</p>
     * <p> This builder defaults to using KeenConstants.SERVER_ADDRESS for base URL.</p>
     *
     * @author claireyoung
     * @since 1.0.0
     */
    public static class Builder {

        private HttpHandler httpHandler;
        private KeenJsonHandler jsonHandler;
        private String baseUrl;
        private KeenProject project;

        /**
         * Builder to create a KeenQueryClient with {@link KeenProject} .
         *
         * @param project The {@link KeenProject} to use.
         */
        public Builder(KeenProject project) {
            this.project = project;
        }

        /**
         * Gets the default {@link HttpHandler} to use if none is explicitly set for this builder.
         * <p/>
         * This implementation returns a handler that will use {@link java.net.HttpURLConnection}
         * to make HTTP requests.
         * <p/>
         * Subclasses should override this to provide an alternative default {@link HttpHandler}.
         *
         * @return The default {@link HttpHandler}.
         * @throws Exception If there is an error creating the {@link HttpHandler}.
         */
        protected HttpHandler getDefaultHttpHandler() throws Exception {
            return new UrlConnectionHttpHandler();
        }

        /**
         * Gets the {@link HttpHandler} that this builder is currently configured to use for making
         * HTTP requests. If null, a default will be used instead.
         *
         * @return The {@link HttpHandler} to use.
         */
        public HttpHandler getHttpHandler() {
            return httpHandler;
        }

        /**
         * Sets the {@link HttpHandler} to use for making HTTP requests.
         *
         * @param httpHandler The {@link HttpHandler} to use.
         */
        public void setHttpHandler(HttpHandler httpHandler) {
            this.httpHandler = httpHandler;
        }

        /**
         * Sets the {@link HttpHandler} to use for making HTTP requests.
         *
         * @param httpHandler The {@link HttpHandler} to use.
         * @return This instance (for method chaining).
         */
        public Builder withHttpHandler(HttpHandler httpHandler) {
            setHttpHandler(httpHandler);
            return this;
        }

        /**
         * Gets the default {@link KeenJsonHandler} to use if none is explicitly set for this builder.
         * <p/>
         * Subclasses must override this to provide a default {@link KeenJsonHandler}.
         *
         * @return The default {@link KeenJsonHandler}.
         * @throws Exception If there is an error creating the {@link KeenJsonHandler}.
         */
        protected KeenJsonHandler getDefaultJsonHandler() throws Exception {
            return new JacksonJsonHandler();
        }

        /**
         * Gets the {@link KeenJsonHandler} that this builder is currently configured to use for
         * handling JSON operations. If null, a default will be used instead.
         *
         * @return The {@link KeenJsonHandler} to use.
         */
        public KeenJsonHandler getJsonHandler() {
            return jsonHandler;
        }

        /**
         * Sets the {@link KeenJsonHandler} to use for handling JSON operations.
         *
         * @param jsonHandler The {@link KeenJsonHandler} to use.
         */
        public void setJsonHandler(KeenJsonHandler jsonHandler) {
            this.jsonHandler = jsonHandler;
        }

        /**
         * Sets the {@link KeenJsonHandler} to use for handling JSON operations.
         *
         * @param jsonHandler The {@link KeenJsonHandler} to use.
         * @return This instance (for method chaining).
         */
        public Builder withJsonHandler(KeenJsonHandler jsonHandler) {
            setJsonHandler(jsonHandler);
            return this;
        }

        /**
         * Gets the base URL to use for queries.
         *
         * @return The base URL to use.
         */
        public String getBaseURL() { return this.baseUrl; }

        /**
         * Sets the Base URL to use for queries.
         *
         * @param baseUrl The base URL to use.
         */
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }

        /**
         * Sets the Base URL to use for queries.
         *
         * @param baseUrl The base URL to use.
         * @return This instance (for method chaining).
         */
        public Builder withBaseUrl(String baseUrl) {
            setBaseUrl(baseUrl);
            return this;
        }

        /**
         * Gets the {@link KeenProject} to use for queries.
         *
         * @return The {@link KeenProject}.
         */
        public KeenProject getKeenProject() { return this.project; }

        /**
         * Sets the {@link KeenProject} to use for queries.
         *
         * @param project The Keen Project containing Project ID and read/write keys.
         */
        public void setKeenProject(KeenProject project) { this.project = project; }

        /**
         * Builds a new Keen query client using the interfaces which have been specified explicitly on
         * this builder instance via the set* or with* methods, or the default interfaces if none
         * have been specified.
         *
         * @return A newly constructed Keen client.
         *  @throws IllegalArgumentException when the project is null.
         */
        public KeenQueryClient build() {

            try {
                if (httpHandler == null) {
                    httpHandler = getDefaultHttpHandler();
                }
            } catch (Exception e) {
                KeenLogging.log("Exception building HTTP handler: " + e.getMessage());
            }

            try {
                if (jsonHandler == null) {
                    jsonHandler = getDefaultJsonHandler();
                }
            } catch (Exception e) {
                KeenLogging.log("Exception building JSON handler: " + e.getMessage());
            }

            if (project == null) {
                throw new IllegalStateException("Cannot build KeenQueryClient with null project.");
            }

            if (baseUrl == null) {
                baseUrl = KeenConstants.SERVER_ADDRESS;
            }
            return buildInstance();
        }

        /**
         * Builds an instance based on this builder.
         */
        protected KeenQueryClient buildInstance() {
            return new KeenQueryClient(this);
        }

    }
}
