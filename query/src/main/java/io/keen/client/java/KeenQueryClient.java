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

import io.keen.client.java.exceptions.KeenQueryClientException;

import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.OutputSource;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;
import io.keen.client.java.http.UrlConnectionHttpHandler;

import io.keen.client.java.result.Group;
import io.keen.client.java.result.Interval;
import io.keen.client.java.result.QueryResult;
import io.keen.client.java.result.DoubleResult;
import io.keen.client.java.result.LongResult;
import io.keen.client.java.result.StringResult;
import io.keen.client.java.result.ListResult;
import io.keen.client.java.result.IntervalResult;
import io.keen.client.java.result.GroupByResult;

public class KeenQueryClient {

    private static final String ENCODING = "UTF-8";
    private KeenJsonHandler jsonHandler;
    private String baseUrl;
    private KeenProject project;
    private HttpHandler httpHandler;


    // TODO: talk to Kevin about allowing this option:
    // I thought it would be nice to have a constructor that takes in a Keen Client,
    // but then if that client isn't active, then we'd need to throw an exception,
    // and I'm not sure about throwing exceptions in constructors...
    /**
     * Initializes the query based on {@link KeenClient} provided. This automatically sets the
     * default project, Base URL, and the Json handler.
     *
     * @param client The {@link KeenClient} provides Json handler, base URL, and default project information .
     */
    public void initialize(KeenClient client) throws KeenQueryClientException {
        if (client.isActive() == false ) {
            throw new KeenQueryClientException("Keen client is not active. Initialization failed.");
        }

        this.jsonHandler = client.getJsonHandler();
        this.baseUrl = client.getBaseUrl();
        this.project = client.getDefaultProject();

        // should try ot get Keen client's http handler instead, but it's private.
        this.httpHandler = new UrlConnectionHttpHandler();
    }

    /**
     * Gets the default project that this {@link KeenClient} should use if no project is specified.
     *
     * @return The {@link KeenProject}.
     */
    public KeenProject getProject() {
        return this.project;
    }

    /**
     * Sets the default project that this {@link KeenClient} should use if no project is specified.
     *
     * @param project The project for queries.
     */
    public void setProject(KeenProject project) {
        this.project = project;
    }
    /**
     * Sets the base API URL associated with this instance of the {@link KeenClient}.
     * <p>
     * Use this if you want to disable SSL.
     * </p>
     * @param baseUrl The new base URL (i.e. 'http://api.keen.io'), or null to reset the base URL to
     *                the default ('https://api.keen.io').
     */
    public void setBaseUrl(String baseUrl) {
        if (baseUrl == null || baseUrl.isEmpty()) {
            this.baseUrl = KeenConstants.SERVER_ADDRESS;
        } else {
            this.baseUrl = baseUrl;
        }
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
     * Count Resource query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/#count
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Long count(String eventCollection, Timeframe timeframe) throws IOException {
        Query queryParams = new Query.QueryBuilder(QueryType.COUNT_RESOURCE)
                .withEventCollection(eventCollection)
                .build();
        QueryResult result = execute(queryParams, timeframe);

        return queryResultToLong(result);
    }

    /**
     * Count Unique query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/#count-unique
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Long countUnique(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException {
        Query queryParams = new Query.QueryBuilder(QueryType.COUNT_UNIQUE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        QueryResult result = execute(queryParams, timeframe);

        return queryResultToLong(result);
    }

    /**
     * Minimum query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#minimum-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double minimum(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException {
        Query queryParams = new Query.QueryBuilder(QueryType.MINIMUM_RESOURCE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        QueryResult result =  execute(queryParams, timeframe);
        return queryResultToDouble(result);
    }

    /**
     * Maximum query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#maximum-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double maximum(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.QueryBuilder(QueryType.MAXIMUM_RESOURCE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        QueryResult result = execute(queryParams, timeframe);
        return queryResultToDouble(result);
    }

    /**
     * Average query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#average-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double average(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.QueryBuilder(QueryType.AVERAGE_RESOURCE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        QueryResult result = execute(queryParams, timeframe);
        return queryResultToDouble(result);
    }

    /**
     * Median query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#median-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double median(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.QueryBuilder(QueryType.MEDIAN_RESOURCE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        QueryResult result =  execute(queryParams, timeframe);
        return queryResultToDouble(result);
    }

    /**
     * Percentile query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#percentile-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @param percentile     The percentile.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double percentile(String eventCollection, String targetProperty, Double percentile, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.QueryBuilder(QueryType.PERCENTILE_RESOURCE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withPercentile(percentile)
                .build();
        QueryResult result = execute(queryParams, timeframe);
        return queryResultToDouble(result);
    }

    /**
     * Sum Resource query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#sum-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Double sum(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.QueryBuilder(QueryType.SUM_RESOURCE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        QueryResult result =  execute(queryParams, timeframe);
        return queryResultToDouble(result);
    }

    /**
     * Select Unique Resource query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#select-unique-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public QueryResult selectUnique(String eventCollection, String targetProperty, Timeframe timeframe) throws IOException  {
        Query queryParams = new Query.QueryBuilder(QueryType.SELECT_UNIQUE_RESOURCE)
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        QueryResult result = execute(queryParams, timeframe);
        return result;
    }

    public QueryResult execute(Query params, Timeframe timeframe) throws IOException {
        Object returnVal = executeHelper(params, timeframe);

        QueryResult result = constructQueryResult(returnVal, params.hasGroupBy(), params.hasInterval());
        return result;
    }

    // Construct Query Result
    private static QueryResult constructQueryResult(Object input, boolean isGroupBy, boolean isInterval) {
        QueryResult thisObject = null;

        // below code determines what type of object QueryResult holds.
        if (input instanceof Integer) {
            Integer intValue = (Integer) input;
            thisObject = new LongResult(intValue.longValue());
        } else if (input instanceof Long) {
            thisObject = new LongResult((Long) input);
        } else if (input instanceof Double) {
            thisObject = new DoubleResult((Double) input);
        } else if (input instanceof String) {
            thisObject = new StringResult((String) input);
        } else if (input instanceof ArrayList) {

            // recursively construct the children of this...
            ArrayList<QueryResult> listOutput = new ArrayList<QueryResult>();
            ArrayList<Object> listInput = (ArrayList<Object>)input;
            if (isInterval) {
                thisObject = constructIntervalResult(listInput, isGroupBy);
            } else if (isGroupBy) {
                thisObject = constructGroupByResult(listInput);
            } else {
                for (Object child : listInput) {
                    QueryResult resultItem = constructQueryResult(child, false, false);
                    listOutput.add(resultItem);
                }
                thisObject = new ListResult(listOutput);
            }
        }

        return thisObject;
    }

    private static IntervalResult constructIntervalResult(ArrayList<Object> intervals, boolean isGroupBy) {
        Map<AbsoluteTimeframe, QueryResult> intervalResult = new HashMap<AbsoluteTimeframe, QueryResult>();

        for (Object child : intervals) {
            if (child instanceof HashMap) {
                HashMap<String, Object> inputMap = (HashMap<String, Object>) child;
                // If this is an interval, it should have keys "timeframe" and "value"
                if (inputMap.containsKey(KeenQueryConstants.TIMEFRAME) && (inputMap.containsKey(KeenQueryConstants.VALUE))) {
                    AbsoluteTimeframe absoluteTimeframe = null;
                    Object timeframe = inputMap.get(KeenQueryConstants.TIMEFRAME);
                    if (timeframe instanceof HashMap) {
                        HashMap<String, String> hashTimeframe = (HashMap<String, String>) timeframe;
                        String start = hashTimeframe.get(KeenQueryConstants.START);
                        String end = hashTimeframe.get(KeenQueryConstants.END);
                        absoluteTimeframe = new AbsoluteTimeframe(start, end);
                    }

                    Object value = inputMap.get(KeenQueryConstants.VALUE);
                    QueryResult queryResultValue = constructQueryResult(value, isGroupBy, false);

                    intervalResult.put(absoluteTimeframe, queryResultValue);
                }
            }
        }

        return new IntervalResult(intervalResult);
    }

    private static GroupByResult constructGroupByResult(ArrayList<Object> groups) {
        Map<Group, QueryResult> groupByResult = new HashMap<Group, QueryResult>();

        for (Object child : groups) {
            if (child instanceof HashMap) {
                HashMap<String, Object> inputMap = (HashMap<String, Object>) child;

                // If this is a GroupByResult, it should have key "result", along with properties to group by.
                if (inputMap.containsKey(KeenQueryConstants.RESULT)) {
                    QueryResult result = null;
                    HashMap<String, Object> properties = new HashMap<String, Object>();
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
                }
            }
        }

        return new GroupByResult(groupByResult);
    }
//    // Construct Query Result
//    private static QueryResult constructQueryResult(Object input, boolean isGroupBy, boolean isInterval) {
//        QueryResult thisObject = null;
//
//        // below code determines what type of object QueryResult holds.
//        if (input instanceof Integer) {
//            Integer intValue = (Integer)input;
//            thisObject = new LongResult(intValue.longValue());
//        }else if (input instanceof Long) {
//            thisObject = new LongResult((Long)input);
//        } else if (input instanceof Double) {
//            thisObject = new DoubleResult((Double)input);
//        } else if (input instanceof String) {
//            thisObject = new StringResult((String)input);
//        } else if (input instanceof ArrayList) {
//
//            // recursively construct the children of this...
//            ArrayList<QueryResult> listOutput = new ArrayList<QueryResult>();
//            ArrayList<Object> listInput = (ArrayList<Object>)input;
//            for (Object child : listInput) {
//                QueryResult resultItem = constructQueryResult(child, isGroupBy, isInterval);
//                listOutput.add(resultItem);
//            }
//            thisObject = new ListResult(listOutput);
//        } else {
//            if (input instanceof HashMap) {
//
//                HashMap<String, Object> inputMap = (HashMap<String, Object>) input;
//
//                // Next, we try to detect Interval or GroupBy.
//                // if there is an interval or groupBy, we expect to process them at
//                // the top level. When we recurse, we want to just make sure that
//                // we don't have any nested Intervals or GroupByResult's by explicitly setting
//                // them to false.
//                if (isInterval) {
//
//                    // If this is an interval, it should have keys "timeframe" and "value"
//                    if (inputMap.containsKey(KeenQueryConstants.TIMEFRAME) && (inputMap.containsKey(KeenQueryConstants.VALUE))) {
//                        AbsoluteTimeframe absoluteTimeframe = null;
//                        Object timeframe = inputMap.get(KeenQueryConstants.TIMEFRAME);
//                        if (timeframe instanceof HashMap) {
//                            HashMap<String, String> hashTimeframe = (HashMap<String, String>) timeframe;
//                            String start = hashTimeframe.get(KeenQueryConstants.START);
//                            String end = hashTimeframe.get(KeenQueryConstants.END);
//                            absoluteTimeframe = new AbsoluteTimeframe(start, end);
//                        }
//
//                        Object value = inputMap.get(KeenQueryConstants.VALUE);
//                        QueryResult queryResultValue = constructQueryResult(value, isGroupBy, false);
//
//                        Map<AbsoluteTimeframe, QueryResult> intervalResult = new HashMap<AbsoluteTimeframe, QueryResult>();
//                        intervalResult.put(absoluteTimeframe, queryResultValue);
//
//                        thisObject = new IntervalResult(intervalResult);
//                    }
//                } else if (isGroupBy) {
//
//                    // If this is a GroupByResult, it should have key "result", along with properties to group by.
//                    if (inputMap.containsKey(KeenQueryConstants.RESULT)) {
//                        QueryResult result = null;
//                        HashMap<String, Object> properties = new HashMap<String, Object>();
//                        for (String key : inputMap.keySet()) {
//                            if (key.equals(KeenQueryConstants.RESULT)) {
//                                // there should not be intervals nested inside GroupByResult's; only
//                                // the other way around.
//                                result = constructQueryResult(inputMap.get(key), false, false);
//                            } else {
//                                properties.put(key, inputMap.get(key));
//                            }
//                        }
//
//                        Group groupBy = new Group(properties);
//
//                        HashMap<Group, QueryResult> groupByResult = new HashMap<Group, QueryResult>();
//                        groupByResult.put(groupBy, result);
//
//                        thisObject = new GroupByResult(groupByResult);
//                    }
//                }
//            }
//        }
//
//        // this is a catch-all for Select Unique queries, where the object can be of any type.
////        if (thisObject == null) {
////            thisObject = new QueryResult(input);
////        }
//        return thisObject;
//    }
//

    /**
     * Posts a query to the server.
     *
     * @param params         The {@link Query} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    private Object executeHelper(Query params, Timeframe timeframe) throws IOException {
        QueryType queryType = params.getQueryType();

        if (false == params.AreParamsValid(queryType)) {
            throw new IllegalArgumentException("Keen Query parameters are insufficient. Please check Query API docs for required arguments.");
        }

        String urlString = formatBaseURL(QueryType.getQueryType(queryType));

        // Query parameter args.
        Map<String, Object> allQueryArgs = params.ConstructQueryArgs();
        if (timeframe != null) {
            allQueryArgs.putAll(timeframe.constructTimeframeArgs());
        }

        Object returnVal = "";
        URL url = new URL(urlString);
        returnVal = publishObject(project, url, allQueryArgs);

        return returnVal;
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
    protected Object publishObject(KeenProject project, URL url,
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
     * @param builder The {@link QueryBuilder} from which to retrieve this client's interfaces and settings.
     */
    protected KeenQueryClient(QueryBuilder builder) {
        // Initialize final properties using the builder.
        this.httpHandler = builder.httpHandler;
        this.jsonHandler = builder.jsonHandler;
        this.baseUrl = builder.baseUrl;
        this.project = builder.project;
    }

    public static class QueryBuilder {

        private HttpHandler httpHandler;
        private KeenJsonHandler jsonHandler;
        private String baseUrl;
        private KeenProject project;

        /**
         * Builder to create a KeenQueryClient with {@link KeenProject} .
         *
         * @param project The {@link KeenProject} to use.
         */
        public QueryBuilder(KeenProject project) {
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
        public QueryBuilder withHttpHandler(HttpHandler httpHandler) {
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
        public QueryBuilder withJsonHandler(KeenJsonHandler jsonHandler) {
            setJsonHandler(jsonHandler);
            return this;
        }

        /**
         * Gets the base URL to use for queries.
         *
         * @return The base URL to use.
         */
        public String getBaseURL() {return this.baseUrl;}

        /**
         * Sets the Base URL to use for queries.
         *
         * @param baseUrl The base URL to use.
         */
        public void setBaseUrl(String baseUrl) {this.baseUrl = baseUrl;}

        /**
         * Sets the Base URL to use for queries.
         *
         * @param baseUrl The base URL to use.
         * @return This instance (for method chaining).
         */
        public QueryBuilder withBaseUrl(String baseUrl) {setBaseUrl(baseUrl); return this;}

        /**
         * Gets the {@link KeenProject} to use for queries.
         *
         * @return The {@link KeenProject}.
         */
        public KeenProject getKeenProject() {return this.project;}

        /**
         * Sets the {@link KeenProject} to use for queries.
         *
         * @param project The Keen Project containing Project ID and read/write keys.
         */
        public void setKeenProject(KeenProject project) {this.project = project;}

        /**
         * Builds a new Keen query client using the interfaces which have been specified explicitly on
         * this builder instance via the set* or with* methods, or the default interfaces if none
         * have been specified.
         *
         * @return A newly constructed Keen client.
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
                project = new KeenProject("project","<readKey>", "<writeKey>");
            }
            if (baseUrl == null) {
                baseUrl = KeenConstants.SERVER_ADDRESS;
            }
            return buildInstance();
        }

        /**
         * Builds an instance based on this builder. This method is exposed only as a test hook to
         * allow test classes to modify how the {@link KeenClient} is constructed (i.e. by
         * providing a mock {@link Environment}.
         *
         * @return The new {@link KeenClient}.
         */
        protected KeenQueryClient buildInstance() {
            return new KeenQueryClient(this);
        }


        }
}
