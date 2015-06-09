package io.keen.client.java;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.List;

import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.KeenQueryClientException;

import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.OutputSource;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;
import io.keen.client.java.http.UrlConnectionHttpHandler;


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
     * Query API info here: https://keen.io/docs/api/reference/#count-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object count(String eventCollection) throws IOException, KeenQueryClientException {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .build();
        Object result = count(queryParams);
        return result;
    }

    /**
     * Count Unique query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#count-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object count(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.COUNT_RESOURCE, queryParams);
        return result;
    }

    /**
     * Count Unique query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#count-unique-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object countUnique(String eventCollection, String targetProperty) throws IOException, KeenQueryClientException {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Object result = countUnique(queryParams);
        return result;
    }

    /**
     * Count Unique query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#count-unique-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object countUnique(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.COUNT_UNIQUE, queryParams);
        return result;
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
    public Object minimum(String eventCollection, String targetProperty) throws IOException, KeenQueryClientException {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Object result =  minimum(queryParams);
        return result;
    }

    /**
     * Minimum query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#minimum-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object minimum(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.MINIMUM_RESOURCE, queryParams);
        return result;
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
    public Object maximum(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Object result = maximum(queryParams);
        return result;
    }

    /**
     * Maximum query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#maximum-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object maximum(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.MAXIMUM_RESOURCE, queryParams);
        return result;
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
    public Object average(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Object result = average(queryParams);
        return result;
    }

    /**
     * Average query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#average-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object average(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.AVERAGE_RESOURCE, queryParams);
        return result;
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
    public Object median(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Object result =  median(queryParams);
        return result;
    }

    /**
     * Median query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#median-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object median(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.MEDIAN_RESOURCE, queryParams);
        return result;
    }

    /**
     * Percentile query with only the required arguments.
     * Query API info here: https://keen.io/docs/api/reference/#percentile-resource
     *
     * @param eventCollection     The name of the event collection you are analyzing.
     * @param targetProperty     The name of the property you are analyzing.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object percentile(String eventCollection, String targetProperty, Double percentile) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withPercentile(percentile)
                .build();
        Object result =  percentile(queryParams);
        return result;
    }

    /**
     * Percentile query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#percentile-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object percentile(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.PERCENTILE_RESOURCE, queryParams);
        return result;
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
    public Object sum(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Object result =  sum(queryParams);
        return result;
    }

    /**
     * Sum Resource query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#sum-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object sum(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.SUM_RESOURCE, queryParams);
        return result;
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
    public Object selectUnique(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Object result = selectUnique(queryParams);
        return result;
    }

    /**
     * Select Unique Resource query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/api/reference/#select-unique-resource
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object selectUnique(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Object result = runQuery(KeenQueryConstants.SELECT_UNIQUE_RESOURCE, queryParams);
        return result;
    }

    /**
     * Extraction query with just the required argument - the event collection, and an email address.
     * Query API info here: https://keen.io/docs/data-analysis/extractions/
     *
     * @param eventCollection     The Event Collection.
     * @param email     The email to send query results to.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */    public void extraction(String eventCollection, String email) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withEmail(email)
                .build();
        Object result = extraction(queryParams);
        // possibly exception if something went wrong, but no return value because email is sent
    }

    /**
     * Extraction query with just the required argument - the event collection.
     * Query API info here: https://keen.io/docs/data-analysis/extractions/
     *
     * @param eventCollection     The Event Collection.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object extraction(String eventCollection) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .build();
        Object result = extraction(queryParams);
        return result;
    }

    /**
     * Extraction query with all required and optional arguments.
     * Query API info here: https://keen.io/docs/data-analysis/extractions/
     *
     * @param queryParams     The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object extraction(KeenQueryParams queryParams) throws IOException {
        Object result = runQuery(KeenQueryConstants.EXTRACTION_RESOURCE, queryParams);
        return result;
    }

    /**
     * Posts a query to the server.
     *
     * @param queryName     The name of the query, as specified by {@link KeenQueryConstants}.
     * @param params         The {@link KeenQueryParams} with all the required and optional arguments.
     * @return The response from the server in the "result" map.
     * @throws IOException If there was an error communicating with the server or
     * an error message received from the server.
     */
    public Object runQuery(String queryName, KeenQueryParams params) throws IOException {
        if (false == params.AreParamsValid(queryName)) {
            throw new IllegalArgumentException("Keen Query parameters are insufficient. Please check Query API docs for required arguments.");
        }

        String urlString = String.format(Locale.US, "%s/%s/projects/%s/queries/%s",
                baseUrl,
                KeenConstants.API_VERSION,
                project.getProjectId(),
                queryName        // query name
        );

        // Query parameter args.
        Map<String, Object> allQueryArgs = params.ConstructQueryArgs();

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

//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        String resultString = response.body.toString();
//        String resultString = outputStream.toString(ENCODING);

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


    // https://keen.io/docs/api/reference/#funnel-resource
    public Object funnel(List<Map<String, Object>> steps) throws IOException, KeenException {
        if ( steps == null || steps.isEmpty()) {
            throw new IllegalArgumentException("Keen Query parameters are insufficient. Funnel Query requires \"steps\" argument to be non-empty.");
        }

        String urlString = String.format(Locale.US, "%s/%s/projects/%s/queries/%s",
                baseUrl,
                KeenConstants.API_VERSION,
                project.getProjectId(),
                KeenQueryConstants.FUNNEL       // query name
        );
        // funnel args
        Map<String, Object> jsonFunnelArgs = new HashMap<String, Object>();
        jsonFunnelArgs.put(KeenQueryConstants.STEPS, steps);

        Object returnVal = "";

        URL url = new URL(urlString);
        returnVal = publishObject(project, url, jsonFunnelArgs);

        return returnVal;
    }

    // https://keen.io/docs/data-analysis/multi-analysis/
    public Object multiAnalysis(String eventCollection, Map<String, Object> analysis) throws IOException {
        if ( analysis == null || analysis.isEmpty()) {
            throw new IllegalArgumentException("Keen Query parameters are insufficient. Multi-analysis Query requires \"analysis\" argument to be non-empty.");
        }

        String urlString = String.format(Locale.US, "%s/%s/projects/%s/queries/%s",
                baseUrl,
                KeenConstants.API_VERSION,
                project.getProjectId(),
                KeenQueryConstants.FUNNEL       // query name
        );

        // JSON arg with multi-analysis
        Map<String, Object> analysisArg = new HashMap<String, Object>();
        analysisArg.put(KeenQueryConstants.ANALYSIS, analysis);
        analysisArg.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);

        Object returnVal = "";

        URL url = new URL(urlString);
        returnVal = publishObject(project, url, analysisArg);

        return returnVal;
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
         * Sets the {@link KeenProject} to use for queries.
         *
         * @param project The Keen Project containing Project ID and read/write keys.
         * @return This instance (for method chaining).
         */
        public QueryBuilder withKeenProject(KeenProject project) {setKeenProject(project); return this;}

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
