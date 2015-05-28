package io.keen.client.java;

        import java.io.IOException;
        import java.io.OutputStream;
        import java.io.OutputStreamWriter;
        import java.io.StringReader;
        import java.net.URL;
        import java.net.URLConnection;
        import java.util.HashMap;
        import java.util.Locale;
        import java.util.Map;
        import java.util.List;
        import java.util.Objects;

        import io.keen.client.java.exceptions.InvalidEventException;
        import io.keen.client.java.exceptions.KeenException;
        import io.keen.client.java.http.HttpHandler;
        import io.keen.client.java.http.OutputSource;
        import io.keen.client.java.http.Request;
        import io.keen.client.java.http.Response;
        import io.keen.client.java.http.UrlConnectionHttpHandler;
        import io.keen.client.java.exceptions.KeenQueryClientException;

public class KeenQueryClient {

    private static final String ENCODING = "UTF-8";
    private KeenJsonHandler jsonHandler;
    private String baseUrl;
    private KeenProject project;
    private KeenQueryParams queryParams;
    private HttpHandler httpHandler;

    public KeenQueryClient(KeenJsonHandler jsonHandler) {
        this.jsonHandler = jsonHandler;
        baseUrl = KeenConstants.SERVER_ADDRESS;
        queryParams = new KeenQueryParams();
        project = null;
        httpHandler = new UrlConnectionHttpHandler();
        // how about project/read/write keys?
    }

    /**
     * Initializes the query based on {@link KeenClient} provided. This automatically sets the
     * default project, Base URL, and the Json handler.
     *
     * @param client The {@link KeenClient} provides Json handler, base URL, and default project information .
     */
    public void initialize(KeenClient client) {
        this.jsonHandler = client.getJsonHandler();
        this.baseUrl = client.getBaseUrl();
        this.project = client.getDefaultProject();
        this.httpHandler = new UrlConnectionHttpHandler();
    }

    /**
     * Gets the default project that this {@link KeenClient} should use if no project is specified.
     *
     */
    public KeenProject getDefaultProject() {
        return this.project;
    }


    /**
     * Sets the default project that this {@link KeenClient} should use if no project is specified.
     *
     * @param project The project for queries.
     */
    public void setDefaultProject(KeenProject project) {
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
     * Sets optional  {@link KeenQueryParams} to use for query.
     *
     * @param params The {@link KeenQueryParams} to use for query.
     */
    public void setQueryParams(KeenQueryParams params) {
        this.queryParams = params;
    }


    /**
     * Gets the optional  {@link KeenQueryParams} to use for query.
     */
    public KeenQueryParams getQueryParams() {
        return this.queryParams;
    }

    /**
     * Clears the optional  {@link KeenQueryParams} to use for query.
     */
    public void clearQueryParams() {
        this.queryParams = new KeenQueryParams();
    }

    public Map<String, Object> count(String eventCollection) throws IOException  {
        queryParams.eventCollection = eventCollection;
        return runQuery(KeenQueryConstants.COUNT_RESOURCE, queryParams);
    }

    public Map<String, Object> countUnique(String eventCollection, String targetProperty) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        return runQuery(KeenQueryConstants.COUNT_UNIQUE, queryParams);
    }

    public Map<String, Object> minimum(String eventCollection, String targetProperty) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        return runQuery(KeenQueryConstants.MINIMUM_RESOURCE, queryParams);
    }

    public Map<String, Object> maximum(String eventCollection, String targetProperty) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        return runQuery(KeenQueryConstants.MAXIMUM_RESOURCE, queryParams);
    }

    public Map<String, Object> average(String eventCollection, String targetProperty) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        return runQuery(KeenQueryConstants.AVERAGE_RESOURCE, queryParams);
    }

    public Map<String, Object> median(String eventCollection, String targetProperty) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        return runQuery(KeenQueryConstants.MEDIAN_RESOURCE, queryParams);
    }

    public Map<String, Object> percentile(String eventCollection, String targetProperty, String percentile) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        queryParams.percentile = percentile;
        return runQuery(KeenQueryConstants.PERCENTILE_RESOURCE, queryParams);
    }

    public Map<String, Object> sum(String eventCollection, String targetProperty) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        return runQuery(KeenQueryConstants.SUM_RESOURCE, queryParams);
    }

    public Map<String, Object> selectUnique(String eventCollection, String targetProperty) throws IOException  {
        queryParams.setEventCollectionAndTargetProperty(eventCollection, targetProperty);
        return runQuery(KeenQueryConstants.SELECT_UNIQUE_RESOURCE, queryParams);
    }

    public Map<String, Object> extraction(String eventCollection) throws IOException  {
        queryParams.eventCollection = eventCollection;
        return runQuery(KeenQueryConstants.EXTRACTION_RESOURCE, queryParams);
    }

    public Map<String, Object> runQuery(String queryName, KeenQueryParams params) throws IOException, KeenException {
        // TODO: validateInput - throw exception
        if (false == params.AreParamsValid(queryName)) {
            // TODO: handle this?
            throw new KeenQueryClientException("Keen Query parameters are insufficient. Please check Query API docs for required arguments.");
        }

        // TODO: or we can just pass in the project, and make this totally standalone.
//        KeenProject project = KeenClient.client().getDefaultProject();
        String urlString = String.format(Locale.US, "%s/%s/projects/%s/queries/%s",
                baseUrl,
                KeenConstants.API_VERSION,
                project.getProjectId(),
                queryName        // query name
        );

        // Query parameter args.
        Map<String, Object> allQueryArgs = params.ConstructQueryArgs();

        String returnVal = "";
        try {
            URL url = new URL(urlString);
            returnVal = publishObject(project, url, allQueryArgs);
        } catch (IOException e) {
            // TODO: catch IOException
        }

        // Parse the response into a map.
        StringReader reader = new StringReader(returnVal);
        Map<String, Object> responseMap;
        responseMap = this.jsonHandler.readJson(reader);

        return responseMap;
    }

    // TODO: parse JSON for return value?
    public String funnel(List<Map<String, Object>> steps) {
        if ( steps == null || steps.isEmpty()) {
            // TODO: handle validation
        }

//        KeenProject project = KeenClient.client().getDefaultProject();
        String urlString = String.format(Locale.US, "%s/%s/projects/%s/queries/%s",
                baseUrl,
                KeenConstants.API_VERSION,
                project.getProjectId(),
                KeenQueryConstants.FUNNEL       // query name
        );
        // funnel args
        Map<String, Object> jsonFunnelArgs = new HashMap<String, Object>();
        jsonFunnelArgs.put(KeenQueryConstants.FUNNEL, steps);

        String returnVal = "";
        try {
            URL url = new URL(urlString);
            returnVal = publishObject(project, url, jsonFunnelArgs);
        } catch (IOException e) {
            // TODO: catch IOException
        }
        return returnVal;
    }

    // TODO: parse JSON for return value?
    public String multiAnalysis(String eventCollection, Map<String, Object> analysis) {

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

        String returnVal = "";
        try {
            URL url = new URL(urlString);
            returnVal = publishObject(project, url, analysisArg);
        } catch (IOException e) {
            // TODO: catch IOException
        }
        return returnVal;
    }

    protected synchronized String publishObject(KeenProject project, URL url,
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

        // TODO: If logging is enabled, log the request being sent.
        // Send the request.
        String readkey = project.getReadKey();
        Request request = new Request(url, "POST", readkey, source, null);

//        HttpHandler httpHandler = new UrlConnectionHttpHandler(); // TODO: comment next line out.
//        Response response = httpHandler.execute(request);
        Response response = httpHandler.execute(request);

        return response.body;
    }


    /**
     * Constructs a Keen Query client using a builder.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    protected KeenQueryClient(Builder builder) {
        // Initialize final properties using the builder.
        this.httpHandler = builder.httpHandler;
        this.jsonHandler = builder.jsonHandler;

        // If any of the interfaces are null, mark this client as inactive.
        if (httpHandler == null || jsonHandler == null) {
//            setActive(false);     // TODO: is active?
        }

        // Initialize other properties.
        this.baseUrl = KeenConstants.SERVER_ADDRESS;

        this.queryParams = new KeenQueryParams();
    }

    public static abstract class Builder {

        private HttpHandler httpHandler;
        private KeenJsonHandler jsonHandler;
        private KeenNetworkStatusHandler networkStatusHandler;

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
        protected abstract KeenJsonHandler getDefaultJsonHandler() throws Exception;

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
         * Builds a new Keen client using the interfaces which have been specified explicitly on
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
