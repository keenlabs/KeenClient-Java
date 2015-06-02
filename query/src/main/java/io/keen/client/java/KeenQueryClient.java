package io.keen.client.java;

import java.io.ByteArrayOutputStream;
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
//    private KeenQueryParams queryParams;
    private HttpHandler httpHandler;

    // Constructors are replaced with Builder.
//    public KeenQueryClient() {
//
//        // we make an executive decision to use Jackson
//        this(new JacksonJsonHandler());
//    }
//
//    public KeenQueryClient(KeenJsonHandler jsonHandler) {
//        this.jsonHandler = jsonHandler;
//        baseUrl = KeenConstants.SERVER_ADDRESS;
//        queryParams = new KeenQueryParams();
//        project = null;
//        httpHandler = new UrlConnectionHttpHandler();
//        // how about project/read/write keys?
//    }


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
     * Sets optional  {@link KeenQueryParams} to use for query.
     *
     * @param params The {@link KeenQueryParams} to use for query.
     */
//    public void setQueryParams(KeenQueryParams params) {
//        this.queryParams = params;
//    }


    /**
     * Gets the optional  {@link KeenQueryParams} to use for query.
     */
//    public KeenQueryParams getQueryParams() {
//        return this.queryParams;
//    }

    /**
     * Clears the optional  {@link KeenQueryParams} to use for query.
     */
//    public void clearQueryParams() {
//        this.queryParams = new KeenQueryParams();
//    }

    public Map<String, Object> count(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.COUNT_RESOURCE, queryParams);
        return result;
    }

    public Map<String, Object> count(String eventCollection) throws IOException, KeenQueryClientException {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .build();
        Map<String, Object> result = count(queryParams);
        return result;
    }

    public Map<String, Object> countUnique(String eventCollection, String targetProperty) throws IOException, KeenQueryClientException {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Map<String, Object> result = countUnique(queryParams);
        return result;
    }

    public Map<String, Object> countUnique(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.COUNT_UNIQUE, queryParams);
        return result;
    }

    public Map<String, Object> minimum(String eventCollection, String targetProperty) throws IOException, KeenQueryClientException {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Map<String, Object> result =  minimum(queryParams);
        return result;
    }

    public Map<String, Object> minimum(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.MINIMUM_RESOURCE, queryParams);
        return result;
    }

    public Map<String, Object> maximum(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Map<String, Object> result = maximum(queryParams);
        return result;
    }

    public Map<String, Object> maximum(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.MAXIMUM_RESOURCE, queryParams);
        return result;
    }

    public Map<String, Object> average(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Map<String, Object> result = average(queryParams);
        return result;
    }

    public Map<String, Object> average(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.AVERAGE_RESOURCE, queryParams);
        return result;
    }

    public Map<String, Object> median(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Map<String, Object> result =  median(queryParams);
        return result;
    }

    public Map<String, Object> median(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.MEDIAN_RESOURCE, queryParams);
        return result;
    }

    // TODO: change to double
    public Map<String, Object> percentile(String eventCollection, String targetProperty, String percentile) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .withPercentile(percentile)
                .build();
        Map<String, Object> result =  percentile(queryParams);
        return result;
    }

    public Map<String, Object> percentile(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.PERCENTILE_RESOURCE, queryParams);
        return result;
    }

    public Map<String, Object> sum(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Map<String, Object> result =  sum(queryParams);
        return result;
    }

    public Map<String, Object> sum(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.SUM_RESOURCE, queryParams);
        return result;
    }

    public Map<String, Object> selectUnique(String eventCollection, String targetProperty) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withTargetProperty(targetProperty)
                .build();
        Map<String, Object> result = selectUnique(queryParams);
        return result;
    }

    public Map<String, Object> selectUnique(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.SELECT_UNIQUE_RESOURCE, queryParams);
        return result;
    }

    // with email - no return value
    public void extraction(String eventCollection, String email) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .withEmail(email)
                .build();
        Map<String, Object> result = extraction(queryParams);
        // possibly exception if something went wrong, but no return value because email is sent
    }

    // without email - return value
    public Map<String, Object> extraction(String eventCollection) throws IOException  {
        KeenQueryParams queryParams = new KeenQueryParams.QueryParamBuilder()
                .withEventCollection(eventCollection)
                .build();
        Map<String, Object> result = extraction(queryParams);
        return result;
    }

    public Map<String, Object> extraction(KeenQueryParams queryParams) throws IOException, KeenQueryClientException {
        Map<String, Object> result = runQuery(KeenQueryConstants.EXTRACTION_RESOURCE, queryParams);
        return result;
    }

    // TODO: return result map. If error (aka, no "result" key), throw exception.
    // TODO: non-200 response code

    public Map<String, Object> runQuery(String queryName, KeenQueryParams params) throws IOException, KeenException {
        if (false == params.AreParamsValid(queryName)) {
            // todo: we can throw an IllegalArgumentException for this.
            throw new KeenQueryClientException("Keen Query parameters are insufficient. Please check Query API docs for required arguments.");
        }

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
    public Map<String, Object>funnel(List<Map<String, Object>> steps) throws IOException, KeenException {
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
        jsonFunnelArgs.put(KeenQueryConstants.STEPS, steps);

        String returnVal = "";
        try {
            URL url = new URL(urlString);
            returnVal = publishObject(project, url, jsonFunnelArgs);
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

    protected String publishObject(KeenProject project, URL url,
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


        return response.body;
    }


    /**
     * Constructs a Keen Query client using a builder.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    protected KeenQueryClient(QueryBuilder builder) {
        // Initialize final properties using the builder.
        this.httpHandler = builder.httpHandler;
        this.jsonHandler = builder.jsonHandler;
        this.baseUrl = builder.baseUrl;
        this.project = builder.project;
//        this.queryParams = builder.keenQueryParams;
    }

    public static class QueryBuilder {

        private HttpHandler httpHandler;
        private KeenJsonHandler jsonHandler;
        private String baseUrl;
        private KeenProject project;
//        private KeenQueryParams keenQueryParams;

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

        public String getBaseURL() {return this.baseUrl;}
        public void setBaseUrl(String baseUrl) {this.baseUrl = baseUrl;}
        public QueryBuilder withBaseUrl(String baseUrl) {setBaseUrl(baseUrl); return this;}

        public KeenProject getKeenProject() {return this.project;}
        public void setKeenProject(KeenProject project) {this.project = project;}
        public QueryBuilder withKeenProject(KeenProject project) {setKeenProject(project); return this;}

//        public KeenQueryParams getKeenQueryParams() {return this.keenQueryParams;}
//        public void setKeenQueryParams(KeenQueryParams project) {this.keenQueryParams = keenQueryParams;}
//        public Builder withKeenQueryParams(KeenQueryParams keenQueryParams) {setKeenQueryParams(keenQueryParams); return this;}

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

            // todo: Project really needs to not be null... Handle differently?
            try {
                if (project == null) {
                    project = new KeenProject("project","<readKey>", "writeKey");
                }
            } catch (Exception e) {
                KeenLogging.log("Exception building Project: " + e.getMessage());
            }

//            try {
//                if (keenQueryParams == null) {
//                    keenQueryParams = new KeenQueryParams();
//                }
//            } catch (Exception e) {
//                KeenLogging.log("Exception building Keen Query Params: " + e.getMessage());
//            }

            try {
                if (baseUrl == null) {
                    baseUrl = KeenConstants.SERVER_ADDRESS;
                }
            } catch (Exception e) {
                KeenLogging.log("Exception building Base URL: " + e.getMessage());
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

//    private int getIntegerResult(Map<String, Object> jsonMap) throws NumberFormatException, KeenQueryClientException {
//        int intValue = 0;
//        Object result = jsonMap.get(KeenQueryConstants.RESULT);
//        if ( result != null) {
//            intValue = Integer.parseInt((String)result);
//        } else {
//            String errorMessage = getErrorMessage(jsonMap);
//            if (errorMessage.isEmpty()) {
//                errorMessage = "Query result not found.";
//            }
//            throw new KeenQueryClientException(errorMessage);
//        }
//        return intValue;
//    }
//
//
//    private double getDoubleResult(Map<String, Object> jsonMap) throws NumberFormatException, KeenQueryClientException {
//        double doubleValue = 0;
//        Object result = jsonMap.get(KeenQueryConstants.RESULT);
//        if ( result != null) {
//            doubleValue = Double.valueOf((String) result);
//        } else {
//            String errorMessage = getErrorMessage(jsonMap);
//            if (errorMessage.isEmpty()) {
//                errorMessage = "Query result not found.";
//            }
//            throw new KeenQueryClientException(errorMessage);
//        }
//        return doubleValue;
//    }
//
//    private List<?> getListResult(Map<String, Object> jsonMap) throws KeenQueryClientException {
//        Object result = jsonMap.get(KeenQueryConstants.RESULT);
//        if (result != null) {
//            if (result instanceof List<?>) {
//                return (List<?>)result;
//            }
//        } else {
//            String errorMessage = getErrorMessage(jsonMap);
//            if (errorMessage.isEmpty()) {
//                errorMessage = "Query result not found.";
//            }
//            throw new KeenQueryClientException(errorMessage);
//        }
//        return null;
//    }
//
//    private Map<?,?> getMapResult(Map<String, Object> jsonMap) throws KeenQueryClientException {
//        Object result = jsonMap.get(KeenQueryConstants.RESULT);
//        if (result != null) {
//            if (result instanceof Map<?,?>) {
//                return (Map<?,?>)result;
//            }
//        } else {
//            String errorMessage = getErrorMessage(jsonMap);
//            if (errorMessage.isEmpty()) {
//                errorMessage = "Query result not found.";
//            }
//            throw new KeenQueryClientException(errorMessage);
//        }
//        return null;
//    }
//
//    private String getStringResult(Map<String, Object> jsonMap) throws KeenQueryClientException {
//        Object result = jsonMap.get(KeenQueryConstants.RESULT);
//        if (result != null) {
//            if (result instanceof String) {
//                return (String)result;
//            }
//            // todo: else what if not instanceof expected type?
//        } else {
//            String errorMessage = getErrorMessage(jsonMap);
//            if (errorMessage.isEmpty()) {
//                errorMessage = "Query result not found.";
//            }
//            throw new KeenQueryClientException(errorMessage);
//        }
//        return null;
//    }
//
//    private String getErrorMessage(Map<String, Object> jsonMap) {
//        // get as much useful information as possible from error
//        StringBuffer errorString = new StringBuffer();
//        Object errorCode = jsonMap.get(KeenQueryConstants.ERROR_CODE);
//        Object message = jsonMap.get(KeenQueryConstants.MESSAGE);
//        if (errorCode != null) {
//            errorString.append((String)errorCode);
//            if (message != null) {
//                errorString.append(": " + (String)message);
//            }
//
//            return errorString.toString();
//        }
//
//        return "";
//    }

}
