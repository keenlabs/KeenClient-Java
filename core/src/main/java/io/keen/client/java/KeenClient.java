package io.keen.client.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import io.keen.client.java.exceptions.InvalidEventCollectionException;
import io.keen.client.java.exceptions.InvalidEventException;
import io.keen.client.java.exceptions.NoWriteKeyException;
import io.keen.client.java.exceptions.ServerException;

/**
 * KeenClient has static methods to return managed instances of itself and instance methods to collect new events
 * and upload them through the Keen API.
 * <p/>
 * Example usage:
 * <p/>
 * <pre>
 *     KeenClient.initialize("my_project_id", "my_write_key", "my_read_key");
 *     Map<String, Object> myEvent = new HashMap<String, Object>();
 *     myEvent.put("property name", "property value");
 *     KeenClient.client().addEvent("purchases", myEvent);
 * </pre>
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenClient {

    ///// PUBLIC STATIC METHODS /////

    // TODO: Move this out of the public methods section.
    /**
     * Use this to attempt to initialize the client from environment variables.
     */
    static void initialize(KeenJsonHandler jsonHandler, KeenEventStore eventStore,
                           Executor publishExecutor) {
        if (ClientSingleton.INSTANCE.client != null) {
            throw new IllegalStateException("The Keen library is already initialized!");
        }
        ClientSingleton.INSTANCE.client = new KeenClient(jsonHandler, eventStore, publishExecutor);
    }

    /**
     * Call this to retrieve the singleton instance of KeenClient.
     * <p/>
     * If you only have to use a single Keen project, just use this.
     *
     * @return A managed instance of KeenClient, or null if KeenClient.initialize() hasn't been called previously.
     */
    public static KeenClient client() {
        if (ClientSingleton.INSTANCE.client == null) {
            throw new IllegalStateException("Please call KeenInitializer.initialize() before requesting the shared client.");
        }
        return ClientSingleton.INSTANCE.client;
    }

    public static void setDebugMode(boolean isDebugMode) {
        KeenClient.client().isDebugMode = isDebugMode;
    }

    ///// PUBLIC METHODS //////

    /**
     * Call this any time you want to add an event that will be sent to the Keen IO server.
     *
     * @param eventCollection The name of the event collection you want to put this event into.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see docs).
     *                        Nested Maps and lists are acceptable (and encouraged!).
     */
    public void addEvent(String eventCollection, Map<String, Object> event) {
        addEvent(eventCollection, event, null);
    }

    public void addEvent(String eventCollection, Map<String, Object> event,
                         Map<String, Object> keenProperties) {
        addEvent(null, eventCollection, event, keenProperties, null);
    }

    /**
     * Call this any time you want to add an event that will be sent to the Keen IO server AND
     * you want to override Keen-defaulted properties (like timestamp).
     *
     * @param eventCollection The name of the event collection you want to put this event into.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see docs).
     *                        Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -> Calendar.getInstance()
     * @param callback        An instance of KeenCallback. Will invoke onSuccess when adding the event succeeds.
     *                        Will invoke onError when adding the event fails.
     */
    public void addEvent(KeenProject project, String eventCollection, Map<String, Object> event,
                         Map<String, Object> keenProperties, KeenCallback callback) {

        if (!isActive) {
            handleLibraryInactive(callback);
            return;
        }

        if (project == null && defaultProject == null) {
            handleFailure(null, new IllegalStateException("No project specified, but no default project found"));
            return;
        }
        KeenProject useProject = (project == null? defaultProject : project);

        try {
            // Build the event.
            Map<String, Object> newEvent =
                    validateAndBuildEvent(useProject, eventCollection, event, keenProperties);

            // Publish the event.
            String response = publish(project, eventCollection, newEvent);
            // TODO: Validate the response?
            handleSuccess(callback);
        } catch (Exception e) {
            handleFailure(callback, e);
        }
    }

    public void addEventAsync(String eventCollection, Map<String, Object> event) {
        addEventAsync(eventCollection, event, null);
    }

    public void addEventAsync(String eventCollection, Map<String, Object> event,
                              final Map<String, Object> keenProperties) {
        addEventAsync(null, eventCollection, event, keenProperties, null);
    }

    public void addEventAsync(final KeenProject project, final String eventCollection,
                              final Map<String, Object> event,
                              final Map<String, Object> keenProperties,
                              final KeenCallback callback) {

        if (!isActive) {
            handleLibraryInactive(callback);
            return;
        }

        if (project == null && defaultProject == null) {
            handleFailure(null, new IllegalStateException("No project specified, but no default project found"));
            return;
        }
        final KeenProject useProject = (project == null? defaultProject : project);

        // Wrap the asynchronous execute in a try/catch block in case the executor throws a
        // RejectedExecutionException (or anything else).
        try {
            publishExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    addEvent(useProject, eventCollection, event, keenProperties, callback);
                }
            });
        } catch (Exception e) {
            handleFailure(callback, e);
        }
    }

    public void queueEvent(String eventCollection, Map<String, Object> event) {
        queueEvent(eventCollection, event, null);
    }

    public void queueEvent(String eventCollection, Map<String, Object> event,
                           Map<String, Object> keenProperties) {
        queueEvent(null, eventCollection, event, keenProperties, null);
    }

    public void queueEvent(KeenProject project, String eventCollection, Map<String, Object> event,
                           Map<String, Object> keenProperties, final KeenCallback callback) {

        if (!isActive) {
            handleLibraryInactive(callback);
            return;
        }

        if (project == null && defaultProject == null) {
            handleFailure(null, new IllegalStateException("No project specified, but no default project found"));
            return;
        }
        KeenProject useProject = (project == null? defaultProject : project);

        OutputStreamWriter writer = null;
        try {
            // Build the event
            Map<String, Object> newEvent =
                    validateAndBuildEvent(useProject, eventCollection, event, keenProperties);

            // Write the event out to the event store.
            OutputStream out = eventStore.getCacheOutputStream(eventCollection);
            writer = new OutputStreamWriter(out, ENCODING);
            jsonHandler.writeJson(writer, newEvent);
            handleSuccess(callback);
        } catch (Exception e) {
            handleFailure(callback, e);
        } finally {
            KeenUtils.closeQuietly(writer);
        }
    }


    public void sendQueuedEvents() {
        sendQueuedEvents(null);
    }

    public void sendQueuedEvents(KeenProject project) {
        sendQueuedEvents(project, null);
    }

    public void sendQueuedEvents(KeenProject project, KeenCallback callback) {

        if (!isActive) {
            handleLibraryInactive(callback);
            return;
        }

        if (project == null && defaultProject == null) {
            handleFailure(null, new IllegalStateException("No project specified, but no default project found"));
            return;
        }
        KeenProject useProject = (project == null? defaultProject : project);

        try {
            KeenEventStore.CacheEntries entries = eventStore.retrieveCached();
            Map<String, List<Map<String, Object>>> events = entries.events;
            String response = publishAll(useProject, events);
            if (response != null) {
                try {
                    handleAddEventsResponse(entries.handles, response);
                } catch (Exception e) {
                    // Errors handling the response are non-fatal; just log them.
                    KeenLogging.log("Error handling response to batch publish: " + e.getMessage());
                }
            }
            handleSuccess(callback);
        } catch (Exception e) {
            handleFailure(callback, e);
        }
    }

    public void sendQueuedEventsAsync() {
        sendQueuedEventsAsync(null);
    }

    public void sendQueuedEventsAsync(final KeenProject project) {
        sendQueuedEventsAsync(project, null);
    }

    public void sendQueuedEventsAsync(final KeenProject project, final KeenCallback callback) {

        if (!isActive) {
            handleLibraryInactive(callback);
            return;
        }

        if (project == null && defaultProject == null) {
            handleFailure(null, new IllegalStateException("No project specified, but no default project found"));
            return;
        }
        final KeenProject useProject = (project == null? defaultProject : project);

        // Wrap the asynchronous execute in a try/catch block in case the executor throws a
        // RejectedExecutionException (or anything else).
        try {
            publishExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    sendQueuedEvents(useProject, callback);
                }
            });
        } catch (Exception e) {
            handleFailure(callback, e);
        }
    }

    /**
     * Getter for the default project that this {@link KeenClient} will use if no project is
     * specified.
     *
     * @return the default project.
     */
    public KeenProject getDefaultProject() {
        return defaultProject;
    }

    /**
     * Setter for the default project that this {@link KeenClient} should use if no project is
     * specified.
     *
     * @param defaultProject the new default project.
     */
    public void setDefaultProject(KeenProject defaultProject) {
        this.defaultProject = defaultProject;
    }

    /**
     * Getter for the base API URL associated with this instance of the {@link KeenClient}.
     *
     * @return the base API URL
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * Setter for the base API URL associated with this instance of the {@link KeenClient}.
     * <p/>
     * Use this if you want to disable SSL.
     *
     * @param baseUrl the new base URL (i.e. 'http://api.keen.io')
     */
    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Getter for the {@link GlobalPropertiesEvaluator} associated with this instance of the {@link KeenClient}.
     *
     * @return the {@link GlobalPropertiesEvaluator}
     */
    public GlobalPropertiesEvaluator getGlobalPropertiesEvaluator() {
        return globalPropertiesEvaluator;
    }

    /**
     * Call this to set the {@link GlobalPropertiesEvaluator} for this instance of the {@link KeenClient}.
     * The evaluator is invoked every time an event is added to an event collection.
     * <p/>
     * Global properties are properties which are sent with EVERY event. For example, you may wish to always
     * capture device information like OS version, handset type, orientation, etc.
     * <p/>
     * The evaluator takes as a parameter a single String, which is the name of the event collection the
     * event's being added to. You're responsible for returning a Map which represents the global properties
     * for this particular event collection.
     * <p/>
     * Note that because we use a class defined by you, you can create DYNAMIC global properties. For example,
     * if you want to capture device orientation, then your evaluator can ask the device for its current orientation
     * and then construct the Map. If your global properties aren't dynamic, then just return the same Map
     * every time.
     * <p/>
     * Example usage:
     * <pre>
     *     {@code KeenClient client = KeenClient.client();
     *     GlobalPropertiesEvaluator evaluator = new GlobalPropertiesEvaluator() {
     *         @Override
     *         public Map<String, Object> getGlobalProperties(String eventCollection) {
     *             Map<String, Object> map = new HashMap<String, Object>();
     *             map.put("some dynamic property name", "some dynamic property value");
     *             return map;
     *         }
     *     };
     *     client.setGlobalPropertiesEvaluator(evaluator);
     *     }
     * </pre>
     *
     * @param globalPropertiesEvaluator The evaluator which is invoked any time an event is added to an event
     *                                  collection.
     */
    public void setGlobalPropertiesEvaluator(GlobalPropertiesEvaluator globalPropertiesEvaluator) {
        this.globalPropertiesEvaluator = globalPropertiesEvaluator;
    }

    /**
     * Getter for the Keen Global Properties map. See docs for {@link #setGlobalProperties(java.util.Map)}.
     */
    public Map<String, Object> getGlobalProperties() {
        return globalProperties;
    }

    /**
     * Call this to set the Keen Global Properties Map for this instance of the {@link KeenClient}. The Map
     * is used every time an event is added to an event collection.
     * <p/>
     * Keen Global Properties are properties which are sent with EVERY event. For example, you may wish to always
     * capture static information like user ID, app version, etc.
     * <p/>
     * Every time an event is added to an event collection, the SDK will check to see if this property is defined.
     * If it is, the SDK will copy all the properties from the global properties into the newly added event.
     * <p/>
     * Note that because this is just a Map, it's much more difficult to create DYNAMIC global properties.
     * It also doesn't support per-collection properties. If either of these use cases are important to you, please use
     * the {@link GlobalPropertiesEvaluator}.
     * <p/>
     * Also note that the Keen properties defined in {@link #getGlobalPropertiesEvaluator()} take precedence over
     * the properties defined in getGlobalProperties, and that the Keen Properties defined in each
     * individual event take precedence over either of the Global Properties.
     * <p/>
     * Example usage:
     * <p/>
     * <pre>
     * KeenClient client = KeenClient.client();
     * Map<String, Object> map = new HashMap<String, Object>();
     * map.put("some standard key", "some standard value");
     * client.setGlobalProperties(map);
     * </pre>
     *
     * @param globalProperties The new map you wish to use as the Keen Global Properties.
     */
    public void setGlobalProperties(Map<String, Object> globalProperties) {
        this.globalProperties = globalProperties;
    }

    ///// PROTECTED METHODS /////

    protected Map<String, Object> validateAndBuildEvent(KeenProject project,
            String eventCollection, Map<String, Object> event, Map<String, Object> keenProperties) {

        if (project.getWriteKey() == null) {
            throw new NoWriteKeyException("You can't send events to Keen IO if you haven't set a write key.");
        }

        validateEventCollection(eventCollection);
        validateEvent(event);

        KeenLogging.log(String.format("Adding event to collection: %s", eventCollection));

        // build the event
        Map<String, Object> newEvent = new HashMap<String, Object>();
        // handle keen properties
        Calendar currentTime = Calendar.getInstance();
        String timestamp = ISO_8601_FORMAT.format(currentTime.getTime());
        if (keenProperties == null) {
            keenProperties = new HashMap<String, Object>();
            keenProperties.put("timestamp", timestamp);
        } else {
            if (!keenProperties.containsKey("timestamp")) {
                keenProperties.put("timestamp", timestamp);
            }
        }
        newEvent.put("keen", keenProperties);

        // handle global properties
        Map<String, Object> globalProperties = getGlobalProperties();
        if (globalProperties != null) {
            newEvent.putAll(globalProperties);
        }
        GlobalPropertiesEvaluator globalPropertiesEvaluator = getGlobalPropertiesEvaluator();
        if (globalPropertiesEvaluator != null) {
            Map<String, Object> props = globalPropertiesEvaluator.getGlobalProperties(eventCollection);
            if (props != null) {
                newEvent.putAll(props);
            }
        }

        // now handle user-defined properties
        newEvent.putAll(event);
        return newEvent;
    }

    protected KeenJsonHandler getJsonHandler() {
        return jsonHandler;
    }

    ///// PRIVATE TYPES /////

    private enum ClientSingleton {
        INSTANCE;
        KeenClient client;
    }

    ///// PRIVATE CONSTANTS /////

    private static final DateFormat ISO_8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final String ENCODING = "UTF-8";

    ///// PRIVATE FIELDS /////

    // TODO: Set this flag to false if the library is not operational for any reason.
    private boolean isActive = true;
    private boolean isDebugMode;
    private final Executor publishExecutor;
    private final KeenEventStore eventStore;
    private final KeenJsonHandler jsonHandler;
    private KeenProject defaultProject;
    private String baseUrl;
    private GlobalPropertiesEvaluator globalPropertiesEvaluator;
    private Map<String, Object> globalProperties;

    ///// PRIVATE CONSTRUCTORS /////

    /**
     * Constructs the singleton instance of the Keen client. This constructor is private to prevent
     * additional instances from being created.
     */
    private KeenClient(KeenJsonHandler jsonHandler, KeenEventStore eventStore,
                       Executor publishExecutor) {
        // TODO: Validate that JSON handler exists?
        this.jsonHandler = jsonHandler;
        this.eventStore = eventStore;
        this.publishExecutor = publishExecutor;
        this.baseUrl = KeenConstants.SERVER_ADDRESS;
        this.globalPropertiesEvaluator = null;
        this.globalProperties = null;
    }

    ///// PRIVATE METHODS /////

    private void validateEventCollection(String eventCollection) {
        if (eventCollection == null || eventCollection.length() == 0) {
            throw new InvalidEventCollectionException("You must specify a non-null, " +
                    "non-empty event collection: " + eventCollection);
        }
        if (eventCollection.startsWith("$")) {
            throw new InvalidEventCollectionException("An event collection name cannot start with the dollar sign ($)" +
                    " character.");
        }
        if (eventCollection.length() > 256) {
            throw new InvalidEventCollectionException("An event collection name cannot be longer than 256 characters.");
        }
    }

    private void validateEvent(Map<String, Object> event) {
        validateEvent(event, 0);
    }

    @SuppressWarnings("unchecked") // cast to generic Map will always be okay in this case
    private void validateEvent(Map<String, Object> event, int depth) {
        if (depth == 0) {
            if (event == null || event.size() == 0) {
                throw new InvalidEventException("You must specify a non-null, non-empty event.");
            }
            if (event.containsKey("keen")) {
                throw new InvalidEventException("An event cannot contain a root-level property named 'keen'.");
            }
        }

        for (Map.Entry<String, Object> entry : event.entrySet()) {
            String key = entry.getKey();
            if (key.contains(".")) {
                throw new InvalidEventException("An event cannot contain a property with the period (.) character in " +
                        "it.");
            }
            if (key.startsWith("$")) {
                throw new InvalidEventException("An event cannot contain a property that starts with the dollar sign " +
                        "($) character in it.");
            }
            if (key.length() > 256) {
                throw new InvalidEventException("An event cannot contain a property name longer than 256 characters.");
            }
            Object value = entry.getValue();
            if (value instanceof String) {
                String strValue = (String) value;
                if (strValue.length() >= 10000) {
                    throw new InvalidEventException("An event cannot contain a string property value longer than 10," +
                            "000 characters.");
                }
            } else if (value instanceof Map) {
                validateEvent((Map<String, Object>) value, depth + 1);
            }
        }
    }

    private String publish(KeenProject project, String eventCollection,
                           Map<String, Object> event) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format("%s/%s/projects/%s/events/%s", getBaseUrl(),
                KeenConstants.API_VERSION, project.getProjectId(), eventCollection);
        URL url = new URL(urlString);
        return publishObject(project, url, event);
    }

    private String publishAll(KeenProject project,
                              Map<String, List<Map<String, Object>>> events) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format("%s/%s/projects/%s/events", getBaseUrl(),
                KeenConstants.API_VERSION, project.getProjectId());
        URL url = new URL(urlString);
        return publishObject(project, url, events);
    }

    private synchronized String publishObject(KeenProject project, URL url,
                                              Map<String, ?> requestData) throws IOException {
        if (requestData == null || requestData.size() == 0) {
            KeenLogging.log("No API calls were made because there were no events to upload");
            return null;
        }

        // set up the POST
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Authorization", project.getWriteKey());

        // write JSON to the output stream
        OutputStreamWriter writer = null;
        try {
            writer = new OutputStreamWriter(connection.getOutputStream(), ENCODING);
            jsonHandler.writeJson(writer, requestData);
        } finally {
            KeenUtils.closeQuietly(writer);
        }

        // Check whether the response succeeded.
        int responseCode = connection.getResponseCode();
        String response = null;
        if (responseCode / 100 == 2) {
            InputStream responseStream = connection.getInputStream();
            try {
                response = KeenUtils.convertStreamToString(responseStream);
                KeenLogging.log("Response: " + response);
            } finally {
                KeenUtils.closeQuietly(responseStream);
                connection.disconnect();
            }
        } else {
            KeenLogging.log(String.format("Response code was NOT 200. It was: %d", responseCode));

            // Initialize the response to a default string, in case the error message can't be read.
            response = "Server error: " + responseCode;

            // Try to read the error message.
            InputStream errorStream = connection.getErrorStream();
            try {
                response = KeenUtils.convertStreamToString(errorStream);
                KeenLogging.log(String.format("Error response body was: %s", response));
            } finally {
                KeenUtils.closeQuietly(errorStream);
                connection.disconnect();
            }

            // Throw an exception to indicate the request failed.
            throw new ServerException(response);
        }

        return response;
    }

    /**
     *
     * @param handles
     * @param response
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private void handleAddEventsResponse(Map<String, List<Object>> handles, String response) throws IOException {
        // Parse the response into a map.
        StringReader reader = new StringReader(response);
        Map<String, Object> responseMap;
        responseMap = jsonHandler.readJson(reader);

        // TODO: Wrap the various unsafe casts used below in try/catch(ClassCastException) blocks?
        // It's not obvious what the best way is to try and recover from them, but just hoping it
        // doesn't happen is probably the wrong answer.

        // Loop through all the event collections.
        for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
            String collectionName = entry.getKey();

            // Get the list of handles in this collection.
            List<Object> collectionHandles = handles.get(collectionName);

            // Iterate through the elements in the collection
            List<Map<String, Object>> eventResults = (List<Map<String, Object>>) entry.getValue();
            int index = 0;
            for (Map<String, Object> eventResult : eventResults) {
                // now loop through each event collection's individual results
                boolean removeCacheEntry = true;
                boolean success = (Boolean) eventResult.get(KeenConstants.SUCCESS_PARAM);
                if (!success) {
                    // grab error code and description
                    Map errorDict = (Map) eventResult.get(KeenConstants.ERROR_PARAM);
                    String errorCode = (String) errorDict.get(KeenConstants.NAME_PARAM);
                    if (errorCode.equals(KeenConstants.INVALID_COLLECTION_NAME_ERROR) ||
                            errorCode.equals(KeenConstants.INVALID_PROPERTY_NAME_ERROR) ||
                            errorCode.equals(KeenConstants.INVALID_PROPERTY_VALUE_ERROR)) {
                        removeCacheEntry = true;
                        KeenLogging.log("An invalid event was found. Deleting it. Error: " +
                                errorDict.get(KeenConstants.DESCRIPTION_PARAM));
                    } else {
                        String description = (String) errorDict.get(KeenConstants.DESCRIPTION_PARAM);
                        removeCacheEntry = false;
                        KeenLogging.log(String.format("The event could not be inserted for some reason. " +
                                "Error name and description: %s %s", errorCode,
                                description));
                    }
                }

                // If the cache entry should be removed, get the handle at the appropriate index
                // and ask the event store to remove it.
                if (removeCacheEntry) {
                    Object handle = collectionHandles.get(index);
                    // Try to remove the object from the cache. Catch and log exceptions to prevent
                    // a single failure from derailing the rest of the cleanup.
                    try {
                        eventStore.removeFromCache(handle);
                    } catch (IOException e) {
                        KeenLogging.log("Failed to remove object '" + handle + "' from cache");
                    }
                }
                index++;
            }
        }
    }

    private void handleSuccess(KeenCallback callback) {
        if (callback != null) {
            try {
                callback.onSuccess();
            } catch (Exception userException) {
                // Do nothing.
            }
        }
    }

    private void handleFailure(KeenCallback callback, Exception e) {
        if (isDebugMode) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } else {
            KeenLogging.log("Encountered error: " + e.getMessage());
            if (callback != null) {
                try {
                    callback.onFailure(e);
                } catch (Exception userException) {
                    // Do nothing.
                }
            }
        }
    }

    // TODO: Cap how many times this failure is reported, and after that just fail silently.
    private void handleLibraryInactive(KeenCallback callback) {
        handleFailure(callback, new IllegalStateException("The Keen library failed to initialize " +
                "properly and is inactive"));
    }

}
