package io.keen.client.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import io.keen.client.java.exceptions.InvalidEventCollectionException;
import io.keen.client.java.exceptions.InvalidEventException;
import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.NoWriteKeyException;

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

    private static final String ENCODING = "UTF-8";

    public interface KeenJsonHandler {
        Map<String, Object> readJson(Reader reader) throws IOException;
        void writeJson(Writer writer, Map<String, ?> value) throws IOException;
    }

    public interface EventStore {
        OutputStream getCacheOutputStream(String eventCollection) throws IOException;
        CacheEntries retrieveCached() throws IOException;
        void removeFromCache(Object handle) throws IOException;
    }

    public static class CacheEntries {
        public final Map<String, List<Object>> handles;
        public final Map<String, List<Map<String, Object>>> events;
        public CacheEntries(Map<String, List<Object>> handles,
                            Map<String, List<Map<String, Object>>> events) {
            this.handles = handles;
            this.events = events;
        }
    }

    public static class KeenClientInterfaces {
        // TODO: Should these be public?
        public KeenJsonHandler jsonHandler;
        public EventStore eventStore;
        public Executor publishExecutor;
    }

    // TODO: Should this be static?
    public static KeenClientInterfaces interfaces;

    static {
        initialize();
    }

    enum ClientSingleton {
        INSTANCE;
        KeenClient client;
    }

    /**
     * Use this to attempt to initialize the client from environment variables.
     */
    public static void initialize() {
        initialize(new Environment());
    }

    /**
     * Used for tests.
     */
    static void initialize(Environment env) {
        if (env.getKeenProjectId() != null) {
            KeenClient.initialize(env.getKeenProjectId(),
                    env.getKeenWriteKey(),
                    env.getKeenReadKey());
        }
    }

    /**
     * Call this to initialize the singleton instance of KeenClient and set its Project Id.
     * <p/>
     * You'll generally want to call this at the very beginning of your application's lifecycle. Once you've called
     * this, you can then call KeenClient.client() afterwards.
     *
     * @param projectId The Keen IO Project Id.
     * @param writeKey  Your Keen IO Write Key.
     * @param readKey   Your Keen IO Read Key.
     */
    public static void initialize(String projectId, String writeKey, String readKey) {
        ClientSingleton.INSTANCE.client = new KeenClient(projectId, writeKey, readKey);
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
            throw new IllegalStateException("Please call KeenClient.initialize() before requesting the shared client.");
        }
        return ClientSingleton.INSTANCE.client;
    }

    /////////////////////////////////////////////

    private final String projectId;
    private final String writeKey;
    private final String readKey;
    private String baseUrl;
    private GlobalPropertiesEvaluator globalPropertiesEvaluator;
    private Map<String, Object> globalProperties;

    /**
     * Call this if your code needs to use more than one Keen project and API Key (or if you don't want to use
     * the managed, singleton instance provided by this library).
     *
     * @param projectId The Keen IO Project ID.
     * @param writeKey  Your Keen IO Write Key.
     * @param readKey   Your Keen IO Read Key.
     */
    public KeenClient(String projectId, String writeKey, String readKey) {
        if (projectId == null || projectId.length() == 0) {
            throw new IllegalArgumentException("Invalid project id specified: " + projectId);
        }

        this.projectId = projectId;
        this.writeKey = writeKey;
        this.readKey = readKey;
        this.baseUrl = KeenConstants.SERVER_ADDRESS;
        this.globalPropertiesEvaluator = null;
        this.globalProperties = null;
    }

    /////////////////////////////////////////////

    /**
     * Call this any time you want to add an event that will be sent to the Keen IO server.
     *
     * @param eventCollection The name of the event collection you want to put this event into.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see docs).
     *                        Nested Maps and lists are acceptable (and encouraged!).
     * @throws KeenException
     */
    public void addEvent(String eventCollection, Map<String, Object> event) throws KeenException {
        addEvent(eventCollection, event, null, null);
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
     * @param callback        An instance of AddEventCallback. Will invoke onSuccess when adding the event succeeds.
     *                        Will invoke onError when adding the event fails.
     * @throws KeenException
     */
    public void addEvent(String eventCollection, Map<String, Object> event, Map<String, Object> keenProperties,
                         AddEventCallback callback) throws KeenException {
        // Build the event
        Map<String, Object> newEvent = validateAndBuildEvent(eventCollection, event, keenProperties);

        try {
            String response = publish(eventCollection, newEvent);
            if (response == null) {
                // TODO: Handle failure.
            } else {
                // TODO: Handle success.
            }
        } catch (IOException e) {
            // TODO: How should this be handled?
            KeenLogging.log("Failed to serialize event");
        }

        // TODO: Use the callback.
    }

    public void addEventAsync(final String eventCollection, final Map<String, Object> event) throws KeenException {
        interfaces.publishExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    addEvent(eventCollection, event);
                } catch (KeenException e) {
                    // TODO: Decide how to handle exceptions here.
                }
            }
        });
    }

    public void queueEvent(String eventCollection, Map<String, Object> event) throws KeenException {
        queueEvent(eventCollection, event, null);
    }

    public void queueEvent(String eventCollection, Map<String, Object> event,
                           Map<String, Object> keenProperties) throws KeenException {
        // Build the event
        Map<String, Object> newEvent = validateAndBuildEvent(eventCollection, event, keenProperties);

        OutputStreamWriter writer = null;
        try {
            OutputStream out = interfaces.eventStore.getCacheOutputStream(eventCollection);
            writer = new OutputStreamWriter(out, ENCODING);
            interfaces.jsonHandler.writeJson(writer, newEvent);
        } catch (IOException e) {
            // TODO: How should this be handled?
            KeenLogging.log("Failed to serialize event");
        } finally {
            KeenUtils.closeQuietly(writer);
        }
    }

    public void sendQueuedEvents() throws KeenException {
        try {
            CacheEntries entries = interfaces.eventStore.retrieveCached();
            Map<String, List<Map<String, Object>>> events = entries.events;
            String response = publishAll(events);
            if (response == null) {
                // TODO: Handle request failure.
            } else {
                handleAddEventsResponse(entries.handles, response);
            }
        } catch (IOException e) {
            // TODO: How should this be handled?
            KeenLogging.log("Failed to serialize event");
        }
    }

    public void sendQueuedEventsAsync() throws KeenException {
        interfaces.publishExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendQueuedEvents();
                } catch (KeenException e) {
                    // TODO: Return this or call a handler or something?
                }
            }
        });
    }

    private static final DateFormat ISO_8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    Map<String, Object> validateAndBuildEvent(String eventCollection, Map<String, Object> event,
                                              Map<String, Object> keenProperties) throws KeenException {
        if (getWriteKey() == null) {
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

    private void validateEventCollection(String eventCollection) throws InvalidEventCollectionException {
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

    private void validateEvent(Map<String, Object> event) throws InvalidEventException {
        validateEvent(event, 0);
    }

    @SuppressWarnings("unchecked") // cast to generic Map will always be okay in this case
    private void validateEvent(Map<String, Object> event, int depth) throws InvalidEventException {
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

    private String publish(String eventCollection, Map<String, Object> event) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format("%s/%s/projects/%s/events/%s", getBaseUrl(),
                KeenConstants.API_VERSION, getProjectId(), eventCollection);
        URL url = new URL(urlString);
        return publishObject(url, event);
    }

    private String publishAll(Map<String, List<Map<String, Object>>> events) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format("%s/%s/projects/%s/events", getBaseUrl(),
                KeenConstants.API_VERSION, getProjectId());
        URL url = new URL(urlString);
        return publishObject(url, events);
    }

    private synchronized String publishObject(URL url, Map<String, ?> requestData) throws IOException {
        // set up the POST
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Authorization", getWriteKey());
        // TODO: Set a user agent?

        // write JSON to the output stream
        OutputStreamWriter writer = null;
        try {
            writer = new OutputStreamWriter(connection.getOutputStream(), ENCODING);
            interfaces.jsonHandler.writeJson(writer, requestData);
        } finally {
            KeenUtils.closeQuietly(writer);
        }

        int responseCode = connection.getResponseCode();
        String response = null;
        if (responseCode / 100 == 2) {
            InputStream responseStream = connection.getInputStream();
            try {
                response = KeenUtils.convertStreamToString(responseStream);
                KeenLogging.log("Response: " + response);
            } finally {
                KeenUtils.closeQuietly(responseStream);
            }
        } else {
            InputStream errorStream = connection.getErrorStream();
            try {
                String error = KeenUtils.convertStreamToString(errorStream);
                KeenLogging.log("Error: " + error);
            } finally {
                KeenUtils.closeQuietly(errorStream);
            }
        }

        connection.disconnect();
        return response;
    }

    /*
    TODO: Is some of this logic useful for processing the error response? If so, make use of it
    from publish and/or publishObject. Otherwise, remove this dead code.

    private boolean parseAddEventResponse(String response) throws IOException {
        // Parse the response into a map.
        StringReader reader = new StringReader(response);
        Map<String, Object> responseMap = interfaces.jsonHandler.readJson(reader);

        // TODO: Move this logic into a shared helper? Unfortunately this is a bit tricky because
        // of the different structure of the error responses.
        boolean success = (Boolean) responseMap.get(KeenConstants.CREATED_PARAM);
        if (!success) {
            // grab error code and description
            String errorCode = (String) responseMap.get(KeenConstants.ERROR_CODE_PARAM);
            String description = (String) responseMap.get(KeenConstants.MESSAGE_PARAM);
            if (errorCode.equals(KeenConstants.INVALID_COLLECTION_NAME_ERROR) ||
                    errorCode.equals(KeenConstants.INVALID_PROPERTY_NAME_ERROR) ||
                    errorCode.equals(KeenConstants.INVALID_PROPERTY_VALUE_ERROR)) {
                KeenLogging.log("An invalid event was found. Deleting it. Error: " + description);
            } else {
                KeenLogging.log(String.format("The event could not be inserted for some reason. " +
                        "Error name and description: %s %s", errorCode, description));
            }
        }

        return success;
    }
    */

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
        Map<String, Object> responseMap = interfaces.jsonHandler.readJson(reader);

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
                    // TODO: Error handling?
                    interfaces.eventStore.removeFromCache(handle);
                }
                index++;
            }
        }
    }

    /**
     * Getter for the Keen Project Id associated with this instance of the {@link KeenClient}.
     *
     * @return the Keen Project Id
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Getter for the Keen Write Key associated with this instance of the {@link KeenClient}.
     *
     * @return the Keen Write Key
     */
    public String getWriteKey() {
        return writeKey;
    }

    /**
     * Getter for the Keen Read Key associated with this instance of the {@link KeenClient}.
     *
     * @return the Keen Read Key
     */
    public String getReadKey() {
        return readKey;
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

}
