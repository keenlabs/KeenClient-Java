package io.keen.client.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
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
public abstract class KeenClient {

    ///// PROTECTED ABSTRACT METHODS /////

    /**
     * Creates a {@link io.keen.client.java.KeenJsonHandler} which will be used for the life of the
     * client to handle reading and writing JSON.
     *
     * @return A new {@link io.keen.client.java.KeenJsonHandler}.
     */
    protected abstract KeenJsonHandler instantiateJsonHandler();

    /**
     * Creates a {@link io.keen.client.java.KeenEventStore} which will be used for the life of the
     * client to handle storing events in between batch posts.
     *
     * @return A new {@link io.keen.client.java.KeenEventStore}.
     */
    protected abstract KeenEventStore instantiateEventStore();

    /**
     * Creates an {@link java.util.concurrent.Executor} which will be used for the life of the
     * client to process asynchronous requests.
     *
     * @return A new {@link java.util.concurrent.Executor}.
     */
    protected abstract Executor instantiatePublishExecutor();

    ///// PUBLIC STATIC METHODS /////

    /**
     * Call this to retrieve the {@code KeenClient} singleton instance.
     *
     * @return The singleton instance of the client.
     */
    public static KeenClient client() {
        if (ClientSingleton.INSTANCE.client == null) {
            throw new IllegalStateException("Please call KeenClient.initialize() before requesting the client.");
        }
        return ClientSingleton.INSTANCE.client;
    }

    ///// PUBLIC METHODS //////

    /**
     * Adds an event to the default project with default Keen properties and no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     */
    public void addEvent(String eventCollection, Map<String, Object> event) {
        addEvent(eventCollection, event, null);
    }

    /**
     * Adds an event to the default project with no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     */
    public void addEvent(String eventCollection, Map<String, Object> event,
                         Map<String, Object> keenProperties) {
        addEvent(null, eventCollection, event, keenProperties, null);
    }

    /**
     * Synchronously adds an event to the specified collection. This method will immediately
     * publish the event to the Keen server in the current thread.
     *
     * @param project The project in which to publish the event. If a default project has been set
     *                on the client, this parameter may be null, in which case the default project
     *                will be used.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event A Map that consists of key/value pairs. Keen naming conventions apply (see
     *              docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties A Map that consists of key/value pairs to override default properties.
     *                       ex: "timestamp" -> Calendar.getInstance()
     * @param callback An optional callback to receive notification of success or failure.
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

    /**
     * Adds an event to the default project with default Keen properties and no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     */
    public void addEventAsync(String eventCollection, Map<String, Object> event) {
        addEventAsync(eventCollection, event, null);
    }

    /**
     * Adds an event to the default project with no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     */
    public void addEventAsync(String eventCollection, Map<String, Object> event,
                              final Map<String, Object> keenProperties) {
        addEventAsync(null, eventCollection, event, keenProperties, null);
    }

    /**
     * Asynchronously adds an event to the specified collection. This method will request that
     * the Keen client's {@link java.util.concurrent.Executor} executes the publish operation.
     *
     * @param project The project in which to publish the event. If a default project has been set
     *                on the client this parameter may be null, in which case the default project
     *                will be used.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event A Map that consists of key/value pairs. Keen naming conventions apply (see
     *              docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties A Map that consists of key/value pairs to override default properties.
     *                       ex: "timestamp" -> Calendar.getInstance()
     * @param callback An optional callback to receive notification of success or failure.
     */
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

    /**
     * Queues an event in the default project with default Keen properties and no callbacks.
     *
     * @see #queueEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     */
    public void queueEvent(String eventCollection, Map<String, Object> event) {
        queueEvent(eventCollection, event, null);
    }

    /**
     * Queues an event in the default project with no callbacks.
     *
     * @see #queueEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     */
    public void queueEvent(String eventCollection, Map<String, Object> event,
                           Map<String, Object> keenProperties) {
        queueEvent(null, eventCollection, event, keenProperties, null);
    }

    /**
     * Synchronously queues an event for publishing. The event will be cached in the client's
     * {@link io.keen.client.java.KeenEventStore} until the next call to either
     * {@link #sendQueuedEvents()} or {@link #sendQueuedEventsAsync()}.
     *
     * @param project The project in which to publish the event. If a default project has been set
     *                on the client this parameter may be null, in which case the default project
     *                will be used.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event A Map that consists of key/value pairs. Keen naming conventions apply (see
     *              docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties A Map that consists of key/value pairs to override default properties.
     *                       ex: "timestamp" -> Calendar.getInstance()
     * @param callback An optional callback to receive notification of success or failure.
     */
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

        try {
            // Build the event
            Map<String, Object> newEvent =
                    validateAndBuildEvent(useProject, eventCollection, event, keenProperties);

            // Write the event out to the event store.
            eventStore.store(eventCollection, newEvent);
            handleSuccess(callback);
        } catch (Exception e) {
            handleFailure(callback, e);
        }
    }

    /**
     * Sends all queued events for the default project with no callbacks.
     *
     * @see #sendQueuedEvents(KeenProject, KeenCallback)
     */
    public void sendQueuedEvents() {
        sendQueuedEvents(null);
    }

    /**
     * Sends all queued events for the specified project with no callbacks.
     *
     * @see #sendQueuedEvents(KeenProject, KeenCallback)
     */
    public void sendQueuedEvents(KeenProject project) {
        sendQueuedEvents(project, null);
    }

    /**
     * Synchronously sends all queued events for the given project. This method will immediately
     * publish the events to the Keen server in the current thread.
     *
     * @param project The project for which to send queued events. If a default project has been set
     *                on the client this parameter may be null, in which case the default project
     *                will be used.
     * @param callback An optional callback to receive notification of success or failure.
     */
    public synchronized void sendQueuedEvents(KeenProject project, KeenCallback callback) {

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
            Map<String, List<Object>> eventHandles = eventStore.getHandles();
            Map<String, List<Map<String, Object>>> events = buildEventMap(eventHandles);
            String response = publishAll(useProject, events);
            if (response != null) {
                try {
                    handleAddEventsResponse(eventHandles, response);
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

    /**
     * Sends all queued events for the default project with no callbacks.
     *
     * @see #sendQueuedEventsAsync(KeenProject, KeenCallback)
     */
    public void sendQueuedEventsAsync() {
        sendQueuedEventsAsync(null);
    }

    /**
     * Sends all queued events for the specified project with no callbacks.
     *
     * @see #sendQueuedEventsAsync(KeenProject, KeenCallback)
     */
    public void sendQueuedEventsAsync(final KeenProject project) {
        sendQueuedEventsAsync(project, null);
    }

    /**
     * Asynchronously sends all queued events for the given project. This method will request that
     * the Keen client's {@link java.util.concurrent.Executor} executes the publish operation.
     *
     * @param project The project for which to send queued events. If a default project has been set
     *                on the client this parameter may be null, in which case the default project
     *                will be used.
     * @param callback An optional callback to receive notification of success or failure.
     */
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
     * Gets the default project that this {@link KeenClient} will use if no project is specified.
     *
     * @return The default project.
     */
    public KeenProject getDefaultProject() {
        return defaultProject;
    }

    /**
     * Sets the default project that this {@link KeenClient} should use if no project is specified.
     *
     * @param defaultProject The new default project.
     */
    public void setDefaultProject(KeenProject defaultProject) {
        this.defaultProject = defaultProject;
    }

    /**
     * Gets the base API URL associated with this instance of the {@link KeenClient}.
     *
     * @return The base API URL
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * Sets the base API URL associated with this instance of the {@link KeenClient}.
     * <p/>
     * Use this if you want to disable SSL.
     *
     * @param baseUrl The new base URL (i.e. 'http://api.keen.io')
     */
    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Gets the {@link GlobalPropertiesEvaluator} associated with this instance of the {@link KeenClient}.
     *
     * @return The {@link GlobalPropertiesEvaluator}
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
     * Gets the Keen Global Properties map. See docs for {@link #setGlobalProperties(java.util.Map)}.
     *
     * @return The Global Properties map.
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

    /**
     * Sets whether or not the Keen client should run in debug mode. When debug mode is enabled,
     * all exceptions will be thrown immediately; otherwise they will be logged and reported to
     * any callbacks, but never thrown.
     *
     * @param isDebugMode
     */
    public void setDebugMode(boolean isDebugMode) {
        this.isDebugMode = isDebugMode;
    }

    ///// PROTECTED CONSTRUCTORS /////

    /**
     * Constructs the singleton instance of the Keen client. This constructor is private to prevent
     * additional instances from being created.
     */
    protected KeenClient() {
        this.baseUrl = KeenConstants.SERVER_ADDRESS;
        this.globalPropertiesEvaluator = null;
        this.globalProperties = null;
    }

    ///// PROTECTED METHODS /////

    /**
     * Initializes the Keen client. This method is intended to be called by implementations of
     * the {@link io.keen.client.java.KeenClient} abstract class, which create an instance that
     * will become the singleton.
     *
     * Only the first call to this method has any effect. All subsequent calls are ignored.
     *
     * @param client The {@link io.keen.client.java.KeenClient} implementation to use as the
     *               singleton client for the library.
     */
    protected static void initialize(KeenClient client) {
        if (client == null) {
            throw new IllegalArgumentException("Client must not be null");
        }

        if (ClientSingleton.INSTANCE.client != null) {
            // Do nothing.
            return;
        }

        ClientSingleton.INSTANCE.client = client;

        // TODO: Validate that JSON handler exists?
        // Note: the order of instantiation is important. The JSON handler must be instantiated
        // first because the event store may depend upon it.
        client.jsonHandler = client.instantiateJsonHandler();
        client.eventStore = client.instantiateEventStore();
        client.publishExecutor = client.instantiatePublishExecutor();
    }

    /**
     * Validates an event and inserts global properties, producing a new event object which is
     * ready to be published to the Keen service.
     *
     * @param project The project in which the event will be published.
     * @param eventCollection The name of the collection in which the event will be published.
     * @param event A Map that consists of key/value pairs.
     * @param keenProperties A Map that consists of key/value pairs to override default properties.
     * @return A new event Map containing Keen properties and global properties.
     */
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

    /**
     * Gets the {@link io.keen.client.java.KeenJsonHandler} for this client. This method is
     * available to subclasses and other library components in case they need to perform JSON
     * operations, such as to serialize events to files on disk.
     *
     * @return The client's {@link io.keen.client.java.KeenJsonHandler}.
     */
    protected KeenJsonHandler getJsonHandler() {
        return jsonHandler;
    }

    ///// PRIVATE TYPES /////

    /**
     * The {@link io.keen.client.java.KeenClient} class's singleton enum.
     */
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
    private Executor publishExecutor;
    private KeenEventStore eventStore;
    private KeenJsonHandler jsonHandler;
    private KeenProject defaultProject;
    private String baseUrl;
    private GlobalPropertiesEvaluator globalPropertiesEvaluator;
    private Map<String, Object> globalProperties;

    ///// PRIVATE METHODS /////

    /**
     * Validates the name of an event collection.
     *
     * @param eventCollection An event collection name to be validated.
     *
     * @throws io.keen.client.java.exceptions.InvalidEventCollectionException
     *     If the event collection name is invalid. See Keen documentation for details.
     */
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

    /**
     * @see #validateEvent(java.util.Map, int)
     */
    private void validateEvent(Map<String, Object> event) {
        validateEvent(event, 0);
    }

    /**
     * Validates an event.
     *
     * @param event The event to validate.
     * @param depth The number of layers of the map structure that have already been traversed; this
     *              should be 0 for the initial call and will increment on each recursive call.
     */
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
            // TODO: Validate Iterable objects?
        }
    }

    /**
     * Builds a map from collection name to a list of event maps, given a map from collection name
     * to a list of event handles. This method just uses the event store to retrieve each event by
     * its handle.
     *
     * @param eventHandles A map from collection name to a list of event handles in the event store.
     * @return A map from collection name to a list of event maps.
     * @throws IOException If there is an error retrieving events from the store.
     */
    private Map<String, List<Map<String, Object>>> buildEventMap(
            Map<String, List<Object>> eventHandles) throws IOException {
        Map<String, List<Map<String, Object>>> result =
                new HashMap<String, List<Map<String, Object>>>();
        for (Map.Entry<String, List<Object>> entry : eventHandles.entrySet()) {
            String eventCollection = entry.getKey();
            List<Object> handles = entry.getValue();

            // Skip event collections that don't contain any events.
            if (handles == null || handles.size() == 0) {
                continue;
            }

            // Build the event list by retrieving events from the store.
            List<Map<String, Object>> events = new ArrayList<Map<String, Object>>(handles.size());
            for (Object handle : handles) {
                events.add(eventStore.get(handle));
            }
            result.put(eventCollection, events);
        }
        return result;
    }

    /**
     * Publishes a single event to the Keen service.
     *
     * @param project The project in which to publish the event.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event The event to publish.
     * @return The response from the server.
     * @throws IOException If there was an error communicating with the server.
     */
    private String publish(KeenProject project, String eventCollection,
                           Map<String, Object> event) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format("%s/%s/projects/%s/events/%s", getBaseUrl(),
                KeenConstants.API_VERSION, project.getProjectId(), eventCollection);
        URL url = new URL(urlString);
        return publishObject(project, url, event);
    }

    /**
     * Publishes a batch of events to the Keen service.
     *
     * @param project The project in which to publish the event.
     * @param events A map from collection name to a list of event maps.
     * @return The response from the server.
     * @throws IOException If there was an error communicating with the server.
     */
    private String publishAll(KeenProject project,
                              Map<String, List<Map<String, Object>>> events) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format("%s/%s/projects/%s/events", getBaseUrl(),
                KeenConstants.API_VERSION, project.getProjectId());
        URL url = new URL(urlString);
        return publishObject(project, url, events);
    }

    /**
     * Posts a request to the server in the specified project, using the given URL and request data.
     * The request data will be serialized into JSON using the client's
     * {@link io.keen.client.java.KeenJsonHandler}.
     *
     * @param project The project in which the event(s) will be published; this is used to
     *                determine the write key to use for authentication.
     * @param url The URL to which the POST should be sent.
     * @param requestData The request data, which will be serialized into JSON and sent in the
     *                    request body.
     * @return The response from the server.
     * @throws IOException If there was an error communicating with the server.
     */
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
     * Handles a response from the Keen service to a batch post events operation. In particular,
     * this method will iterate through the responses and remove any successfully processed events
     * (or events which failed for known fatal reasons) from the event store so they won't be sent
     * in subsequent posts.
     *
     * @param handles A map from collection names to lists of handles in the event store. This is
     *                referenced against the response from the server to determine which events to
     *                remove from the store.
     * @param response The response from the server.
     * @throws IOException If there is an error removing events from the store.
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
                        eventStore.remove(handle);
                    } catch (IOException e) {
                        KeenLogging.log("Failed to remove object '" + handle + "' from cache");
                    }
                }
                index++;
            }
        }
    }

    /**
     * Reports success to a callback. If the callback is null, this is a no-op. Any exceptions
     * thrown by the callback are silently ignored.
     *
     * @param callback A callback; may be null.
     */
    private void handleSuccess(KeenCallback callback) {
        if (callback != null) {
            try {
                callback.onSuccess();
            } catch (Exception userException) {
                // Do nothing.
            }
        }
    }

    /**
     * Handles a failure in the Keen library. If the client is running in debug mode, this will
     * immediately throw a runtime exception. Otherwise, this will log an error message and, if the
     * callback is non-null, call the {@link KeenCallback#onFailure(Exception)} method. Any
     * exceptions thrown by the callback are silently ignored.
     *
     * @param callback A callback; may be null.
     * @param e The exception which caused the failure.
     */
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

    /**
     * Reports failure when the library is inactive due to failed initialization.
     *
     * @param callback A callback; may be null.
     */
    // TODO: Cap how many times this failure is reported, and after that just fail silently.
    private void handleLibraryInactive(KeenCallback callback) {
        handleFailure(callback, new IllegalStateException("The Keen library failed to initialize " +
                "properly and is inactive"));
    }

}
