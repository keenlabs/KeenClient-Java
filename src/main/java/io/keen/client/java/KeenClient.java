package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.keen.client.java.exceptions.InvalidEventCollectionException;
import io.keen.client.java.exceptions.InvalidEventException;
import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.NoWriteKeyException;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    static final ObjectMapper MAPPER;
    static ExecutorService EXECUTOR_SERVICE;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    private enum ClientSingleton {
        INSTANCE;
        private KeenClient client;
    }
    
    protected ExecutorService createExecutorService() {
        return Executors.newFixedThreadPool(KeenConfig.NUM_THREADS_FOR_HTTP_REQUESTS);
    }
    
    private void checkExecutorService() {
        if (EXECUTOR_SERVICE == null || EXECUTOR_SERVICE.isShutdown()) {
            EXECUTOR_SERVICE = createExecutorService();
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
        this.globalPropertiesEvaluator = null;
        this.globalProperties = null;
        
        checkExecutorService();
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
        // get the event
        Map<String, Object> newEvent = validateAndBuildEvent(eventCollection, event, keenProperties);
        // send the request as a callable in another thread
        processRunnableInNewThread(new KeenHttpRequestRunnable(this, eventCollection, newEvent, callback));
    }

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
        Calendar timestamp = Calendar.getInstance();
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

    /**
     * Responsible for taking a {@link Runnable} and running it a new thread.
     * <p/>
     * Default implementation uses an {@link ExecutorService} to manage a thread pool and submit jobs to that
     * thread pool.
     * <p/>
     * Override this if you want to manage your own threads. Just make sure you eventually run every {@link Runnable}
     * passed in. It's probably a good idea to set {@link KeenConfig}.NUM_THREADS_FOR_HTTP_REQUESTS to 0 if you do this.
     *
     * @param runnable The {@link Runnable} to run. In practice, this {@link Runnable} is responsible for
     *                 uploading an event to Keen IO.
     */
    public void processRunnableInNewThread(Runnable runnable) {
        EXECUTOR_SERVICE.submit(runnable);
    }

    /**
     * Shutdown the shared thread pool, with optional wait for all running threads to
     * complete.
     * <p/>
     * New events submitted using addEvent will be rejected with a
     * RejectedExecutionException on all currently instantiated KeenClients.
     * 
     * @param timeout
     *            A non-zero timeout in millis will block the current thread
     *            while waiting for the current events to be completed.
     * @throws InterruptedException if interrupted while waiting
     */
    public static void shutdown(long timeout) throws InterruptedException {
        if (EXECUTOR_SERVICE != null) {
            EXECUTOR_SERVICE.shutdown();
            if (timeout > 0) {
                EXECUTOR_SERVICE.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            }
        }
    }

}
