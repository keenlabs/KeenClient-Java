package io.keen.client.java;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.keen.client.java.exceptions.InvalidEventCollectionException;
import io.keen.client.java.exceptions.InvalidEventException;
import io.keen.client.java.exceptions.NoWriteKeyException;
import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.HttpMethods;
import io.keen.client.java.http.OutputSource;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;
import io.keen.client.java.http.UrlConnectionHttpHandler;

/**
 * <p>
 * KeenClient provides all of the functionality required to:
 * </p>
 *
 * <ul>
 *     <li>Create events from map objects</li>
 *     <li>Automatically insert properties into events as they are created</li>
 *     <li>Post events to the Keen server, either one-at-a-time or in batches</li>
 *     <li>Store events in between batch posts, if desired</li>
 *     <li>Perform posts either synchronously or asynchronously</li>
 * </ul>
 *
 * <p>
 * To create a {@link KeenClient}, use a subclass of {@link io.keen.client.java.KeenClient.Builder}
 * which provides the default interfaces for various operations (HTTP, JSON, queueing, async).
 * </p>
 *
 * @author dkador, klitwack
 * @since 1.0.0
 */
public class KeenClient {

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

    /**
     * Initializes the static Keen client. Only the first call to this method has any effect. All
     * subsequent calls are ignored.
     *
     * @param client The {@link io.keen.client.java.KeenClient} implementation to use as the
     *               singleton client for the library.
     */
    public static void initialize(KeenClient client) {
        if (client == null) {
            throw new IllegalArgumentException("Client must not be null");
        }

        if (ClientSingleton.INSTANCE.client != null) {
            // Do nothing.
            return;
        }

        ClientSingleton.INSTANCE.client = client;
    }

    /**
     * Gets whether or not the singleton KeenClient has been initialized.
     *
     * @return {@code true} if and only if the client has been initialized.
     */
    public static boolean isInitialized() {
        return (ClientSingleton.INSTANCE.client != null);
    }

    ///// PUBLIC METHODS //////

    /**
     * Adds an event to the default project with default Keen properties and no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     */
    public void addEvent(String eventCollection, Map<String, Object> event) {
        addEvent(eventCollection, event, null);
    }

    /**
     * Adds an event to the default project with no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
     */
    public void addEvent(String eventCollection, Map<String, Object> event,
                         Map<String, Object> keenProperties) {
        addEvent(null, eventCollection, event, keenProperties, null);
    }

    /**
     * Synchronously adds an event to the specified collection. This method will immediately
     * publish the event to the Keen server in the current thread.
     *
     * @param project         The project in which to publish the event. If a default project has been set
     *                        on the client, this parameter may be null, in which case the default project
     *                        will be used.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
     * @param callback        An optional callback to receive notification of success or failure.
     */
    public void addEvent(KeenProject project, String eventCollection, Map<String, Object> event,
                         Map<String, Object> keenProperties, KeenCallback callback) {

        if (!isActive) {
            handleLibraryInactive(callback);
            return;
        }

        if (project == null && defaultProject == null) {
            handleFailure(null,
                          project,
                          eventCollection,
                          event,
                          keenProperties,
                          new IllegalStateException("No project specified, but no default project found"));
            return;
        }
        KeenProject useProject = (project == null ? defaultProject : project);

        try {
            // Build the event.
            Map<String, Object> newEvent =
                    validateAndBuildEvent(useProject, eventCollection, event, keenProperties);

            // Publish the event.
            publish(useProject, eventCollection, newEvent);
            handleSuccess(callback, project, eventCollection, event, keenProperties);
        } catch (Exception e) {
            handleFailure(callback, project, eventCollection, event, keenProperties, e);
        }
    }

    /**
     * Adds an event to the default project with default Keen properties and no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     */
    public void addEventAsync(String eventCollection, Map<String, Object> event) {
        addEventAsync(eventCollection, event, null);
    }

    /**
     * Adds an event to the default project with no callbacks.
     *
     * @see #addEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
     */
    public void addEventAsync(String eventCollection, Map<String, Object> event,
                              final Map<String, Object> keenProperties) {
        addEventAsync(null, eventCollection, event, keenProperties, null);
    }

    /**
     * Asynchronously adds an event to the specified collection. This method will request that
     * the Keen client's {@link java.util.concurrent.Executor} executes the publish operation.
     *
     * @param project         The project in which to publish the event. If a default project has been set
     *                        on the client this parameter may be null, in which case the default project
     *                        will be used.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
     * @param callback        An optional callback to receive notification of success or failure.
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
            handleFailure(null,
                          project,
                          eventCollection,
                          event,
                          keenProperties,
                          new IllegalStateException(
                                  "No project specified, but no default project found"));
            return;
        }
        final KeenProject useProject = (project == null ? defaultProject : project);

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
            handleFailure(callback, project, eventCollection, event, keenProperties, e);
        }
    }

    /**
     * Queues an event in the default project with default Keen properties and no callbacks.
     *
     * @see #queueEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     */
    public void queueEvent(String eventCollection, Map<String, Object> event) {
        queueEvent(eventCollection, event, null);
    }

    /**
     * Queues an event in the default project with no callbacks.
     *
     * @see #queueEvent(KeenProject, String, java.util.Map, java.util.Map, KeenCallback)
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
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
     * @param project         The project in which to publish the event. If a default project has been set
     *                        on the client this parameter may be null, in which case the default project
     *                        will be used.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
     * @param callback        An optional callback to receive notification of success or failure.
     */
    public void queueEvent(KeenProject project, String eventCollection, Map<String, Object> event,
                           Map<String, Object> keenProperties, final KeenCallback callback) {

        if (!isActive) {
            handleLibraryInactive(callback);
            return;
        }

        if (project == null && defaultProject == null) {
            handleFailure(null,
                          project,
                          eventCollection,
                          event,
                          keenProperties,
                          new IllegalStateException(
                                  "No project specified, but no default project found"));
            return;
        }
        KeenProject useProject = (project == null ? defaultProject : project);

        try {
            // Build the event
            Map<String, Object> newEvent =
                    validateAndBuildEvent(useProject, eventCollection, event, keenProperties);

            // Serialize the event into JSON.
            StringWriter writer = new StringWriter();
            jsonHandler.writeJson(writer, newEvent);
            String jsonEvent = writer.toString();
            KeenUtils.closeQuietly(writer);

            try {
                // Save the JSON event out to the event store.
                Object handle = eventStore.store(useProject.getProjectId(), eventCollection, jsonEvent);

                if (eventStore instanceof KeenAttemptCountingEventStore) {
                    synchronized (attemptsLock) {
                        Map<String, Integer> attempts = getAttemptsMap(useProject.getProjectId(), eventCollection);
                        attempts.put("" + handle.hashCode(), maxAttempts);
                        setAttemptsMap(useProject.getProjectId(), eventCollection, attempts);
                    }
                }
            } catch(IOException ex) {
                KeenLogging.log("Failed to set the event POST attempt count. The event was still " +
                        "queued and will we POSTed.");
            }
            handleSuccess(callback, project, eventCollection, event, keenProperties);
        } catch (Exception e) {
            handleFailure(callback, project, eventCollection, event, keenProperties, e);
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
     * @param project  The project for which to send queued events. If a default project has been set
     *                 on the client this parameter may be null, in which case the default project
     *                 will be used.
     */
    public void sendQueuedEvents(KeenProject project) {
        sendQueuedEvents(project, null);
    }

    /**
     * Synchronously sends all queued events for the given project. This method will immediately
     * publish the events to the Keen server in the current thread.
     *
     * @param project  The project for which to send queued events. If a default project has been set
     *                 on the client this parameter may be null, in which case the default project
     *                 will be used.
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

        if (!isNetworkConnected()) {
            KeenLogging.log("Not sending events because there is no network connection. " +
                            "Events will be retried next time `sendQueuedEvents` is called.");
            handleFailure(callback, new Exception("Network not connected."));
            return;
        }

        KeenProject useProject = (project == null ? defaultProject : project);

        try {
            String projectId = useProject.getProjectId();
            Map<String, List<Object>> eventHandles = eventStore.getHandles(projectId);
            Map<String, List<Map<String, Object>>> events = buildEventMap(projectId, eventHandles);
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
     * @param project  The project for which to send queued events. If a default project has been set
     *                 on the client this parameter may be null, in which case the default project
     *                 will be used.
     */
    public void sendQueuedEventsAsync(final KeenProject project) {
        sendQueuedEventsAsync(project, null);
    }

    /**
     * Asynchronously sends all queued events for the given project. This method will request that
     * the Keen client's {@link java.util.concurrent.Executor} executes the publish operation.
     *
     * @param project  The project for which to send queued events. If a default project has been set
     *                 on the client this parameter may be null, in which case the default project
     *                 will be used.
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
        final KeenProject useProject = (project == null ? defaultProject : project);

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
     * Gets the JSON handler for this client.
     *
     * @return The {@link io.keen.client.java.KeenJsonHandler}.
     */
    public KeenJsonHandler getJsonHandler() {
        return jsonHandler;
    }

    /**
     * Gets the event store for this client.
     *
     * @return The {@link io.keen.client.java.KeenEventStore}.
     */
    public KeenEventStore getEventStore() {
        return eventStore;
    }

    /**
     * Gets the executor for asynchronous publishing for this client.
     *
     * @return The {@link java.util.concurrent.Executor}.
     */
    public Executor getPublishExecutor() {
        return publishExecutor;
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
     * <p>
     * Use this if you want to disable SSL.
     * </p>
     * @param baseUrl The new base URL (i.e. 'http://api.keen.io'), or null to reset the base URL to
     *                the default ('https://api.keen.io').
     */
    public void setBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            this.baseUrl = KeenConstants.SERVER_ADDRESS;
        } else {
            this.baseUrl = baseUrl;
        }
    }

    /**
     * Sets the maximum number of HTTPS POST retry attempts for all events added in the future.
     *
     * @param maxAttempts the maximum number attempts
     */
    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    /**
     * Sets the maximum number of HTTPS POST retry attempts for all events added in the future.
     *
     * @return the maximum number attempts
     */
    public int getMaxAttempts() {
        return maxAttempts;
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
     * <p>
     * Global properties are properties which are sent with EVERY event. For example, you may wish to always
     * capture device information like OS version, handset type, orientation, etc.
     * </p> <p>
     * The evaluator takes as a parameter a single String, which is the name of the event collection the
     * event's being added to. You're responsible for returning a Map which represents the global properties
     * for this particular event collection.
     * </p><p>
     * Note that because we use a class defined by you, you can create DYNAMIC global properties. For example,
     * if you want to capture device orientation, then your evaluator can ask the device for its current orientation
     * and then construct the Map. If your global properties aren't dynamic, then just return the same Map
     * every time.
     * </p>
     * Example usage:
     * <pre>
     *     {@code KeenClient client = KeenClient.client();
     *     GlobalPropertiesEvaluator evaluator = new GlobalPropertiesEvaluator() {
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
     * <p>
     * Keen Global Properties are properties which are sent with EVERY event. For example, you may wish to always
     * capture static information like user ID, app version, etc.
     * </p><p>
     * Every time an event is added to an event collection, the SDK will check to see if this property is defined.
     * If it is, the SDK will copy all the properties from the global properties into the newly added event.
     * </p><p>
     * Note that because this is just a Map, it's much more difficult to create DYNAMIC global properties.
     * It also doesn't support per-collection properties. If either of these use cases are important to you, please use
     * the {@link GlobalPropertiesEvaluator}.
     * </p><p>
     * Also note that the Keen properties defined in {@link #getGlobalPropertiesEvaluator()} take precedence over
     * the properties defined in getGlobalProperties, and that the Keen Properties defined in each
     * individual event take precedence over either of the Global Properties.
     * </p><p>
     * Example usage:
     * </p>
     * <pre>
     *     {@code
     * KeenClient client = KeenClient.client();
     * Map<String, Object> map = new HashMap<String, Object>();
     * map.put("some standard key", "some standard value");
     * client.setGlobalProperties(map);
     * }
     * </pre>
     *
     * @param globalProperties The new map you wish to use as the Keen Global Properties.
     */
    public void setGlobalProperties(Map<String, Object> globalProperties) {
        this.globalProperties = globalProperties;
    }

    /**
     * Gets whether or not the Keen client is running in debug mode.
     *
     * @return {@code true} if debug mode is enabled, otherwise {@code false}.
     */
    public boolean isDebugMode() {
        return isDebugMode;
    }

    /**
     * Sets whether or not the Keen client should run in debug mode. When debug mode is enabled,
     * all exceptions will be thrown immediately; otherwise they will be logged and reported to
     * any callbacks, but never thrown.
     *
     * @param isDebugMode {@code true} to enable debug mode, or {@code false} to disable it.
     */
    public void setDebugMode(boolean isDebugMode) {
        this.isDebugMode = isDebugMode;
    }

    /**
     * Gets whether or not the client is in active mode.
     *
     * @return {@code true} if the client is active,; {@code false} if it is inactive.
     */
    public boolean isActive() {
        return isActive;
    }

    /**
      * Sets an HTTP proxy server configuration for this client.
      *
      * @param proxyHost The proxy hostname or IP address.
      * @param proxyPort The proxy port number.
      */
     public void setProxy(String proxyHost, int proxyPort) {
         this.proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
     }

    /**
      * Sets an HTTP proxy server configuration for this client.
      *
      * @param proxy The Proxy object to set.
      */
     public void setProxy(Proxy proxy) {
         this.proxy = proxy;
     }

    /**
     * Gets the client Proxy.
     *
     * @return the proxy.
     */
     public Proxy getProxy() {
         return proxy;
     }

    ///// PROTECTED ABSTRACT BUILDER IMPLEMENTATION /////

    /**
     * Builder class for instantiating Keen clients. Subclasses should override this and
     * implement the getDefault* methods to provide new default behavior.
     * <p>
     * This builder doesn't include any default implementation for handling JSON serialization and
     * de-serialization. Subclasses must provide one.
     * </p><p>
     * This builder defaults to using HttpURLConnection to handle HTTP requests.
     * </p><p>
     * To cache events in between batch uploads, this builder defaults to a RAM-based event store.
     * <p>
     * This builder defaults to a fixed thread pool (constructed with
     * {@link java.util.concurrent.Executors#newFixedThreadPool(int)}) to run asynchronous requests.
     */
    public static abstract class Builder {

        private HttpHandler httpHandler;
        private KeenJsonHandler jsonHandler;
        private KeenEventStore eventStore;
        private Executor publishExecutor;
        private KeenNetworkStatusHandler networkStatusHandler;

        /**
         * Gets the default {@link HttpHandler} to use if none is explicitly set for this builder.
         *
         * This implementation returns a handler that will use {@link java.net.HttpURLConnection}
         * to make HTTP requests.
         *
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
         *
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
         * Gets the default {@link KeenEventStore} to use if none is explicitly set for this builder.
         *
         * This implementation returns a RAM-based store.
         *
         * Subclasses should override this to provide an alternative default {@link KeenEventStore}.
         *
         * @return The default {@link KeenEventStore}.
         * @throws Exception If there is an error creating the {@link KeenEventStore}.
         */
        protected KeenEventStore getDefaultEventStore() throws Exception {
            return new RamEventStore();
        }

        /**
         * Gets the {@link KeenEventStore} that this builder is currently configured to use for
         * storing events between batch publish operations. If null, a default will be used instead.
         *
         * @return The {@link KeenEventStore} to use.
         */
        public KeenEventStore getEventStore() {
            return eventStore;
        }

        /**
         * Sets the {@link KeenEventStore} to use for storing events in between batch publish
         * operations.
         *
         * @param eventStore The {@link KeenEventStore} to use.
         */
        public void setEventStore(KeenEventStore eventStore) {
            this.eventStore = eventStore;
        }

        /**
         * Sets the {@link KeenEventStore} to use for storing events in between batch publish
         * operations.
         *
         * @param eventStore The {@link KeenEventStore} to use.
         * @return This instance (for method chaining).
         */
        public Builder withEventStore(KeenEventStore eventStore) {
            setEventStore(eventStore);
            return this;
        }

        /**
         * Gets the default {@link Executor} to use if none is explicitly set for this builder.
         *
         * This implementation returns a simple fixed thread pool with the number of threads equal
         * to the number of available processors.
         *
         * Subclasses should override this to provide an alternative default {@link Executor}.
         *
         * @return The default {@link Executor}.
         * @throws Exception If there is an error creating the {@link Executor}.
         */
        protected Executor getDefaultPublishExecutor() throws Exception {
            int procCount = Runtime.getRuntime().availableProcessors();
            return Executors.newFixedThreadPool(procCount);
        }

        /**
         * Gets the {@link Executor} that this builder is currently configured to use for
         * asynchronous publishing operations. If null, a default will be used instead.
         *
         * @return The {@link Executor} to use.
         */
        public Executor getPublishExecutor() {
            return publishExecutor;
        }

        /**
         * Sets the {@link Executor} to use for asynchronous publishing operations.
         *
         * @param publishExecutor The {@link Executor} to use.
         */
        public void setPublishExecutor(Executor publishExecutor) {
            this.publishExecutor = publishExecutor;
        }

        /**
         * Sets the {@link Executor} to use for asynchronous publishing operations.
         *
         * @param publishExecutor The {@link Executor} to use.
         * @return This instance (for method chaining).
         */
        public Builder withPublishExecutor(Executor publishExecutor) {
            setPublishExecutor(publishExecutor);
            return this;
        }

        /**
         * Gets the default {@link KeenNetworkStatusHandler} to use if none is explicitly set for this builder.
         *
         * This implementation always returns true.
         *
         * Subclasses should override this to provide an alternative default {@link KeenNetworkStatusHandler}.
         *
         * @return The default {@link KeenNetworkStatusHandler}.
         */
        protected KeenNetworkStatusHandler getDefaultNetworkStatusHandler() {
            return new AlwaysConnectedNetworkStatusHandler();
        }

        /**
         * Gets the {@link KeenNetworkStatusHandler} that this builder is currently configured to use.
         * If null, a default will be used instead.
         *
         * @return The {@link KeenNetworkStatusHandler} to use.
         */
        public KeenNetworkStatusHandler getNetworkStatusHandler () {
            return networkStatusHandler;
        }

        /**
         * Sets the {@link KeenNetworkStatusHandler} to use.
         *
         * @param networkStatusHandler The {@link KeenNetworkStatusHandler} to use.
         */
        public void setNetworkStatusHandler(KeenNetworkStatusHandler networkStatusHandler) {
            this.networkStatusHandler = networkStatusHandler;
        }

        /**
         * Sets the {@link KeenNetworkStatusHandler} to use.
         *
         * @param networkStatusHandler The {@link KeenNetworkStatusHandler} to use.
         * @return This instance (for method chaining).
         */
        public Builder withNetworkStatusHandler(KeenNetworkStatusHandler networkStatusHandler) {
            setNetworkStatusHandler(networkStatusHandler);
            return this;
        }

        /**
         * Builds a new Keen client using the interfaces which have been specified explicitly on
         * this builder instance via the set* or with* methods, or the default interfaces if none
         * have been specified.
         *
         * @return A newly constructed Keen client.
         */
        public KeenClient build() {
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

            try {
                if (eventStore == null) {
                    eventStore = getDefaultEventStore();
                }
            } catch (Exception e) {
                KeenLogging.log("Exception building event store: " + e.getMessage());
            }

            try {
                if (publishExecutor == null) {
                    publishExecutor = getDefaultPublishExecutor();
                }
            } catch (Exception e) {
                KeenLogging.log("Exception building publish executor: " + e.getMessage());
            }

            try {
                if (networkStatusHandler == null) {
                    networkStatusHandler = getDefaultNetworkStatusHandler();
                }
            } catch (Exception e) {
                KeenLogging.log("Exception building network status handler: " + e.getMessage());
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
        protected KeenClient buildInstance() {
            return new KeenClient(this);
        }

    }

    ///// PROTECTED CONSTRUCTORS /////

    /**
     * Constructs a Keen client using system environment variables.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     */
    protected KeenClient(Builder builder) {
        this(builder, new Environment());
    }

    /**
     * Constructs a Keen client using the provided environment.
     *
     * NOTE: This constructor is only intended for use by test code, and should not be used
     * directly. Subclasses should call the default {@link #KeenClient(Builder)} constructor.
     *
     * @param builder The builder from which to retrieve this client's interfaces and settings.
     * @param env The environment to use to attempt to build the default project.
     */
    KeenClient(Builder builder, Environment env) {
        // Initialize final properties using the builder.
        this.httpHandler = builder.httpHandler;
        this.jsonHandler = builder.jsonHandler;
        this.eventStore = builder.eventStore;
        this.publishExecutor = builder.publishExecutor;
        this.networkStatusHandler = builder.networkStatusHandler;

        // If any of the interfaces are null, mark this client as inactive.
        if (httpHandler == null || jsonHandler == null ||
            eventStore == null || publishExecutor == null) {
            setActive(false);
        }

        // Initialize other properties.
        this.baseUrl = KeenConstants.SERVER_ADDRESS;
        this.globalPropertiesEvaluator = null;
        this.globalProperties = null;

        // If a default project has been specified in environment variables, use it.
        if (env.getKeenProjectId() != null) {
            defaultProject = new KeenProject(env);
        }
    }

    ///// PROTECTED METHODS /////

    /**
     * Sets whether or not the client is in active mode. When the client is inactive, all requests
     * will be ignored.
     *
     * @param isActive {@code true} to make the client active, or {@code false} to make it
     *                 inactive.
     */
    protected void setActive(boolean isActive) {
        this.isActive = isActive;
        KeenLogging.log("Keen Client set to " + (isActive? "active" : "inactive"));
    }

    /**
     * Validates an event and inserts global properties, producing a new event object which is
     * ready to be published to the Keen service.
     *
     * @param project         The project in which the event will be published.
     * @param eventCollection The name of the collection in which the event will be published.
     * @param event           A Map that consists of key/value pairs.
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     * @return A new event Map containing Keen properties and global properties.
     */
    protected Map<String, Object> validateAndBuildEvent(KeenProject project,
                                                        String eventCollection, Map<String, Object> event, Map<String, Object> keenProperties) {

        if (project.getWriteKey() == null) {
            throw new NoWriteKeyException("You can't send events to Keen IO if you haven't set a write key.");
        }

        validateEventCollection(eventCollection);
        validateEvent(event);

        KeenLogging.log(String.format(Locale.US, "Adding event to collection: %s", eventCollection));

        // Create maps to aggregate keen & non-keen properties
        Map<String, Object> newEvent = new HashMap<String, Object>();
        Map<String, Object> mergedKeenProperties = new HashMap<String, Object>();

        // separate keen & non-keen properties from static globals and merge them into separate maps
        if (null != globalProperties) {
            mergeGlobalProperties(getGlobalProperties(), mergedKeenProperties, newEvent);
        }

        // separate keen & non-keen properties from dynamic globals and merge them into separate maps
        GlobalPropertiesEvaluator globalPropertiesEvaluator = getGlobalPropertiesEvaluator();
        if (globalPropertiesEvaluator != null) {
            mergeGlobalProperties(globalPropertiesEvaluator.getGlobalProperties(eventCollection), mergedKeenProperties,
                    newEvent);
        }

        // merge any per-event keen properties
        if (keenProperties != null) {
            mergedKeenProperties.putAll(keenProperties);
        }

        // if no keen.timestamp was provided by globals or event, add one now
        if (!mergedKeenProperties.containsKey("timestamp")) {
            Calendar currentTime = Calendar.getInstance();
            String timestamp = ISO_8601_FORMAT.format(currentTime.getTime());
            mergedKeenProperties.put("timestamp", timestamp);
        }

        // add merged keen properties to event
        newEvent.put("keen", mergedKeenProperties);

        // merge any per-event non-keen properties
        newEvent.putAll(event);
        return newEvent;
    }

    /**
     * Removes the "keen" key from the globalProperties map and, if a map was removed, then all of its pairs are added to the keenProperties map.
     * Anything left in the globalProperties map is then added to the newEvent map.
     *
     * @param globalProperties
     * @param keenProperties
     * @param newEvent
     */
    private void mergeGlobalProperties(Map<String, Object> globalProperties, Map<String, Object> keenProperties,
                                       Map<String, Object> newEvent) {
        if (globalProperties != null) {
            // Clone globals so we don't modify the original
            globalProperties = new HashMap<String, Object>(globalProperties);
            Object keen = globalProperties.remove("keen");
            if (keen instanceof Map) {
                keenProperties.putAll((Map)keen);
            }
            newEvent.putAll(globalProperties);
        }
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

    private static final DateFormat ISO_8601_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.US);

    ///// PRIVATE FIELDS /////

    private final HttpHandler httpHandler;
    private final KeenJsonHandler jsonHandler;
    private final KeenEventStore eventStore;
    private final Executor publishExecutor;
    private final KeenNetworkStatusHandler networkStatusHandler;
    private final Object attemptsLock = new Object();

    private boolean isActive = true;
    private boolean isDebugMode;
    private int maxAttempts = KeenConstants.DEFAULT_MAX_ATTEMPTS;
    private KeenProject defaultProject;
    private String baseUrl;
    private GlobalPropertiesEvaluator globalPropertiesEvaluator;
    private Map<String, Object> globalProperties;
    private Proxy proxy;

    ///// PRIVATE METHODS /////

    /**
     * Validates the name of an event collection.
     *
     * @param eventCollection An event collection name to be validated.
     * @throws io.keen.client.java.exceptions.InvalidEventCollectionException If the event collection name is invalid. See Keen documentation for details.
     */
    private void validateEventCollection(String eventCollection) {
        if (eventCollection == null || eventCollection.length() == 0) {
            throw new InvalidEventCollectionException("You must specify a non-null, " +
                    "non-empty event collection: " + eventCollection);
        }
        if (eventCollection.length() > 256) {
            throw new InvalidEventCollectionException("An event collection name cannot be longer than 256 characters.");
        }
    }

    /**
     * @see #validateEvent(java.util.Map, int)
     * @param event The event to validate.
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
        } else if (depth > KeenConstants.MAX_EVENT_DEPTH) {
            throw new InvalidEventException("An event's depth (i.e. layers of nesting) cannot exceed " +
                    KeenConstants.MAX_EVENT_DEPTH);
        }

        for (Map.Entry<String, Object> entry : event.entrySet()) {
            String key = entry.getKey();
            if (key.contains(".")) {
                throw new InvalidEventException("An event cannot contain a property with the period (.) character in " +
                        "it.");
            }
            if (key.length() > 256) {
                throw new InvalidEventException("An event cannot contain a property name longer than 256 characters.");
            }

            validateEventValue(entry.getValue(), depth);
        }
    }

    /**
     * Validates a value within an event structure. This method will handle validating each element
     * in a list, as well as recursively validating nested maps.
     *
     * @param value The value to validate.
     * @param depth The current depth of validation.
     */
    @SuppressWarnings("unchecked") // cast to generic Map will always be okay in this case
    private void validateEventValue(Object value, int depth) {
        if (value instanceof String) {
            String strValue = (String) value;
            if (strValue.length() >= 10000) {
                throw new InvalidEventException("An event cannot contain a string property value longer than 10," +
                        "000 characters.");
            }
        } else if (value instanceof Map) {
            validateEvent((Map<String, Object>) value, depth + 1);
        } else if (value instanceof Iterable) {
            for (Object listElement : (Iterable) value) {
                validateEventValue(listElement, depth);
            }
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
    private Map<String, List<Map<String, Object>>> buildEventMap(String projectId,
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

            Map<String, Integer> attempts;
            if (eventStore instanceof KeenAttemptCountingEventStore) {
                synchronized (attemptsLock) {
                    try {
                        attempts = getAttemptsMap(projectId, eventCollection);
                    } catch (IOException ex) {
                        // setting this to a fresh map will effectively declare this the "last attempt" for
                        // these events.
                        attempts = new HashMap<String, Integer>();
                        KeenLogging.log("Failed to read attempt counts map. Events will still be POSTed. " +
                                "Exception: " + ex);
                    }

                    for (Object handle : handles) {
                        Map<String, Object> event = getEvent(handle);

                        String attemptsKey = "" + handle.hashCode();
                        Integer remainingAttempts = attempts.get(attemptsKey);
                        if (remainingAttempts == null) {
                            // treat null as "this is the last attempt"
                            remainingAttempts = 1;
                        }

                        // decrement the remaining attempts count and put the new value on the map
                        remainingAttempts--;
                        attempts.put(attemptsKey, remainingAttempts);

                        if (remainingAttempts >= 0) {
                            // if we had some remaining attempts, then try again
                            events.add(event);
                        } else {
                            // otherwise remove it from the store
                            eventStore.remove(handle);

                            // iff eventStore.remove succeeds we can do some housekeeping and remove the
                            // key from the attempts hash.
                            attempts.remove(attemptsKey);
                        }
                    }

                    try {
                        setAttemptsMap(projectId, eventCollection, attempts);
                    } catch(IOException ex) {
                        KeenLogging.log("Failed to update event POST attempts counts while sending queued " +
                                "events. Events will still be POSTed. Exception: " + ex);
                    }
                }
            } else {
                for (Object handle : handles) {
                    events.add(getEvent(handle));
                }
            }


            result.put(eventCollection, events);
        }
        return result;
    }

    /**
     * Publishes a single event to the Keen service.
     *
     * @param project         The project in which to publish the event.
     * @param eventCollection The name of the collection in which to publish the event.
     * @param event           The event to publish.
     * @return The response from the server.
     * @throws IOException If there was an error communicating with the server.
     */
    private String publish(KeenProject project, String eventCollection, Map<String, Object> event) throws IOException {
        URL createURL = createURL(project, eventCollection);
        return publishObject(project, createURL, event);
    }

    /**
     * Create URL address which is used to publish it to Keen service.
     *
     * @param project           The project in which to publish the event.
     * @param eventCollection   The name of the collection in which to publish the event.
     * @return URL address
     * @throws URISyntaxException If string could not be parsed as a URI reference.
     * @throws MalformedURLException If URL you created is malformed or there is no legal specified in it.
     */
    private URL createURL(KeenProject project, String eventCollection) {
        URL url = null;
        try {
            eventCollection = new URI(null, null, eventCollection, null).getRawPath();
            String urlString = String.format(Locale.US, "%s/%s/projects/%s/events/%s", getBaseUrl(),
                    KeenConstants.API_VERSION, project.getProjectId(), eventCollection);
            url = new URL(urlString);
        } catch (URISyntaxException e) {
            KeenLogging.log("Event collection name has invalid character to encode", e);
        } catch (MalformedURLException e) {
            KeenLogging.log("Url you create is malformed or there is not legal protocol in string you specified ", e);
        }

        return url;
    }

    /**
     * Publishes a batch of events to the Keen service.
     *
     * @param project The project in which to publish the event.
     * @param events  A map from collection name to a list of event maps.
     * @return The response from the server.
     * @throws IOException If there was an error communicating with the server.
     */
    private String publishAll(KeenProject project,
                              Map<String, List<Map<String, Object>>> events) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format(Locale.US, "%s/%s/projects/%s/events", getBaseUrl(),
                KeenConstants.API_VERSION, project.getProjectId());
        URL url = new URL(urlString);
        return publishObject(project, url, events);
    }

    /**
     * Posts a request to the server in the specified project, using the given URL and request data.
     * The request data will be serialized into JSON using the client's
     * {@link io.keen.client.java.KeenJsonHandler}.
     *
     * @param project     The project in which the event(s) will be published; this is used to
     *                    determine the write key to use for authentication.
     * @param url         The URL to which the POST should be sent.
     * @param requestData The request data, which will be serialized into JSON and sent in the
     *                    request body.
     * @return The response from the server.
     * @throws IOException If there was an error communicating with the server.
     */
    private synchronized String publishObject(KeenProject project, URL url,
                                              final Map<String, ?> requestData) throws IOException {
        if (requestData == null || requestData.size() == 0) {
            KeenLogging.log("No API calls were made because there were no events to upload");
            return null;
        }

        // Build an output source which simply writes the serialized JSON to the output.
        OutputSource source = new OutputSource() {
            @Override
            public void writeTo(OutputStream out) throws IOException {
                OutputStreamWriter writer = new OutputStreamWriter(out, ENCODING);
                jsonHandler.writeJson(writer, requestData);
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
                KeenLogging.log("Couldn't log event written to file: ", e);
            }
        }

        // Send the request.
        String writeKey = project.getWriteKey();
        Request request = new Request(url, HttpMethods.POST, writeKey, source, proxy);
        Response response = httpHandler.execute(request);

        // If logging is enabled, log the response.
        if (KeenLogging.isLoggingEnabled()) {
            KeenLogging.log(String.format(Locale.US,
                    "Received response: '%s' (%d)", response.body,
                    response.statusCode));
        }

        // If the request succeeded, return the response body. Otherwise throw an exception.
        if (response.isSuccess()) {
            return response.body;
        } else {
            throw new ServerException(response.body);
        }
    }

    /**
     * Returns the status of the network connection
     *
     * @return true if there is network connection
     */
    private boolean isNetworkConnected() {
        return networkStatusHandler.isNetworkConnected();
    }

    ///// PRIVATE CONSTANTS /////
    private static final String ENCODING = "UTF-8";

    /**
     * Handles a response from the Keen service to a batch post events operation. In particular,
     * this method will iterate through the responses and remove any successfully processed events
     * (or events which failed for known fatal reasons) from the event store so they won't be sent
     * in subsequent posts.
     *
     * @param handles  A map from collection names to lists of handles in the event store. This is
     *                 referenced against the response from the server to determine which events to
     *                 remove from the store.
     * @param response The response from the server.
     * @throws IOException If there is an error removing events from the store.
     */
    @SuppressWarnings("unchecked")
    private void handleAddEventsResponse(Map<String, List<Object>> handles, String response) throws IOException {
        // Parse the response into a map.
        StringReader reader = new StringReader(response);
        Map<String, Object> responseMap;
        responseMap = jsonHandler.readJson(reader);

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
                        KeenLogging.log(String.format(Locale.US,
                                "The event could not be inserted for some reason. " +
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
                // Do nothing. Issue #98
            }
        }
    }

    /**
     * Reports success to a callback. If the callback is null, this is a no-op. Any exceptions
     * thrown by the callback are silently ignored.
     *
     * @param callback A callback; may be null.
     * @param project         The project in which the event was published. If a default project has been set
     *                        on the client, this parameter may be null, in which case the default project
     *                        was used.
     * @param eventCollection The name of the collection in which the event was published
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
     */
    private void handleSuccess(KeenCallback callback,
                               KeenProject project,
                               String eventCollection,
                               Map<String, Object> event,
                               Map<String, Object> keenProperties) {
        handleSuccess(callback);
        if (callback != null) {
            try {
                if (callback instanceof KeenDetailedCallback){
                    ((KeenDetailedCallback)callback).onSuccess(project,
                                                               eventCollection,
                                                               event,
                                                               keenProperties);
                }
            } catch (Exception userException) {
                // Do nothing. Issue #98
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
     * @param e        The exception which caused the failure.
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
                    // Do nothing. Issue #98
                }
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
     * @param project         The project in which the event was published. If a default project has been set
     *                        on the client, this parameter may be null, in which case the default project
     *                        was used.
     * @param eventCollection The name of the collection in which the event was published
     * @param event           A Map that consists of key/value pairs. Keen naming conventions apply (see
     *                        docs). Nested Maps and lists are acceptable (and encouraged!).
     * @param keenProperties  A Map that consists of key/value pairs to override default properties.
     *                        ex: "timestamp" -&gt; Calendar.getInstance()
     * @param e        The exception which caused the failure.
     */
    private void handleFailure(KeenCallback callback,
                               KeenProject project,
                               String eventCollection,
                               Map<String, Object> event,
                               Map<String, Object> keenProperties,
                               Exception e) {
        if (isDebugMode) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } else {
            handleFailure(callback, e);

            KeenLogging.log("Encountered error: " + e.getMessage());
            if (callback != null) {
                try {
                    if (callback instanceof KeenDetailedCallback){
                        ((KeenDetailedCallback)callback).onFailure(project,
                                                                   eventCollection,
                                                                   event,
                                                                   keenProperties,
                                                                   e);
                    }
                } catch (Exception userException) {
                    // Do nothing. Issue #98
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

    /**
     * Get an event object from the eventStore.
     *
     * @param handle the handle object
     * @return the event object for handle
     * @throws IOException
     */
    private Map<String, Object> getEvent(Object handle) throws IOException {
        // Get the event from the store.
        String jsonEvent = eventStore.get(handle);

        // De-serialize the event from its JSON.
        StringReader reader = new StringReader(jsonEvent);
        Map<String, Object> event = jsonHandler.readJson(reader);
        KeenUtils.closeQuietly(reader);
        return event;
    }

    /**
     * Gets the map of attempt counts from the eventStore
     *
     * @param projectId the project id
     * @param eventCollection the collection name
     * @return a Map of event hashCodes to attempt counts
     * @throws IOException
     */
    private Map<String, Integer> getAttemptsMap(String projectId, String eventCollection) throws IOException {
        Map<String, Integer> attempts = new HashMap<String, Integer>();
        if (eventStore instanceof KeenAttemptCountingEventStore) {
            KeenAttemptCountingEventStore res = (KeenAttemptCountingEventStore)eventStore;
            String attemptsJSON = res.getAttempts(projectId, eventCollection);
            if (attemptsJSON != null) {
                StringReader reader = new StringReader(attemptsJSON);
                Map<String, Object> attemptTmp = jsonHandler.readJson(reader);
                for (Entry<String, Object> entry : attemptTmp.entrySet()) {
                    if (entry.getValue() instanceof Number) {
                        attempts.put(entry.getKey(), ((Number)entry.getValue()).intValue());
                    }
                }
            }
        }

        return attempts;
    }

    /**
     * Set the attempts Map in the eventStore
     *
     * @param projectId the project id
     * @param eventCollection the collection name
     * @param attempts the current attempts Map
     * @throws IOException
     */
    private void setAttemptsMap(String projectId, String eventCollection, Map<String, Integer> attempts) throws IOException {
        if (eventStore instanceof KeenAttemptCountingEventStore) {
            KeenAttemptCountingEventStore res = (KeenAttemptCountingEventStore)eventStore;
            StringWriter writer = null;
            try {
                writer = new StringWriter();
                jsonHandler.writeJson(writer, attempts);
                String attemptsJSON = writer.toString();
                res.setAttempts(projectId, eventCollection, attemptsJSON);
            } finally {
                KeenUtils.closeQuietly(writer);
            }
        }
    }
}
