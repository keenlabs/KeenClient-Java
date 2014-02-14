package io.keen.client.java;

import java.util.concurrent.Executor;

import io.keen.client.java.exceptions.KeenInitializationException;

/**
 * TODO: Documentation
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public abstract class KeenInitializer {

    ///// PUBLIC METHODS /////

    public synchronized void initialize() throws KeenInitializationException {
        initialize(true);
    }

    public synchronized void initialize(boolean createStaticInstance)
            throws KeenInitializationException {
        initializeInterfaces();
        if (createStaticInstance) {
            KeenClient.createStaticInstance();
        }
    }

    public synchronized void initialize(String projectId, String writeKey, String readKey)
            throws KeenInitializationException {
        initializeInterfaces();
        KeenClient.createStaticInstance(projectId, writeKey, readKey);
    }

    public synchronized KeenInitializer withJsonHandler(KeenJsonHandler jsonHandler) {
        this.jsonHandler = jsonHandler;
        return this;
    }

    public synchronized KeenInitializer withEventStore(KeenEventStore eventStore) {
        this.eventStore = eventStore;
        return this;
    }

    public synchronized KeenInitializer withPublishExecutor(Executor publishExecutor) {
        this.publishExecutor = publishExecutor;
        return this;
    }

    ///// PROTECTED ABSTRACT METHODS /////

    protected abstract Executor buildDefaultPublishExecutor() throws KeenInitializationException;
    protected abstract KeenEventStore buildDefaultEventStore() throws KeenInitializationException;
    protected abstract KeenJsonHandler buildDefaultJsonHandler() throws KeenInitializationException;

    ///// PRIVATE FIELDS /////

    // TODO: Make this private once FileEventStore doesn't need it anymore.
    protected KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private Executor publishExecutor;
    private boolean isInitializeCalled;

    ///// PRIVATE METHODS /////

    private synchronized void initializeInterfaces() throws KeenInitializationException {
        if (isInitializeCalled) {
            throw new IllegalStateException("Initialize may only be called once");
        }

        if (jsonHandler == null) {
            jsonHandler = buildDefaultJsonHandler();
        }
        KeenClient.setJsonHandler(jsonHandler);

        if (eventStore == null) {
            eventStore = buildDefaultEventStore();
        }
        KeenClient.setEventStore(eventStore);

        if (publishExecutor == null) {
            publishExecutor = buildDefaultPublishExecutor();
        }
        KeenClient.setPublishExecutor(publishExecutor);

        isInitializeCalled = true;
    }

}
