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
        initialize(new Environment());
    }

    public synchronized void initialize(String projectId, String writeKey, String readKey)
            throws KeenInitializationException {
        initializeClient();
        if (projectId != null) {
            KeenProject defaultProject = new KeenProject(projectId, writeKey, readKey);
            KeenClient.client().setDefaultProject(defaultProject);
        }
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

    ///// DEFAULT ACCESS METHODS /////

    synchronized void initialize(Environment env) {
        initialize(env.getKeenProjectId(), env.getKeenWriteKey(), env.getKeenReadKey());
    }

    ///// PRIVATE FIELDS /////

    // TODO: Make this private once FileEventStore doesn't need it anymore.
    protected KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private Executor publishExecutor;

    ///// PRIVATE METHODS /////

    private synchronized void initializeClient() throws KeenInitializationException {
        if (jsonHandler == null) {
            jsonHandler = buildDefaultJsonHandler();
        }

        if (eventStore == null) {
            eventStore = buildDefaultEventStore();
        }

        if (publishExecutor == null) {
            publishExecutor = buildDefaultPublishExecutor();
        }

        KeenClient.initialize(jsonHandler, eventStore, publishExecutor);
    }

}
