package io.keen.client.java;

/**
 * Simple test client which uses a RAM event store, Jackson, and a single thread Executor.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class TestKeenClient extends KeenClient {

    ///// PUBLIC STATIC METHODS //////

    /**
     * Initializes the Keen library with a test client.
     *
     * @return The singleton Keen client.
     */
    public static KeenClient initialize() {
        // If the library hasn't been initialized yet then initialize it.
        if (!KeenClient.isInitialized()) {
            KeenClient.initialize(new TestKeenClient.Builder().build());
        }
        return KeenClient.client();
    }

    ///// BUILDER IMPLEMENTATION /////

    public static class Builder extends KeenClient.Builder<TestKeenClient> {

        @Override
        protected TestKeenClient newInstance() {
            return new TestKeenClient(this);
        }

        @Override
        protected KeenJsonHandler getDefaultJsonHandler() {
            return new TestJsonHandler();
        }

    }

    ///// DEFAULT-ACCESS CONSTRUCTORS /////

    TestKeenClient(Builder builder) {
        super(builder);
    }

    TestKeenClient(Builder builder, Environment env) {
        super(builder, env);
    }

}
