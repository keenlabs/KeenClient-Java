package io.keen.client.java;

/**
 * Simple test client which uses a RAM event store, Jackson, and a single thread Executor.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class TestKeenClientBuilder extends KeenClient.Builder {

    private final Environment environment;

    public TestKeenClientBuilder() {
        this(new Environment());
    }

    public TestKeenClientBuilder(Environment environment) {
        this.environment = environment;
    }

    @Override
    protected KeenJsonHandler getDefaultJsonHandler() {
        return new TestJsonHandler();
    }

    @Override
    protected KeenClient buildInstance() {
        return new KeenClient(this, environment);
    }

}
