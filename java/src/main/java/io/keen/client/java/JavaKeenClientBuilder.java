package io.keen.client.java;

/**
 * SHIPBLOCK: Fix comments.
 * {@link io.keen.client.java.KeenClient} builder for use in a standard Java environment.
 * <p/>
 * This client uses the Jackson library for reading and writing JSON. As a result, Jackson must be
 * available in order for this library to work properly.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenClientBuilder extends KeenClient.Builder {

    @Override
    protected KeenJsonHandler getDefaultJsonHandler() {
        return new JacksonJsonHandler();
    }

}
