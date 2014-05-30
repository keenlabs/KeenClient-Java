package io.keen.client.java;

/**
 * {@link io.keen.client.java.KeenClient.Builder} with defaults suited for use in a standard Java
 * environment.
 * <p/>
 * This client uses the Jackson library for reading and writing JSON. As a result, Jackson must be
 * available in order for this library to work properly. For applications which would prefer to
 * use a different JSON library, configure the builder to use an appropriate {@link KeenJsonHandler}
 * via the {@link #withJsonHandler(KeenJsonHandler)} method.
 * <p/>
 * Other defaults are those provided by the parent {@link KeenClient.Builder} implementation.
 * <p/>
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
