package io.keen.client.java.exceptions;

/**
 * KeenInitializationException
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class KeenInitializationException extends KeenException {
    private static final long serialVersionUID = 891176564876927569L;

    public KeenInitializationException() {
        super();
    }

    public KeenInitializationException(Throwable cause) {
        super(cause);
    }

    public KeenInitializationException(String message) {
        super(message);
    }

    public KeenInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
