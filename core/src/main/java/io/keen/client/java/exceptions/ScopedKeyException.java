package io.keen.client.java.exceptions;

/**
 * ScopedKeyException
 *
 * @author dkador
 * @since 1.0.3
 */
public class ScopedKeyException extends KeenException {
    private static final long serialVersionUID = -8250886829624436391L;

    public ScopedKeyException() {
        super();
    }

    public ScopedKeyException(Throwable cause) {
        super(cause);
    }

    public ScopedKeyException(String message) {
        super(message);
    }

    public ScopedKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
