package io.keen.client.java.exceptions;

/**
 * KeenException
 *
 * @author dkador
 * @since 1.0.0
 */
public abstract class KeenException extends RuntimeException {
    private static final long serialVersionUID = -2830411036279774949L;

    public KeenException() {
        super();
    }

    public KeenException(Throwable cause) {
        super(cause);
    }

    public KeenException(String message) {
        super(message);
    }

    public KeenException(String message, Throwable cause) {
        super(message, cause);
    }
}
