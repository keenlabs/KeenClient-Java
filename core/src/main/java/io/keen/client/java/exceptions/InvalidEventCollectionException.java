package io.keen.client.java.exceptions;

/**
 * InvalidEventCollectionException
 *
 * @author dkador
 * @since 1.0.0
 */
public class InvalidEventCollectionException extends KeenException {
    private static final long serialVersionUID = 4340205614523528793L;

    public InvalidEventCollectionException() {
        super();
    }

    public InvalidEventCollectionException(Throwable cause) {
        super(cause);
    }

    public InvalidEventCollectionException(String message) {
        super(message);
    }

    public InvalidEventCollectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
