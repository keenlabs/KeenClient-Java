package io.keen.client.java.exceptions;

/**
 * InvalidEventException
 *
 * @author dkador
 * @since 1.0.0
 */
public class InvalidEventException extends KeenException {
    private static final long serialVersionUID = -8714276749665293346L;

    public InvalidEventException() {
        super();
    }

    public InvalidEventException(Throwable cause) {
        super(cause);
    }

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
