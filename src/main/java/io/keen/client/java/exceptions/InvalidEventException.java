package io.keen.client.java.exceptions;

/**
 * InvalidEventException
 *
 * @author dkador
 * @since 1.0.0
 */
public class InvalidEventException extends KeenException {
    public InvalidEventException(String detailMessage) {
        super(detailMessage);
    }
}
