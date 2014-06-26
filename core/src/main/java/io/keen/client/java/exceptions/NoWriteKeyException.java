package io.keen.client.java.exceptions;

/**
 * NoWriteKeyException
 *
 * @author dkador
 * @since 1.0.1
 */
public class NoWriteKeyException extends KeenException {
    private static final long serialVersionUID = -8199471518510440670L;

    public NoWriteKeyException() {
        super();
    }

    public NoWriteKeyException(Throwable cause) {
        super(cause);
    }

    public NoWriteKeyException(String message) {
        super(message);
    }

    public NoWriteKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}