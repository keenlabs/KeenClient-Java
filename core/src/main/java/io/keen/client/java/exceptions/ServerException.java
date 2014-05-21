package io.keen.client.java.exceptions;

/**
 * ServerException
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class ServerException extends KeenException {
    private static final long serialVersionUID = 3913819084183357142L;

    public ServerException() {
        super();
    }

    public ServerException(Throwable cause) {
        super(cause);
    }

    public ServerException(String message) {
        super(message);
    }

    public ServerException(String message, Throwable cause) {
        super(message, cause);
    }
}
