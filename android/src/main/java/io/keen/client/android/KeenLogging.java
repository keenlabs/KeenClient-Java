package io.keen.client.android;

import android.util.Log;

import java.util.logging.*;

/**
 * KeenLogging is a wrapper around a logging module and provides, well, logging for the Keen Android SDK.
 * Logging is disabled by default so as not to clutter up your development experience.
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenLogging {

    private static final Logger LOGGER;

    static {
        LOGGER = Logger.getLogger(KeenLogging.class.getName());
        LOGGER.addHandler(new StreamHandler(System.out, new SimpleFormatter()));
        disableLogging();
    }

    static void log(String msg) {
        LOGGER.log(Level.FINER, msg);
        if (LOGGER.getLevel() == Level.FINER) {
            try {
                Log.d("KEEN_CLIENT", msg);
            } catch (RuntimeException e) {
                // ignore this, it happens when running tests
            }
        }
    }

    /**
     * Call this to enable logging.
     */
    public static void enableLogging() {
        setLogLevel(Level.FINER);
    }

    /**
     * Call this to disable logging.
     */
    public static void disableLogging() {
        setLogLevel(Level.OFF);
    }

    private static void setLogLevel(Level newLevel) {
        LOGGER.setLevel(newLevel);
        for (Handler handler : LOGGER.getHandlers()) {
            handler.setLevel(newLevel);
        }
    }
}