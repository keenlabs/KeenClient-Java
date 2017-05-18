package io.keen.client.java;

import java.util.Locale;

/**
 * Helpers and constants for Saved/Cached Query refresh rate.
 *
 * @author masojus
 */
public final class RefreshRate {
    private static final int SECONDS_PER_HOUR = 3600;

    private RefreshRate() {}

    // The refresh rate range empirically is [14400, 86400] seconds, inclusive at both boundaries.
    // A refresh rate of 0 means caching is turned off.

    public static final int NO_CACHING = 0;
    public static final int MIN = 14400; // Minimum is 4 hrs
    public static final int MAX = 86400; // Maximum is 24 hrs

    public static int fromHours(int hours) {
        int refreshRate = hours * RefreshRate.SECONDS_PER_HOUR;

        RefreshRate.validateRefreshRate(refreshRate);

        return refreshRate;
    }

    static void validateRefreshRate(int refreshRate) {
        if (RefreshRate.NO_CACHING != refreshRate &&
            (RefreshRate.MIN > refreshRate || RefreshRate.MAX < refreshRate)) {
            throw new IllegalArgumentException(String.format(Locale.US,
                                                             "refreshRate '%d' is out of range.",
                                                             refreshRate));
        }
    }
}
