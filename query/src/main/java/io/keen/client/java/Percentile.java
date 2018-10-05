package io.keen.client.java;

/**
 * A Percentile representation enforcing the format and range specified by the Keen APIs.
 *
 * @author masojus
 */
public class Percentile {
    private static final double MIN_PERCENTILE = 0.01;
    private static final double MAX_PERCENTILE = 100.00;

    private final Double percentile;

    private Percentile(double percentile) {
        this.percentile = percentile;
    }

    /**
     * Gets the raw Double.
     *
     * @return The raw Double representation of this percentile.
     */
    public Double asDouble() {
        return this.percentile;
    }

    /**
     * Creates a Percentile instance with a Double value if in the valid range as described in the
     * <a href="https://keen.io/docs/api/#percentile">Keen APIs</a>. This factory will throw
     * if the given percentile does not strictly satisfy the rules in those API docs. Use
     * {@link #createCoerced(double)} if clamping and rounding is fine.
     *
     * @param percentile The percentile to calculate, supporting (0, 100.00] with two decimal places
     *                   of precision.
     * @return A Percentile instance representing the percentile to calculate.
     */
    public static Percentile createStrict(double percentile) {
        if (MIN_PERCENTILE > percentile || MAX_PERCENTILE < percentile) {
            throw new IllegalArgumentException("The 'percentile' must be in the range (0, 100].");
        }

        if (getNumDecimalPlaces(percentile) > 2) {
            throw new IllegalArgumentException("The 'percentile' has over two decimal places.");
        }

        return new Percentile(percentile);
    }

    /**
     * Same as {@link #createStrict(double)}, but converts to double first. Be careful when
     * widening and narrowing as a precise double representation might not be available. For
     * example, calling createStrict(0.01f) will fail. Most likely in that case, one really means
     * to be calling createCoerced(0.01f) or createStrict(0.01d).
     *
     * @param percentile Refer to {@link #createStrict(double)}.
     * @return Refer to {@link #createStrict(double)}.
     */
    public static Percentile createStrict(Number percentile) {
        return createStrict(percentile.doubleValue());
    }

    /**
     * Creates a Percentile instance with a Double value if in the valid range as described in the
     * <a href="https://keen.io/docs/api/#percentile">Keen APIs</a>. This factory will clamp
     * values into the appropriate range and round to two decimal places.
     *
     * @param percentile The percentile to calculate, coerced to (0, 100.00] with two decimal places
     *                   of precision.
     * @return A Percentile instance representing the percentile to calculate.
     */
    public static Percentile createCoerced(double percentile) {
        // Percentile range is entirely positive, so no need for abs().
        double clamped = Math.max(MIN_PERCENTILE, Math.min(MAX_PERCENTILE, percentile));

        // Since the range is 0-100, and only 2 decimal places this technique is fine.
        double rounded =  ((double)Math.round(clamped * 100)) / 100;

        return new Percentile(rounded);
    }

    /**
     * Same as {@link #createCoerced(double)}, but converts to double first.
     *
     * @param percentile Refer to {@link #createCoerced(double)}.
     * @return Refer to {@link #createCoerced(double)}.
     */
    public static Percentile createCoerced(Number percentile) {
        return createCoerced(percentile.doubleValue());
    }

    private static int getNumDecimalPlaces(double doubleVal) {
        // Trailing zeroes will go away.
        String percentileStr = Double.toString(doubleVal);

        // Check for the 'E' in case it's in scientific notation. Nothing in the appropriate
        // range should be in scientific notation, however.
        if (percentileStr.contains("\u0045")) {
            throw new IllegalArgumentException("The 'doubleVal' string representation is in " +
                "scientific notation.");
        }

        // Take number of chars in String representation, take away the character for the decimal,
        // then subtract the characters for the integer part.
        int numDecimalPlaces = (percentileStr.length() - 1) - percentileStr.indexOf('.');

        return numDecimalPlaces;
    }
}
