package io.keen.client.java;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test the Percentile helper class functionality.
 *
 * @author masojus
 */
public class PercentileTest extends KeenQueryTestBase {
    @Test
    public void testPercentileStrict_NormalValues() {
        double lowNormal = 0.05;
        Percentile p1 = Percentile.createStrict(lowNormal);
        assertEquals(lowNormal, p1.asDouble(), DOUBLE_CMP_DELTA);

        double mediumNormal = 55.43;
        Percentile p2 = Percentile.createStrict(mediumNormal);
        assertEquals(mediumNormal, p2.asDouble(), DOUBLE_CMP_DELTA);

        double highNormal = 97d;
        Percentile p3 = Percentile.createStrict(highNormal);
        assertEquals(highNormal, p3.asDouble(), DOUBLE_CMP_DELTA);

        Integer normalInt = 33;
        Percentile p4 = Percentile.createStrict(normalInt);
        assertEquals(normalInt.intValue(), p4.asDouble().intValue());

        Long normalLong = 25L;
        Percentile p6 = Percentile.createStrict(normalLong);
        assertEquals(normalLong.longValue(), p6.asDouble().longValue());

        // Trailing zeroes shouldn't count as more decimal places.
        Percentile p7 = Percentile.createStrict(99.92000);
        assertEquals(99.92, p7.asDouble(), DOUBLE_CMP_DELTA);

        // Scientific notation here shouldn't affect things.
        Percentile p8 = Percentile.createStrict(5.234e1);
        assertEquals(52.34, p8.asDouble(), DOUBLE_CMP_DELTA);
    }

    @Test
    public void testPercentileStrict_BoundaryValues() {
        double lowOk = 0.02;
        Percentile p1 = Percentile.createStrict(lowOk);
        assertEquals(lowOk, p1.asDouble(), DOUBLE_CMP_DELTA);

        double lowLimit = 0.01;
        Percentile p2 = Percentile.createStrict(lowLimit);
        assertEquals(lowLimit, p2.asDouble(), DOUBLE_CMP_DELTA);

        try {
            double lowBad = 0.00;
            Percentile.createStrict(lowBad);
            fail("Expected IllegalArgumentException creating Percentile out of range.");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(),
                       allOf(containsString("range"), containsString("(0, 100]")));
        }

        double highOk = 99.99;
        Percentile p4 = Percentile.createStrict(highOk);
        assertEquals(highOk, p4.asDouble(), DOUBLE_CMP_DELTA);

        double highLimit = 100.00;
        Percentile p5 = Percentile.createStrict(highLimit);
        assertEquals(highLimit, p5.asDouble(), DOUBLE_CMP_DELTA);

        try {
            double highBad = 100.01;
            Percentile.createStrict(highBad);
            fail("Expected IllegalArgumentException creating Percentile out of range.");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(),
                       allOf(containsString("range"), containsString("(0, 100]")));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPercentile_TooManyDecimalPlaces() {
        Percentile.createStrict(0.00 + Double.MIN_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPercentile_TooManyDecimalPlaces2() {
        Percentile.createStrict(0.01f);
    }

    @Test
    public void testPercentileCoerced_NormalValues() {
        double lowNormal = 0.05;
        Percentile p1 = Percentile.createCoerced(lowNormal);
        assertEquals(lowNormal, p1.asDouble(), DOUBLE_CMP_DELTA);

        double mediumNormal = 55.43;
        Percentile p2 = Percentile.createCoerced(mediumNormal);
        assertEquals(mediumNormal, p2.asDouble(), DOUBLE_CMP_DELTA);

        double highNormal = 97d;
        Percentile p3 = Percentile.createCoerced(highNormal);
        assertEquals(highNormal, p3.asDouble(), DOUBLE_CMP_DELTA);

        Integer normalInt = 33;
        Percentile p4 = Percentile.createCoerced(normalInt);
        assertEquals(normalInt.intValue(), p4.asDouble().intValue());

        Long normalLong = 25L;
        Percentile p6 = Percentile.createCoerced(normalLong);
        assertEquals(normalLong.longValue(), p6.asDouble().longValue());

        // Trailing zeroes shouldn't count as more decimal places.
        Percentile p7 = Percentile.createCoerced(99.92000);
        assertEquals(99.92, p7.asDouble(), DOUBLE_CMP_DELTA);

        // Scientific notation here shouldn't affect things.
        Percentile p8 = Percentile.createCoerced(5.234e1);
        assertEquals(52.34, p8.asDouble(), DOUBLE_CMP_DELTA);
    }

    @Test
    public void testPercentileCoerced_CoercedValues() {
        final double MIN_PERCENTILE = 0.01;
        final double MAX_PERCENTILE = 100.00;

        Percentile p1 = Percentile.createCoerced(99.92345);
        assertEquals(99.92, p1.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p2 = Percentile.createCoerced(100.01);
        assertEquals(MAX_PERCENTILE, p2.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p3 = Percentile.createCoerced(0.00 + Double.MIN_VALUE);
        assertEquals(MIN_PERCENTILE, p3.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p4 = Percentile.createCoerced(-1454545.098545);
        assertEquals(MIN_PERCENTILE, p4.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p5 = Percentile.createCoerced(14569545.04557893);
        assertEquals(MAX_PERCENTILE, p5.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p6 = Percentile.createCoerced(57.755);
        assertEquals(57.76, p6.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p7 = Percentile.createCoerced(57.754);
        assertEquals(57.75, p7.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p8 = Percentile.createCoerced(51.450001);
        assertEquals(51.45, p8.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p9 = Percentile.createCoerced(0.01500);
        assertEquals(0.02, p9.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p10 = Percentile.createCoerced(5.23416782e1);
        assertEquals(52.34, p10.asDouble(), DOUBLE_CMP_DELTA);

        Percentile p11 = Percentile.createCoerced(5.23456782e1);
        assertEquals(52.35, p11.asDouble(), DOUBLE_CMP_DELTA);
    }
}
