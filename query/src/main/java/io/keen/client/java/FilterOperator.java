package io.keen.client.java;

/**
 * FilterOperator specifies the operator for the query filter.
 *
 * @author claireyoung
 * @since 1.0.0, 07/24/15
 */
public enum FilterOperator {

    EQUAL_TO(KeenQueryConstants.EQUAL_TO),
    NOT_EQUAL(KeenQueryConstants.NOT_EQUAL),
    LESS_THAN(KeenQueryConstants.LESS_THAN),
    LESS_THAN_EQUAL(KeenQueryConstants.LESS_THAN_EQUAL),
    GREATER_THAN(KeenQueryConstants.GREATER_THAN),
    GREATER_THAN_EQUAL(KeenQueryConstants.GREATER_THAN_EQUAL),
    EXISTS(KeenQueryConstants.EXISTS),
    IN(KeenQueryConstants.IN),
    CONTAINS(KeenQueryConstants.CONTAINS),
    NOT_CONTAINS(KeenQueryConstants.NOT_CONTAINS),
    WITHIN(KeenQueryConstants.WITHIN);

    private final String text;

    private FilterOperator(final String text) {
        this.text = text;
    }

    /**
     * Get the filter operator in String form.
     *
     * @return  filter operator.
     */
    @Override
    public String toString() {
        return text;
    }
}
