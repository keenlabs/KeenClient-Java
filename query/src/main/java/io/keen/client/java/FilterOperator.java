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

    /**
     * Provides the FilterOperator enum constant that corresponds to the given operator string. This
     * is useful because the text representation, such as is provided by the overridden toString(),
     * is different from the enum constant's name. e.g. - The text representation for GREATER_THAN
     * is "gt".
     *
     * Also : FilterOperator.valueOf(FilterOperator.GREATER_THAN.name()) ==
     *        FilterOperator.fromString(FilterOperator.GREATER_THAN.toString()) ==
     *        FilterOperator.GREATER_THAN
     *
     * @param operator The string describing the filter operator, e.g. "ne" or "exists".
     * @return The corresponding FilterOperator.
     */
    public static FilterOperator fromString(final String operator) {
        for (FilterOperator op : FilterOperator.values()) {
            if (op.toString().equals(operator)) {
                return op;
            }
        }

        return null;
    }
}
