package io.keen.client.java;

/**
 * Commonly used constants in the HTTP query request
 *
 * Created by claireyoung on 5/18/15.
 */
public class KeenQueryConstants {

    // Query names
    static final String COUNT_RESOURCE = "count";
    static final String COUNT_UNIQUE = "count_unique";
    static final String MINIMUM_RESOURCE = "minimum";
    static final String MAXIMUM_RESOURCE = "maximum";
    static final String AVERAGE_RESOURCE = "average";
    static final String MEDIAN_RESOURCE = "median";
    static final String PERCENTILE_RESOURCE = "percentile";
    static final String SUM_RESOURCE = "sum";
    static final String SELECT_UNIQUE_RESOURCE = "select_unique";

    // Query parameters
    static final String EVENT_COLLECTION = "event_collection";
    static final String TARGET_PROPERTY = "target_property";
    static final String FILTERS = "filters";
    static final String TIMEFRAME = "timeframe";
    static final String INTERVAL = "interval";
    static final String TIMEZONE = "timezone";
    static final String GROUP_BY = "group_by";
    static final String MAX_AGE = "max_age";
    static final String PERCENTILE = "percentile";

    // filter property names
    static final String PROPERTY_NAME = "property_name";
    static final String OPERATOR = "operator";
    static final String PROPERTY_VALUE = "property_value";

    // filter operators
    static final String EQUAL_TO = "eq";
    static final String NOT_EQUAL = "ne";
    static final String LESS_THAN = "lt";
    static final String LESS_THAN_EQUAL = "lte";
    static final String GREATER_THAN = "gt";
    static final String GREATER_THAN_EQUAL = "gte";
    static final String EXISTS = "exists";
    static final String IN = "in";
    static final String CONTAINS = "contains";
    static final String NOT_CONTAINS = "not_contains";
    static final String WITHIN = "within";

    // return
    static final String RESULT = "result";
    static final String ERROR_CODE = "error_code";
    static final String MESSAGE = "message";
    static final String VALUE = "value";

    // absolute timeframe
    static final String START = "start";
    static final String END = "end";


}
