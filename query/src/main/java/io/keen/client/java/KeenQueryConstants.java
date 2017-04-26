package io.keen.client.java;

/**
 * Commonly used constants in the HTTP query request
 *
 * @author claireyoung, baumatron, masojus
 * @since 1.0.0, 05/18/15
 */
class KeenQueryConstants {
    // Query names
    static final String COUNT = "count";
    static final String COUNT_UNIQUE = "count_unique";
    static final String MINIMUM = "minimum";
    static final String MAXIMUM = "maximum";
    static final String AVERAGE = "average";
    static final String MEDIAN = "median";
    static final String PERCENTILE_RESOURCE = "percentile";
    static final String SUM = "sum";
    static final String SELECT_UNIQUE = "select_unique";
    static final String FUNNEL = "funnel";
    static final String MULTI_ANALYSIS = "multi_analysis";

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

    // funnel
    static final String ACTOR_PROPERTY = "actor_property";
    static final String STEPS = "steps";
    static final String INVERTED = "inverted";
    static final String OPTIONAL = "optional";
    static final String WITH_ACTORS = "with_actors";
    static final String ACTORS = "actors";

    // multi-analysis
    static final String ANALYSES = "analyses";
    static final String ANALYSIS_TYPE = "analysis_type";

    // return
    static final String RESULT = "result";
    static final String ERROR_CODE = "error_code";
    static final String MESSAGE = "message";
    static final String VALUE = "value";

    // absolute timeframe
    static final String START = "start";
    static final String END = "end";

    // Meta queries
    static final String SAVED = "saved";
    static final String DATASETS = "datasets";
    static final String QUERY = "query";
    static final String REFRESH_RATE = "refresh_rate";
    static final String METADATA = "metadata";
    static final String DISPLAY_NAME = "display_name";
    static final String QUERY_NAME = "query_name";
}
