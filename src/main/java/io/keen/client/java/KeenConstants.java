package io.keen.client.java;

/**
 * KeenConstants
 *
 * @author dkador
 * @since 1.0.0
 */
class KeenConstants {

    static final String SERVER_ADDRESS = "https://api.keen.io";
    static final String API_VERSION = "3.0";

    // Keen API constants

    static final String NAME_PARAM = "name";
    static final String DESCRIPTION_PARAM = "description";
    static final String SUCCESS_PARAM = "success";
    static final String ERROR_PARAM = "error";
    static final String INVALID_COLLECTION_NAME_ERROR = "InvalidCollectionNameError";
    static final String INVALID_PROPERTY_NAME_ERROR = "InvalidPropertyNameError";
    static final String INVALID_PROPERTY_VALUE_ERROR = "InvalidPropertyValueError";

    // Keen constants related to how much data we'll cache on the device before aging it out

    // how many events can be stored for a single collection before aging them out
    static final int MAX_EVENTS_PER_COLLECTION = 10000;
    // how many events to drop when aging out
    static final int NUMBER_EVENTS_TO_FORGET = 100;
}
