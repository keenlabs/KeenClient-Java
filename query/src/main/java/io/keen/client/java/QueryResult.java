package io.keen.client.java;


import java.lang.reflect.Array;
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.HashMap;

import javax.management.Query;

/**
 * Created by claireyoung on 6/16/15.
 */
public class QueryResult {
    private Integer integer;
    private Double doubleValue;
    private GroupBy groupBy;
    private Interval interval;
    private String str;
    private ArrayList<QueryResult> list;


    // Constructors - TODO: make them Protected?
    QueryResult(Integer integer) {
        this.integer = integer;
    }

    QueryResult(Double doubleVal) {
        this.doubleValue = doubleVal;
    }

    QueryResult(String str) {
        this.str = str;
    }

    QueryResult(GroupBy groupBy) {
        this.groupBy = groupBy;
    }

    QueryResult(Interval interval) {
        this.interval = interval;
    }

    QueryResult(ArrayList<QueryResult> list) {
        this.list = list;
    }


    public boolean isInteger() {
        return integer != null;
    }

    public boolean isDouble() {
        return doubleValue != null;
    }

    public boolean isGroupBy() {
        return groupBy != null;
    }

    public boolean isInterval() {
        return interval != null;
    }

    public boolean isList() {
        return list != null;
    }

    public boolean isString() {
        return str != null;
    }

    public Integer getInteger() {
        return integer;
    }

    public Double getDouble() {
        return doubleValue;
    }

    public GroupBy getGroupBy() {
        return groupBy;
    }

    public Interval getInterval() {
        return interval;
    }

    public String getString() {return str;}

    public ArrayList<QueryResult> getList() {
        return list;
    }

    public static QueryResult constructQueryResult(Object input, boolean isGroupBy, boolean isInterval) {
        QueryResult thisObject = null;
        if (input instanceof Integer) {
            thisObject = new QueryResult((Integer)input);
        } else if (input instanceof Double) {
            thisObject = new QueryResult((Double)input);
        } else if (input instanceof String) {
            thisObject = new QueryResult((String)input);
        } else if (input instanceof ArrayList) {

            // recursively construct the children of this...
            ArrayList<QueryResult> listOutput = new ArrayList<QueryResult>();
            ArrayList<Object> listInput = (ArrayList<Object>)input;
            for (Object child : listInput) {
                QueryResult resultItem = constructQueryResult(child, isGroupBy, isInterval);
                listOutput.add(resultItem);
            }
            thisObject = new QueryResult(listOutput);
        } else {
            if (input instanceof HashMap) {
                HashMap<String, Object> inputMap = (HashMap<String, Object>) input;
                if (isInterval) {
                    Timeframe timeframeOutput = null;
                    if (inputMap.containsKey(KeenQueryConstants.TIMEFRAME) && inputMap.containsKey(KeenQueryConstants.VALUE)) {
                        Object timeframe = inputMap.get(KeenQueryConstants.TIMEFRAME);
                        if (timeframe instanceof HashMap) {
                            HashMap<String, String> hashTimeframe = (HashMap<String, String>) timeframe;
                            String start = hashTimeframe.get(KeenQueryConstants.START);
                            String end = hashTimeframe.get(KeenQueryConstants.END);
                            timeframeOutput = new Timeframe(start, end);
                        } else if (timeframe instanceof String) {
                            timeframeOutput = new Timeframe((String) timeframe);
                        }

                        Object value = inputMap.get(KeenQueryConstants.VALUE);
                        QueryResult queryResultValue = constructQueryResult(value, isGroupBy, false);
                        thisObject = new QueryResult(new Interval(timeframeOutput, queryResultValue));
                    }
                }
                if (isGroupBy) {
                    if (inputMap.containsKey(KeenQueryConstants.RESULT)) {
                        QueryResult result = null;
                        HashMap<String, QueryResult> properties = new HashMap<String, QueryResult>();
                        for (String key : inputMap.keySet()) {
                            if (key.equals(KeenQueryConstants.RESULT)) {
                                result = constructQueryResult(inputMap.get(key), false, isInterval);
                            } else {
                                properties.put(key, constructQueryResult(inputMap.get(key), false, isInterval));
                            }
                        }
                        thisObject = new QueryResult(new GroupBy(properties, result));
                    }
                }
            }
        }

        return thisObject;
    }

}
