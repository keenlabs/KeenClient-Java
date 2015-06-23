package io.keen.client.java;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by claireyoung on 6/16/15.
 */
public class QueryResult {
    private Integer integer;
    private Double doubleValue;
    private String str;
    private ArrayList<QueryResult> list;
    private Object object;  // to be used carefully!!!
    private HashMap<String, QueryResult> multiAnalysis;


    // Constructors
    protected QueryResult(QueryResult queryResult) {
        this.integer = queryResult.integer;
        this.doubleValue = queryResult.doubleValue;
        this.str = queryResult.str;
        this.list = queryResult.list;
        this.object = queryResult.object;
        this.multiAnalysis = queryResult.multiAnalysis;
    }

    private QueryResult(Integer integer) { this.integer = integer; }

    private QueryResult(Double doubleVal) { this.doubleValue = doubleVal; }

    private QueryResult(String str) { this.str = str; }

    private QueryResult(ArrayList<QueryResult> list) { this.list = list; }

    private QueryResult(Object object) { this.object = object; }

    private QueryResult(HashMap<String, QueryResult> multiAnalysisResult) {
        this.multiAnalysis = multiAnalysisResult;
    }

    // validation methods
    public boolean isInteger() {
        return integer != null;
    }

    public boolean isDouble() {
        return doubleValue != null;
    }

    public boolean isList() { return list != null; }

    public boolean isString() { return str != null; }

    public boolean isObject() {return object != null; }

    public boolean isMultiAnalysis() {return multiAnalysis != null; }

    // getters
    public Integer getInteger() {
        return integer;
    }

    public Double getDouble() { return doubleValue; }

    public String getString() {return str;}

    public HashMap<String, QueryResult> getMultiAnalysis() { return multiAnalysis; }

    public ArrayList<QueryResult> getList() {
        return list;
    }

    public Object getObject() { return object; }

    // Construct Query Result
    public static QueryResult constructQueryResult(Object input, boolean isGroupBy, boolean isInterval, Set<String> multiAnalysisKeys) {
        QueryResult thisObject = null;

        // below code determines what type of object QueryResult holds.
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
                QueryResult resultItem = constructQueryResult(child, isGroupBy, isInterval, multiAnalysisKeys);
                listOutput.add(resultItem);
            }
            thisObject = new QueryResult(listOutput);
        } else {
            if (input instanceof HashMap) {

                HashMap<String, Object> inputMap = (HashMap<String, Object>) input;

                // First, we test whether the input Object is a multiAnalysis HashMap with all
                // the multiAnalysisKeys. If so, this Object is a MultiAnalysis result.
                QueryResult multiAnalysisResult = null;
                if (multiAnalysisKeys != null && multiAnalysisKeys.isEmpty() == false) {
                    boolean isMultiAnalysisResult = true;
                    // if all keys are in this HashMap, then we have a multiAnalysis.
                    for (String key : multiAnalysisKeys) {
                        if (inputMap.containsKey(key) == false) {
                            isMultiAnalysisResult = false;
                        }
                    }

                    // construct the MultiAnalysisResult object.
                    if (isMultiAnalysisResult) {
                        HashMap<String, QueryResult> outputResult = new HashMap<String, QueryResult>();
                        for (String key : multiAnalysisKeys) {
                            outputResult.put(key, constructQueryResult(inputMap.get(key), isGroupBy, isInterval, multiAnalysisKeys));
                        }
                        multiAnalysisResult = new QueryResult(outputResult);
                    }
                }


                // Next, we try to detect Interval or GroupBy.
                // if there is an interval or groupBy, we expect to process them at
                // the top level. When we recurse, we want to just make sure that
                // we don't have any nested Intervals or GroupByResult's by explicitly setting
                // them to false.
                if (isInterval) {

                    // If this is an interval, it should have keys "timeframe" and "value"
                    if (inputMap.containsKey(KeenQueryConstants.TIMEFRAME) && (inputMap.containsKey(KeenQueryConstants.VALUE) || multiAnalysisResult != null)) {
                        Timeframe timeframeOutput = null;
                        Object timeframe = inputMap.get(KeenQueryConstants.TIMEFRAME);
                        if (timeframe instanceof HashMap) {
                            HashMap<String, String> hashTimeframe = (HashMap<String, String>) timeframe;
                            String start = hashTimeframe.get(KeenQueryConstants.START);
                            String end = hashTimeframe.get(KeenQueryConstants.END);
                            timeframeOutput = new Timeframe(start, end);
                        } else if (timeframe instanceof String) {
                            timeframeOutput = new Timeframe((String) timeframe);
                        }

                        if (multiAnalysisResult == null) {
                            Object value = inputMap.get(KeenQueryConstants.VALUE);
                            QueryResult queryResultValue = constructQueryResult(value, isGroupBy, false, multiAnalysisKeys);
                            thisObject = new IntervalResult(timeframeOutput, queryResultValue);
                        } else {
                            thisObject = new IntervalResult(timeframeOutput, multiAnalysisResult);
                        }
                    }
                } else if (isGroupBy) {

                    // If this is a GroupByResult, it should have key "result", along with properties to group by.
                    if (inputMap.containsKey(KeenQueryConstants.RESULT) || multiAnalysisResult != null) {
                        QueryResult result = null;
                        HashMap<String, Object> properties = new HashMap<String, Object>();
                        for (String key : inputMap.keySet()) {
                            if (multiAnalysisResult != null && multiAnalysisKeys.contains(key)) {
                                // do nothing.
                            } else if (key.equals(KeenQueryConstants.RESULT)) {
                                // there should not be intervals nested inside GroupByResult's; only
                                // the other way around.
                                result = constructQueryResult(inputMap.get(key), false, false, multiAnalysisKeys);
                            } else {
                                properties.put(key, inputMap.get(key));
                            }
                        }

                        if (multiAnalysisResult != null) {
                            result = multiAnalysisResult;
                        }
                        thisObject = new GroupByResult(properties, result);
                    }
                } else if (multiAnalysisResult != null) {
                    // multiAnalysis that is NOT in a group-by or Interval.
                    // This is the most typical multiAnalysis result.
                    thisObject = multiAnalysisResult;
                }
            }
        }

        // this is a catch-all for Select Unique queries, where the object can be of any type.
        if (thisObject == null) {
            thisObject = new QueryResult(input);
        }
        return thisObject;
    }

}
