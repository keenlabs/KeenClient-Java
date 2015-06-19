Keen Query Clients
===================

[![Build Status](https://travis-ci.org/keenlabs/KeenClient-Java.png?branch=master)](https://travis-ci.org/keenlabs/KeenClient-Java)

The query capabilities within the Java Keen client enable you to send POST queries and receive the results of the queries in a JSON object. For query types, refer to API technical reference: https://keen.io/docs/api/reference/.

## Usage
### Building a Keen Query Client
You can build a KeenQueryClient by just providing a KeenProject.
```java
KeenProject queryProject = new KeenProject("<project id>", "<write key>", "<read key>");
KeenQueryClient queryClient = new TestKeenQueryClientBuilder(queryProject).build();
```
Optionally, users can also specify a HTTP Handler, base URL, or JSON Handler:
```java
KeenQueryClient queryClient = new TestKeenQueryClientBuilder(queryProject)
	.withHttpHandler(httpHandler)
	.withJsonHandler(jsonHandler)
	.withBaseUrl(baseURL)
	.build();
```
### Using the KeenQueryClient to send Queries
The most simple way that users can use the KeenQueryClient to send queries is as follows. These methods take only the required query parameters as input, and the user receives a very specific Integer or Double response type. Please note that we strongly encourage users to pass in the Timeframe parameter, but it can be null.
```java
Integer count = queryClient.count("<event_collection>", new Timeframe("this_year"));
Integer countUnique = queryClient.countUnique("<event_collection>", "<target_property>", new Timeframe("this_year"));
Double minimum = queryClient.minimum("<event_collection>", "<target_property>", new Timeframe("this_year"));
Double maximum = queryClient.maximum("<event_collection>", "<target_property>", new Timeframe("this_year"));
Double average = queryClient.average("<event_collection>", "<target_property>", new Timeframe("this_year"));
Double median = queryClient.median("<event_collection>", "<target_property>", new Timeframe("this_year"));
Double percentile = queryClient.percentile("<event_collection>", "<target_property>", new Timeframe("this_year"));
Double sum = queryClient.sum("<event_collection>", "<target_property>", new Timeframe("this_year"));
```
The exceptions are Select Unique, Extraction, Funnel, and Multi-Analysis queries which are more a little more complicated and are mentioned below (TODO: Mention below!!!).

### Advanced
Alternatively, users can use optional parameters to send queries. However, because the result may be different depending on the optional parameters, the return value is a QueryResult. The user is expected to verify the expected return type for the query, given the parameters entered.
```java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_month"));
if (result.isInteger()) {
	Integer countValue = result.getInteger();
	// do something with countValue
}
```

Some special cases are when filters "Group By" and "Inteval" are specified, as well as the Select Unique query.

For Select Unique queries, the user gets a list of unique values, given the target property. Therefore, the QueryResult will be a list of unique property values. The QueryResult type only supports Integer, Double, String, and ArrayList values; therefore, if the property value is not one of the aforementioned types, then it may be of generic Object type.

``` java
Query query = new QueryBuilder(QueryType.SELECT_UNIQUE_RESOURCE)
        .withEventCollection("<event_collection>")
        .withTargetProperty("click-number")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_month"));
if (result.isList()) {
	ArrayList<QueryResult> listResults = result.getList();
	for (QueryResult item : listResults) {
		if (item.isInteger()) {
			// do something with Integer value
		}
		
		// note that for Select Unique query, QueryResult can also be a generic Object,
		// depending on the type of the Target Property.
		if (item.isObject()) {
			// in this case, user is responsible for her own parsing of Object.
		}
	}
}
```

Filtering any query via Group-By will cause the query response to consist of an ArrayList of GroupByResult objects. A GroupByResult is a type of QueryResult that also contains a HashMap<String, Object> of property/values.
``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withGroupBy("click-number")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_month"));
if (result.isList()) {
    ArrayList<QueryResult> listResults = result.getList();
    for (QueryResult item : listResults) {
        if (item instanceof GroupByResult) {
            GroupByResult groupBy = (GroupByResult)item;
            HashMap<String, Object> properties = groupBy.getProperties();
            if (groupBy.isInteger()) {
                // do something with integer result.
            }
        }
    }
}
```
Filtering any query via Interval will cause the query response to consist of an ArrayList of IntervalResult objects. An IntervalResult is a type of QueryResult that also contains a Timeframe for the interval.
``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_year"));
if (result.isList()) {
    ArrayList<QueryResult> listResults = result.getList();
    for (QueryResult item : listResults) {
        if (item instanceof IntervalResult) {
            IntervalResult interval = (IntervalResult)item;
            Timeframe itemTimeframe = interval.getTimeframe();
            if (interval.isInteger()) {
                // do something with integer result.
            }
        }
    }
}      
```
Filtering via both Group By and Interval will cause the query response to consist of an ArrayList of IntervalResult objects. Each IntervalResult object contains an ArrayList of GroupBy objects, as follows:

``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .withGroupBy("click-number")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_year"));
if (result.isList()) {
    ArrayList<QueryResult> listResults = result.getList();
    for (QueryResult item : listResults) {
        if (item instanceof IntervalResult) {
            IntervalResult interval = (IntervalResult)item;
            Timeframe itemTimeframe = interval.getTimeframe();
            if (interval.isList()) {
                ArrayList<QueryResult> groupBys = interval.getList();
                for (QueryResult groupByItem : groupBys) {
                    if (groupByItem instanceof GroupByResult) {
                        GroupByResult groupBy = (GroupByResult)groupByItem;
                        HashMap<String, Object> properties = groupBy.getProperties();
                        if (groupBy.isInteger()) {
                            Integer val = groupBy.getInteger();
                            // do something with integer result.
                        }
                    }
                }
            }
        }
    }
}
```

### Utility Methods

There are also some utility methods to add filters and absolute timeframes to a Query:
```java

// notice this will add two filter parameters, with 1 < click-count < 5
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
	            .withEventCollection("<event_collection>")
	            .withFilter("click-count", "lt", 5)
	            .withFilter("click-count", "gt", 1)
	            .build();

Object result = queryClient.count(query, new Timeframe("<start>", "<end>"));
Integer queryResult = null;
if (result instance Integer) {
	queryResult = (Integer)result;
}
```
### Extraction Query
The [Extraction Resource](https://keen.io/docs/api/reference/#extraction-resource) query has slightly different optional parameters than the other queries (Latest, Email, Content Encoding, Content Type, Property Names), so please refer to the [API documentation](https://keen.io/docs/api/reference/#extraction-resource) on the parameters for this query. Otherwise, users can run the query like any of the other queries.
```java
// this has no return value. It will send you an email with the results.
queryClient.extraction("<event collection>", "your@email", new Timeframe("this_year"));

// return value QueryResult should be an ArrayList of QueryResult Objects.
QueryResult result = queryClient.extraction("<event collection>", new Timeframe("this_year"));

// with any optional parameters
Query query = new QueryBuilder(QueryType.EXTRACTION_RESOURCE)
	            .withEventCollection(TEST_EVENT_COLLECTION)
	            .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_year"));
if (result.isList()) {
	ArrayList<QueryResult> listResult = (ArrayList<QueryResult>)result;
	for (QueryResult extractionItem : listResult) {
		if (extractionItem instanceof Object) {
			// do something with JSON object.
		}
	}
}

```

### Funnel and Multi-Analysis Queries

TODO: The below is an old rough draft. TODO: Rewrite!!!

Special queries such as Funnel and Multi-analysis are also supported, although the user is responsible of constructing her own JSON map for steps and multi-analysis, respectively:
Funnel:
```java
// construct the list of steps
List<Map<String, Object>> listSteps = new ArrayList<Map<String, Object>>();
Map<String, Object> steps = new HashMap<String, Object>();
steps.put(KeenQueryConstants.ACTOR_PROPERTY, "click-count");
steps.put(KeenQueryConstants.EVENT_COLLECTION, "<event collection>");
listSteps.add(steps);

// run query
QueryResult result = queryClient.funnel(listSteps);
```
Multi-Analysis queries require an event collection and an "analyses" object as a parameter. Because the analyses can be complex, the user is responsible for constructing her own analyses object.
```java
// create analyses object
Map<String, Object> analyses = new HashMap<String, Object>();

// add a first set for Count Query.
Map<String, String> firstSet = new HashMap<String, String>();
firstSet.put(KeenQueryConstants.ANALYSIS_TYPE, KeenQueryConstants.COUNT_RESOURCE);
analyses.put("count set", firstSet);

// add a second set for Sum Resource Query, with a target property.
Map<String, String> secondSet = new HashMap<String, String>();
secondSet.put(KeenQueryConstants.ANALYSIS_TYPE, KeenQueryConstants.SUM_RESOURCE);
secondSet.put(KeenQueryConstants.TARGET_PROPERTY, "click-count");
analyses.put("sum set", secondSet);

QueryResult result = queryClient.multiAnalysis("<event collection>", analyses);

```
For Multi-Analysis queries with GroupBy and Interval, please make sure that each analysis key is unique (eg. "count set", "sum set" in the example code), and particularly that they are NOT named "result", "value", or any property names (especially the GroupBy properties).
```java
Query queryParams = new QueryBuilder(QueryType.MULTI_ANALYSIS)
                .withEventCollection("<event_collection>")
                .withAnalyses(analysis)
                .withGroupBy("click-number")
                .withGroupBy("keen.id")
                .withInterval("weekly")
                .build();

QueryResult result = queryClientTest.execute(queryParams, new Timeframe("this_year"));

if (result.isList()) {
    ArrayList<QueryResult> listResults = result.getList();
    for (QueryResult item : listResults) {
        if (item instanceof IntervalResult) {
            IntervalResult interval = (IntervalResult)item;
            Timeframe itemTimeframe = interval.getTimeframe();
            if (interval.isList()) {
                ArrayList<QueryResult> groupBys = interval.getList();
                for (QueryResult groupByItem : groupBys) {
                    if (groupByItem instanceof GroupByResult) {
                        GroupByResult groupBy = (GroupByResult)groupByItem;
                        HashMap<String, Object> properties = groupBy.getProperties();
                        if (groupBy.isMultiAnalysis()) {
                            HashMap<String, QueryResult> multiAnalysisResult = groupBy.getMultiAnalysis();
                            // do something with multi-analysis result.
                        }
                    }
                }
            }
        }
    }
}
```

## Changelog

##### 1.0
Introduce KeenQueryClient.