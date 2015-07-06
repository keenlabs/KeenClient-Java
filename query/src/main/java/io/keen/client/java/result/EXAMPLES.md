Example of processing the results of a group_by query:

``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withTargetProperty("click-number")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_month"));
for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
    Map<String, Object> groupProperies = groupResult.getKey().getProperties();
    long groupCount = groupResult.getValue().longValue();
    // ... do something with the group properties and the count result
}
```