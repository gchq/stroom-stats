
# TableSettings

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**queryId** | **String** | TODO | 
**fields** | [**List&lt;Field&gt;**](Field.md) |  | 
**extractValues** | **Boolean** | TODO |  [optional]
**extractionPipeline** | [**DocRef**](DocRef.md) |  |  [optional]
**maxResults** | **List&lt;Integer&gt;** | Defines the maximum number of results to return at each grouping level, e.g. &#39;1000,10,1&#39; means 1000 results at group level 0, 10 at level 1 and 1 at level 2. In the absence of this field system defaults will apply |  [optional]
**showDetail** | **Boolean** | When grouping is used a value of true indicates that the results will include the full detail of any results aggregated into a group as well as their aggregates. A value of false will only include the aggregated values for each group. Defaults to false. |  [optional]



