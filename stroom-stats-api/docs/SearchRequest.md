
# SearchRequest

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**key** | [**QueryKey**](QueryKey.md) |  | 
**query** | [**Query**](Query.md) |  | 
**resultRequests** | [**List&lt;ResultRequest&gt;**](ResultRequest.md) |  | 
**dateTimeLocale** | **String** | The locale to use when formatting date values in the search results. The value is the string form of a java.time.ZoneId | 
**incremental** | **Boolean** | If true the response will contain all results found so far. Future requests for the same query key may return more results. Intended for use on longer running searches to allow partial result sets to be returned as soon as they are available rather than waiting for the full result set. | 



