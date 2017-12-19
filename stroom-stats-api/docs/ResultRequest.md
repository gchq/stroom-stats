
# ResultRequest

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**componentId** | **String** | The ID of the component that will receive the results corresponding to this ResultRequest | 
**mappings** | [**List&lt;TableSettings&gt;**](TableSettings.md) |  | 
**requestedRange** | [**OffsetRange**](OffsetRange.md) |  | 
**openGroups** | **List&lt;String&gt;** | TODO | 
**resultStyle** | [**ResultStyleEnum**](#ResultStyleEnum) | The style of results required. FLAT will provide a FlatResult object, while TABLE will provide a TableResult object | 
**fetch** | [**FetchEnum**](#FetchEnum) | The fetch mode for the query. NONE means fetch no data, ALL means fetch all known results, CHANGES means fetch only those records not see in previous requests |  [optional]


<a name="ResultStyleEnum"></a>
## Enum: ResultStyleEnum
Name | Value
---- | -----
FLAT | &quot;FLAT&quot;
TABLE | &quot;TABLE&quot;


<a name="FetchEnum"></a>
## Enum: FetchEnum
Name | Value
---- | -----
NONE | &quot;NONE&quot;
CHANGES | &quot;CHANGES&quot;
ALL | &quot;ALL&quot;



