
# DataSourceField

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | [**TypeEnum**](#TypeEnum) | The data type for the field | 
**name** | **String** | The name of the field | 
**queryable** | **Boolean** | Whether the field can be used in predicate in a query | 
**conditions** | [**List&lt;ConditionsEnum&gt;**](#List&lt;ConditionsEnum&gt;) | The supported predicate conditions for this field | 


<a name="TypeEnum"></a>
## Enum: TypeEnum
Name | Value
---- | -----
FIELD | &quot;FIELD&quot;
NUMERIC_FIELD | &quot;NUMERIC_FIELD&quot;
DATE_FIELD | &quot;DATE_FIELD&quot;
ID | &quot;ID&quot;


<a name="List<ConditionsEnum>"></a>
## Enum: List&lt;ConditionsEnum&gt;
Name | Value
---- | -----
CONTAINS | &quot;CONTAINS&quot;
EQUALS | &quot;EQUALS&quot;
GREATER_THAN | &quot;GREATER_THAN&quot;
GREATER_THAN_OR_EQUAL_TO | &quot;GREATER_THAN_OR_EQUAL_TO&quot;
LESS_THAN | &quot;LESS_THAN&quot;
LESS_THAN_OR_EQUAL_TO | &quot;LESS_THAN_OR_EQUAL_TO&quot;
BETWEEN | &quot;BETWEEN&quot;
IN | &quot;IN&quot;
IN_DICTIONARY | &quot;IN_DICTIONARY&quot;



