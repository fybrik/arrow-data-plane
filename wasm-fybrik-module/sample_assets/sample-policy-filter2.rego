package dataapi.authz

rule[{"action": {"name":"Filter2", "op": "<", "value": 22, "column": "age"}, "policy": description}] {
	description := "filter the data according to the given operation, value, and column. Filter the rows that has a value in column `column` that satisfies the operation `op` with value `value`"
	#user context and access type check
	input.action.actionType == "read"
    input.resource.tags.finance
}