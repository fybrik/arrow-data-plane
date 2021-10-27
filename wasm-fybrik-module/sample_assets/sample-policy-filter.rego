package dataapi.authz

#rule[{"action": {"name":"FilterMinors", "age": 18, "column": "age"}, "policy": description}] {
#	description := "filter minor"
#	#user context and access type check
#	input.action.actionType == "read"
#}

rule[{"action": {"name":"Filter", "op": ">", "value": 18, "column": "age"}, "policy": description}] {
	description := "filter the data according to the given operation, value, and column. Filter the rows that has a value in column `column` that satisfies the operation `op` with value `value`"
	#user context and access type check
	input.action.actionType == "read"
  input.resource.tags.finance
}