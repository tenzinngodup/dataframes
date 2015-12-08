var Expressions = require("./expressions.js");
var Operations = require("./operations.js");
var Rx = require("rx");

var FilterOperation = Operations.FilterOperation;
var MutateOperation = Operations.MutateOperation;

var ColumnExpression = Expressions.ColumnExpression;
var square = Expressions.square;

var data = [
	{
		"first_name": "George", 
		"last_name": "Washington",
		"party": "No Party",
		"pres_number": 1,
		"death_year": 1799,
		"inauguration_year": 1789,
		"last_year": 1797
	},
	{
		"first_name": "John", 
		"last_name": "Adams",
		"party": "Federalist", 
		"pres_number": 2,
		"death_year": 1826,
		"inauguration_year": 1797,
		"last_year": 1801
	},
	{
		"first_name": "Thomas", 
		"last_name": "Jefferson",
		"party": "Democratic-Republican", 
		"pres_number": 3,
		"death_year": 1826,
		"inauguration_year": 1801,
		"last_year": 1809
	},
	{
		"first_name": "James", 
		"last_name": "Madison",
		"party": "Democratic-Republican", 
		"pres_number": 4,
		"death_year": 1826,
		"inauguration_year": 1801,
		"last_year": 1809
	}
];

var first_name = new ColumnExpression("first_name");
var pres_number = new ColumnExpression("pres_number");

var sq = square(function() { return pres_number + square(pres_number); });

var Row = function(index, columnNames, rowData) {
	var colName;
	this.index = index;
	this.values = {};
	for (var j = 0; j < columnNames.length; j++) {
		colName = columnNames[j];
		this.values[colName] = rowData[colName];
	}
	this.filtersPassed = 0;
	this.groupIndex = NaN;
	this.operationValues = new Map();
}

var Operator = function() {
	this.groups = [];
	this.filtersTested = 0;
}

var Topology = function() {
	this.columns = [];
	this.operations = [];
}

Topology.prototype.addOperation = function(operation) {
	this.addDependencies(operation);
}

Topology.prototype.addDependencies = function(operation) {
	var opExpression = operation.expression;
	if (opExpression instanceof ColumnExpression) {
		if (this.columns.indexOf(opExpression.name) === -1) {
			this.columns.push(opExpression.name);			
		}
	} else {
		var dependencies = opExpression.dependencies();		
		for (var i = 0; i < dependencies.length; i++) {
			dependency = dependencies[i];
			this.addDependencies(new MutateOperation(dependency, dependency.getSignature()));
		}
		this.operations.push(operation);
	}
}

Topology.prototype.execute = function(row) {
	for (var i = 0; i < this.operations.length; i++) {
		this.operations[i].execute(row);
	}
}

var mutation = new MutateOperation(sq, "pres_number_squared");
var filter = new FilterOperation(function() { return first_name === "Thomas" });

var top = new Topology();

top.addOperation(mutation);

var row;
for (var i = 0; i < 4; i++) {
	row = new Row(i, top.columns, data[i]);
	top.execute(row);
	console.log(row);
}


