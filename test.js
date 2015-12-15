var Expressions = require("./expressions.js");
var Topology = require("./topology.js").Topology;

var ColumnExpression = Expressions.ColumnExpression;
var square = Expressions.square;
var cumsum = Expressions.cumsum;
var Expression = Expressions.Expression;

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

var simple = new Expression(function(row) { return cumsum(row.pres_number); });
var filtered = new Expression(function(row) { return cumsum(row.pres_number); });


var top = new Topology();

top.addMutation(simple, "simple_cumsum");
top.generate(data);

for (var i = 0; i < 4; i++) {
	top.execute(i);
}


