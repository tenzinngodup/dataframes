var Expressions = require("./expressions.js");
var Operations = require("./operations.js");

var ArrayColumnExpression = Expressions.ArrayColumnExpression;
var JSONColumnExpression = Expressions.JSONColumnExpression;
var square = Expressions.square;

var RowIndex = Operations.RowIndex;
var TerminalOperation = Operations.TerminalOperation;
var TapOperation = Operations.TapOperation;
var IterateOperation = Operations.IterateOperation;

var data = [
	{
		"first_name": "George", 
		"last_name": "Washington",
		"birth_year": 1,
		"death_year": 1799,
		"inauguration_year": 1789,
		"last_year": 1797
	},
	{
		"first_name": "John", 
		"last_name": "Adams",
		"birth_year": 2,
		"death_year": 1826,
		"inauguration_year": 1797,
		"last_year": 1801
	},
	{
		"first_name": "Thomas", 
		"last_name": "Jefferson",
		"birth_year": 3,
		"death_year": 1826,
		"inauguration_year": 1801,
		"last_year": 1809
	},
	{
		"first_name": "Thomas", 
		"last_name": "Jefferson",
		"birth_year": 4,
		"death_year": 1826,
		"inauguration_year": 1801,
		"last_year": 1809
	}
];

var jsColumn = new JSONColumnExpression(data, "birth_year");

var sq = square(() => { return jsColumn + square(function() { return jsColumn + 5; }); });

var termOp = new TerminalOperation();
var tapOp = new TapOperation(termOp, sq, function(exp, index) { console.log(exp.value(index))} );
var itOp = new IterateOperation(tapOp, 0, 4);

itOp.onNext();

