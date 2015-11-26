var Expressions = require("./expressions.js");
var Operations = require("./operations.js");
var Rx = require("rx");

var ArrayColumnExpression = Expressions.ArrayColumnExpression;
var JSONColumnExpression = Expressions.JSONColumnExpression;
var square = Expressions.square;

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

var number_data = [];
var name_data = [];
var letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

for (var i = 0; i < 100000; i++) {
	number_data.push(Math.floor(Math.random() * 100));
	name_data.push(letters[Math.floor(Math.random() * 26)]);
}

var number = new ArrayColumnExpression(number_data);
var name = new ArrayColumnExpression(name_data);

var sq = square(() => { return number + square(function() { return number + 5; }); });

var source = Rx.Observable
	.range(0, 100000)
	.groupBy(function(i) { return name.value(i); })
	.selectMany(function(group) {
		return group
		  .sum(function(x) { return sq.value(x); });
	});

var results = [];

console.time("streams");

var subscription = source.subscribe(
  function (x) {
    results.push(x);
  }
);

console.timeEnd("streams");

console.time("manual loop");

var groups = {};

for (var i = 0; i < 100000; i++) {
	var letter = name.value(i);
	if (!groups[letter]) {
		groups[letter] = 0;
	}
	groups[letter] += sq.value(i);
}

var letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
var results = [];
for (var i = 0; i < 26; i++) {
	results.push(groups[letters[i]]);
}

console.timeEnd("manual loop");

