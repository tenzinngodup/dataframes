var DataFrame = require("./index");

var data = [
	{
		"first_name": "George", 
		"last_name": "Washington",
		"birth_year": 1732,
		"death_year": 1799,
		"inauguration_year": 1789,
		"last_year": 1797
	},
	{
		"first_name": "John", 
		"last_name": "Adams",
		"birth_year": 1735,
		"death_year": 1826,
		"inauguration_year": 1797,
		"last_year": 1801
	},
	{
		"first_name": "Thomas", 
		"last_name": "Jefferson",
		"birth_year": 1743,
		"death_year": 1826,
		"inauguration_year": 1801,
		"last_year": 1809
	},
	{
		"first_name": "Thomas", 
		"last_name": "Jefferson",
		"birth_year": 1743,
		"death_year": 1826,
		"inauguration_year": 1801,
		"last_year": 1809
	}
];

var df = new DataFrame(data);
//df.show();
//console.log(df.cell(2, 4));
//console.log(df.row(2).toString());
//console.log(df.columns.inauguration_year.toArray());
df = df
  .mutate("years_in_office", function(row) { return row.last_year - row.inauguration_year; })
  .select("first_name", "last_name", "years_in_office", {"years_in_office": "years"})
  .rename({"first_name": "first", "last_name": "last"})
  .distinct("first");
df.show();
console.log(" ")
var promise = df.collect();
promise.then(function(newDf) {
  newDf.show();
}, function(error) {
  throw error;
});

