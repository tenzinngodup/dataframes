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
df = df
  .mutate("years", function(row) { return row.last_year - row.inauguration_year })
  .select("first_name", "last_name", "last_year", "years")
  .rename({"first_name": "first", "last_name": "last"})
  .groupBy("years")
  .summarize(
    {
      "total_years": 
        function(result, row) {
          return result + row["last_year"];
        },
      "combined_first_names":
        function(result, row) {
          result = result || "";
          return "" + result + row["first"] + " ";
        }
  });
df = df.collect().show();
