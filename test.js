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
	}
];

var df = new DataFrame(data);
df.show();
