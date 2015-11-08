var jStat = require("jStat").jStat,
    DataFrame = require("./dataframe");

var rand = jStat.rand(20, 5);
var largerRand = jStat.map(rand, function(x) { return Math.floor(x * 50) } );
df = new DataFrame(largerRand);
df.show();
df.slice(0,5).show();
