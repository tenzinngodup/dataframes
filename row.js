var Row = function(index, columnNames, rowData, states) {
	var colName;
	this.index = index;
  this.states = states;
	this.values = {};
	this.prepValues = [];
	for (var j = 0; j < columnNames.length; j++) {
		colName = columnNames[j];
		this.values[colName] = rowData[colName];
	}
	this.filtersPassed = 0;
	this.groupIndex = 0;
}

var exports = {};

exports.Row = Row;

module.exports = exports;