var Row = function(index, columnNames, rowData) {
	this.index = index;
	this.values = {};
	var colName;
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