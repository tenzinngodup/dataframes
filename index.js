var DataFrame = function(rowData, columnNames) {
  if (rowData.length !== undefined) {
    if (typeof rowData[0] === "object") {
      this.withJSONData(rowData, columnNames);
    }
  }
}

DataFrame.prototype.withJSONData = function(rowData, columnNames) {
  var columns = [];
  var name;
  for (var colIndex = 0; colIndex < columnNames.length; colIndex++) {
    name = columnNames[colIndex];
    var columnExpression = new JSONColumnExpression(rowIndex, rowData, name);
    var column = new Column(name, columnExpression);
    columns.push(column);
    this[name] = column;
  }
  this.columns = columns;
}



module.exports = DataFrame;



