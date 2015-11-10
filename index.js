function Row(df, rowData) {
  this.df = df;
  var columnNames = df.columnNames;
  var columnTypes = df.columnTypes;
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    var type = columnTypes[i];
    var value = rowData[name];
    if (typeof(value) === type) {
      this[name] = value;
    } else {
      this[name] = null;
    }
  }
}

Row.prototype.show = function() {
  var columnNames = this.df.columnNames;
  var rowString = "";
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    rowString += this[name] + " ";
  }
  console.log(rowString);
}

function DataFrame(data) {
  if (data.length) {
    this.setColumnNames(data[0]);
    this.setColumnTypes(data[0]);
    this.setColumns();
  }
  var rowArray = [];
  var row;
  for (var i = 0; i < data.length; i++) {
    row = new Row(this, data[i]);
    rowArray.push(row);
  }
  this.rowArray = rowArray;
}

DataFrame.prototype.setColumnNames = function(rowData) {
  this.columnNames = Object.keys(rowData);
}

DataFrame.prototype.setColumnTypes = function(rowData) {
  var columnTypes = [];
  var name, value, valueType;
  for (var i = 0; i < this.columnNames.length; i++) {
    name = this.columnNames[i];
    value = rowData[name];
    valueType = typeof(value);
    columnTypes.push(valueType);
  }
  this.columnTypes = columnTypes;
}

DataFrame.prototype.setColumns = function() {
  var columns = [];
  var name, type;
  for (var i = 0; i < this.columnNames.length; i++) {
    name = this.columnNames[i];
    type = this.columnTypes[i];
    var column = new Column(this, name, type, i);
    columns[name] = column;
    columns.push(column);
  }
  this.columns = columns;
}

DataFrame.prototype.row = function(rowIndex) {
  return this.rowArray[rowIndex];
}

DataFrame.prototype.column = function(colIndex) {
  var column = this.rowArray.map(function(row) { return row[colIndex] });
  return column;
}

DataFrame.prototype.slice = function(startIndex, endIndex) {
  return new DataFrame(this.rowArray.slice(startIndex, endIndex));
}

DataFrame.prototype.row = function(rowIndex) {
  return this.rowArray[rowIndex];
}

DataFrame.prototype.cell = function(rowIndex, colIndex) {
  return this.rowArray[rowIndex][colIndex];
}

DataFrame.prototype.show = function() {
  console.log(this.columnNames.join(" "));
  for (var rowIndex = 0; rowIndex < this.rowArray.length; rowIndex++) {
    this.rowArray[rowIndex].show();
  }
}

function Column(df, name, type, index) {
  this.df = df;
  this.name = name;
  this.type = type;
  this.index = index;
}

Column.prototype.get = function(rowIndex) {
  var row = this.df.row(rowIndex)
  return row[this.name];
}


module.exports = DataFrame;







