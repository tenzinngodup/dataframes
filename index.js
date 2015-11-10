var DataFrame = function(data) {
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
  this._index = 0;
}

var RowIterator = function(df) {
  this.df = df;
  this.index = 0;
}

RowIterator.prototype.next = function() {
  var rowArray = this.df.rowArray;
  if (this.index < rowArray.length) {
    var result = {done: false, value: rowArray[this.index]};
    this.index++;
    return result;
  } else {
    return {done: true}
  }
}

RowIterator.prototype["@@iterator"] = function() {
  return this;
}

DataFrame.prototype.setColumnNames = function(rowData) {
  this.columnNames = Object.keys(rowData);
}

DataFrame.prototype["@@iterator"] = function() {
  return new RowIterator(this);
}

DataFrame.prototype.setColumnTypes = function(rowData) {
  var columnTypes = [];
  var name, value, valueType;
  for (var i = 0; i < this.columnNames.length; i++) {
    name = this.columnNames[i];
    value = rowData[name];
    valueType = typeof value;
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

DataFrame.prototype.columnArray = function(colIdentifier) {
  var columnArray = [];
  var colName = typeof colIdentifier === "number" ? this.columnNames[colIdentifier] : colIdentifier;
  for (var i = 0; i < this.rowArray.length; i++) {
    columnArray.push(this.rowArray[colName]);
  }
  return columnArray;
}

DataFrame.prototype.slice = function(startIndex, endIndex) {
  return new DataFrame(this.rowArray.slice(startIndex, endIndex));
}

DataFrame.prototype.row = function(rowIndex) {
  return this.rowArray[rowIndex];
}

DataFrame.prototype.cell = function(rowIndex, colIndex) {
  var row = this.rowArray[rowIndex];
  return typeof row !== "undefined" ? row.get(colIndex) : undefined;
}

DataFrame.prototype.show = function() {
  console.log(this.columnNames.join(" "));
  for (var rowIndex = 0; rowIndex < this.rowArray.length; rowIndex++) {
    console.log(this.rowArray[rowIndex].toString());
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

Column.prototype.toArray = function() {
  var columnArray = [];
  var rowIterator = this.df["@@iterator"]();
  var next = rowIterator.next();
  while (!next.done) {
    var row = next.value;
    columnArray.push(row[this.name]);
    next = rowIterator.next();
  }
  return columnArray;
}

Column.prototype["@@iterator"] = function() {
  return new ColumnIterator;
}

var ColumnIterator = function(column) {
  var df = column.df;
  this.columnName = column.name;
  this.rowIterator = df["@@iterator"]();
}

ColumnIterator.prototype.next = function() {
  var nextRowValue = this.rowIterator.next();
  if (nextRowValue.done) {
    return {done: true};
  } else {
    var row = nextRowValue.value;
    return {done: false, value: row[this.columnName]}
  }
}

function Row(df, rowData) {
  this.df = df;
  var columnNames = df.columnNames;
  var columnTypes = df.columnTypes;
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    var type = columnTypes[i];
    var value = rowData[name];
    if (typeof value === type) {
      this[name] = value;
    } else {
      this[name] = null;
    }
  }
}

Row.prototype.get = function(colIdentifier) {
  return typeof colIdentifier === "number" ? this.getByIndex(colIdentifier) : this[colIdentifier];
}

Row.prototype.getByIndex = function(colIndex) {
  var name = this.df.columnNames[colIndex];
  return this[name];
}

Row.prototype.toString = function() {
  var columnNames = this.df.columnNames;
  var rowString = "";
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    rowString += this[name] + " ";
  }
  return rowString;
}

module.exports = DataFrame;







