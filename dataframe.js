var Operations = require("./operations.js");
var Expressions = require("./expressions.js");

var SelectOperation = Operations.SelectOperation;
var RenameOperation = Operations.RenameOperation;
var FilterOperation = Operations.FilterOperation;
var MutateOperation = Operations.MutateOperation;
var NewDataFrameOperation = Operations.NewDataFrameOperation;
var GenerateRowOperation = Operations.GenerateRowOperation;
var GroupByOperation = Operations.GroupByOperation;
var SummarizeOperation = Operations.SummarizeOperation;

var FunctionExpression = Expressions.FunctionExpression;
var SummaryFunctionExpression = Expressions.SummaryFunctionExpression;

var LazyDataFrame = function(previous, step) {
  this._previous = previous;
  this._step = step;
}

LazyDataFrame.prototype.select = function(arg) {
  var step = new Step(SelectOperation, [arg]);
  return new LazyDataFrame(this, step);
}

LazyDataFrame.prototype.rename = function(obj) {
  var step = new Step(RenameOperation, [obj]);
  return new LazyDataFrame(this, step);
}

LazyDataFrame.prototype.filter = function(func) {
  var step = new Step(FilterOperation, [func]);
  return new LazyDataFrame(this, step);
}

LazyDataFrame.prototype.mutate = function(name, func) {
  var step = new Step(MutateOperation, [name, func]);
  return new LazyDataFrame(this, step);
}

LazyDataFrame.prototype.groupBy = function(name, arg) {
  var func;
  if (arg === undefined) {
    func = new Function("row", "return row." + name);
  } else {
    func = arg;
  }
  var step = new Step(GroupByOperation, [name, func]);
  return new LazyDataFrame(this, step);
}

LazyDataFrame.prototype.summarize = function(name, func) {
  var step = new Step(SummarizeOperation, [name, func]);
  return new LazyDataFrame(this, step);
}

LazyDataFrame.prototype.collect = function() {
  var operation = this._step.createOperation();
  var df = this;
  var previousOperation;
  operation.setNextOperation(new NewDataFrameOperation());
  while (df._previous !== null) {
    df = df._previous;
    previousOperation = df._step.createOperation();
    previousOperation.setNextOperation(operation);
    operation = previousOperation;
  }
  operation.setup(df._columns);
  operation.addState();
  var numberOfRows = df._numRows;
  for (var rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
    operation.step(rowIndex);
  }
  var result = operation.complete();
  return new DataFrame({columns: result.columns, columnNames: result.columnNames});
}

LazyDataFrame.prototype.toString = function() {
  return this.collect().toString();
}

LazyDataFrame.prototype.inspect = function() {
  return this.collect().inspect();
}

var Step = function(operationConstructor, args) {
  this.operationConstructor = operationConstructor;
  this.args = args;
}

Step.prototype.createOperation = function() {
  var operation = Object.create(this.operationConstructor.prototype);
  this.operationConstructor.apply(operation, this.args);
  return operation;
}

var DataFrame = function(options) {
  if (typeof options === "object") {
    if (options.columnNames !== undefined) {
      if (options.rows !== undefined) {
        this.fromRowObjects(options.rows, options.columnNames);
      } else if (options.columns !== undefined) {
        this.fromColumnArrays(options.columns, options.columnNames);
      } else {
        throw "Must specify either 'columns' or 'rows'.";
      }
    } else {
      throw "Must specify 'columnNames'."
    }
  } else {
    throw "Must pass an options object."
  }
  this._previous = null;
  this._step = new Step(GenerateRowOperation, []);
}

DataFrame.prototype = Object.create(LazyDataFrame.prototype);
DataFrame.prototype.constructor = DataFrame;

DataFrame.prototype.fromRowObjects = function(rowData, columnNames) {
  var columns = new Map();
  var numRows = rowData.length;
  var rowObject, columnName, columnData, value;
  var numColumns = columnNames.length;
  for (var colIndex = 0; colIndex < numColumns; colIndex++) {
    columnData = [];
    columnName = columnNames[colIndex];
    for (var rowIndex = 0; rowIndex < numRows; rowIndex++) {
      var val = rowData[rowIndex][columnName];
      val = (val !== undefined) ? val : NaN;
      columnData.push(val);
    }
    columns.set(columnName, columnData);
    this[columnName] = columnData;
  }
  this._numColumns = numColumns;
  this._numRows = numRows;
  this._columns = columns;
  this.setWidths();
}

DataFrame.prototype.fromColumnArrays = function(columnData, columnNames) {
  var columns = new Map();
  var numColumns = columnData.length;
  if (numColumns !== columnNames.length) {
    throw "'columns' and 'columnNames' must have the same number of elements.";
  }
  if (numColumns === 0) {
    throw "'columns' must not be an empty array.";
  }
  var numRows = undefined;
  var column, columnName;
  for (var colIndex = 0; colIndex < numColumns; colIndex++) {
    column = columnData[colIndex];
    columnName = columnNames[colIndex];
    if (!Array.isArray(column)) {
      throw "All columns must be arrays.";
    }
    if (numRows === undefined) {
      numRows = column.length;
    } else if (column.length !== numRows) {
      throw "All columns must have the same number of elements.";
    }
    columns.set(columnName, column);
  }
  this._numColumns = numColumns;
  this._numRows = numRows;
  this._columns = columns;
  this.setWidths();
}

DataFrame.prototype.setWidths = function() {
  var numColumns = this._numColumns;
  var columnNames = Array.from(this._columns.keys());
  var columns = Array.from(this._columns.values());
  var numRows = this._numRows;
  var columnWidths = [];
  var cellWidth;
  for (var colIndex = 0; colIndex < numColumns; colIndex++) {
    columnWidths.push(columnNames[colIndex].length);
  }
  for (var rowIndex = 0; rowIndex < numRows; rowIndex++) {
    for (var colIndex = 0; colIndex < numColumns; colIndex++) {
      cellWidth = columns[colIndex][rowIndex].toString().length;
      if (cellWidth > columnWidths[colIndex]) {
        columnWidths[colIndex] = cellWidth;
      }
    }
  }
  this._columnWidths = columnWidths;
}

DataFrame.prototype.toString = function() {
  var str = "";
  var numColumns = this._numColumns;
  var numRows = this._numRows;
  var columns = Array.from(this._columns.values());
  var columnNames = Array.from(this._columns.keys());
  var columnWidths = this._columnWidths;
  var columnPaddings = [];
  var paddingString;
  for (var colIndex = 0; colIndex < numColumns; colIndex++) {
    paddingString = Array(columnWidths[colIndex] + 1).join(" ");
    columnPaddings.push(paddingString);
    str += (columnNames[colIndex] + paddingString).substring(0, paddingString.length) + " |";
  }
  str += "\n";
  for (var rowIndex = 0; rowIndex < numRows; rowIndex++) {
    for (var colIndex = 0; colIndex < numColumns; colIndex++) {
      paddingString = columnPaddings[colIndex];
      str += (columns[colIndex][rowIndex] + paddingString).substring(0, paddingString.length) + " |";
    }
    str += "\n"
  }
  return str;
}

DataFrame.prototype.inspect = function() {
  return this.toString();
}


module.exports = DataFrame;



