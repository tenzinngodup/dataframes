var Topology = require("./topology.js").Topology;

var Operations = require("./operations.js");
var Expressions = require("./expressions.js");

var FilterOperation = Operations.FilterOperation;
var MutateOperation = Operations.MutateOperation;
var NullOperation = Operations.NullOperation;
var LogOperation = Operations.LogOperation;
var TapOperation = Operations.TapOperation;
var GenerateRowOperation = Operations.GenerateRowOperation;
var GroupByOperation = Operations.GroupByOperation;
var SummarizeOperation = Operations.SummarizeOperation;

var FunctionExpression = Expressions.FunctionExpression;
var SummaryFunctionExpression = Expressions.SummaryFunctionExpression;

var DataFrame = function(rowData, columnNames) {
  var columns = [];
  var numColumns = columnNames.length;
  var numRows = rowData.length;
  var rowObject, columnName, columnData, value;
  for (var colIndex = 0; colIndex < numColumns; colIndex++) {
    columnData = [];
    columnName = columnNames[colIndex];
    for (var rowIndex = 0; rowIndex < numRows; rowIndex++) {
      var val = rowData[rowIndex][columnName];
      columnData.push(val);
    }
    columns.push(columnData);
    this[columnName] = columnData;
  }
  this._numColumns = numColumns;
  this._numRows = numRows;
  this._columnNames = columnNames;
  this._columns = columns;
  this.setWidths();

  // set up operator
  this._operator = new GenerateRowOperation(this);
  this._lastOperation = this._operator;
  this._groupByOps = [];
}

DataFrame.prototype.setWidths = function() {
  var numColumns = this._numColumns;
  var numRows = this._numRows;
  var columns = this._columns;
  var columnNames = this._columnNames;
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
  var columns = this._columns;
  var columnNames = this._columnNames;
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

DataFrame.prototype._addOperation = function(op) {
  this._lastOperation.setNextOperation(op);
  this._lastOperation = op;
}

DataFrame.prototype.filter = function(func) {
  var filterExp = new FunctionExpression(func);
  var filterOp = new FilterOperation(filterExp);
  this._addOperation(filterOp);
}

DataFrame.prototype.mutate = function(name, func) {
  var mutateExp = new FunctionExpression(func);
  var mutateOp = new MutateOperation(name, mutateExp);
  this._addOperation(mutateOp);
}

DataFrame.prototype.groupBy = function(name, arg) {
  var func;
  if (arg === undefined) {
    func = new Function("row", "return row." + name);
  } else {
    func = arg;
  }
  var groupByExp = new FunctionExpression(func);
  var groupByOp = new GroupByOperation(name, groupByExp);
  this._addOperation(groupByOp);
  this._groupByOps.push(groupByOp);
}

DataFrame.prototype.summarize = function(name, func) {
  var summarizeExp = new SummaryFunctionExpression(func);
  var summarizeOp = new SummarizeOperation(name, summarizeExp);
  this._addOperation(summarizeOp);
  var lastGroupByOp = this._groupByOps.pop();
  if (lastGroupByOp !== undefined) {
    lastGroupByOp.summarizer = summarizeOp;    
  }
}

DataFrame.prototype.tap = function(func) {
  var tapOp = new TapOperation(func);
  this._addOperation(tapOp);
}

DataFrame.prototype._execute = function() {
  this._lastOperation.setNextOperation(new NullOperation());
  this._operator.addState();
  var numberOfRows = this._numRows;
  for (var i = 0; i < numberOfRows; i++) {
    this._executeRow(i);
  }
  this._complete();
}

DataFrame.prototype._executeRow = function(index) {
  this._operator.step(index);
  console.log("Executed row", index);
}

DataFrame.prototype._complete = function(index) {
  this._operator.complete();
}



module.exports = DataFrame;



