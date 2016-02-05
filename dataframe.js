"use strict";

var Expressions = require("./expressions.js");
var Steps = require("./step.js");

var Step = Steps.Step;
var FirstStep = Steps.FirstStep;
var SliceStep = Steps.SliceStep;
var SelectStep = Steps.SelectStep;
var RenameStep = Steps.RenameStep;
var EvaluateStep = Steps.EvaluateStep;
var SummarizeStep = Steps.SummarizeStep;
var MutateStep = Steps.MutateStep;
var FilterStep = Steps.FilterStep;
var GroupByStep = Steps.GroupByStep;
var NewDataFrameStep = Steps.NewDataFrameStep;

var FunctionExpression = Expressions.FunctionExpression;
var SummaryFunctionExpression = Expressions.SummaryFunctionExpression;

class LazyDataFrame {
  constructor(step) {
    this._step = step;
  }

  select(arg) {
    var step = new SelectStep(this._step, arg);
    if (typeof arg === "object" &&
        !Array.isArray(arg) &&
        !(arg instanceof RegExp)) {
      step = new RenameStep(step, arg);
    }
    return new LazyDataFrame(step);
    return newDataFrame;
  }

  rename(obj) {
    var step = new RenameStep(this._step, obj);
    return new LazyDataFrame(step);
  }

  slice(begin, end) {
    var step = new SliceStep(this._step, begin, end);
    return new LazyDataFrame(step);
  }

  filter(func) {
    var container = new Container();
    var step = new FilterStep(this._step, func);
    return new LazyDataFrame(step);
  }

  mutate(name, func) {
    var step = new MutateStep(this._step, func, name);
    return new LazyDataFrame(step);
  }

  groupBy(name, arg) {
    var func;
    if (arg === undefined) {
      func = new Function("row", "return row." + name);
    } else {
      func = arg;
    }
    var step = new GroupByStep(this._step, func, name);
    return new LazyDataFrame(step);
  }

  summarize(name, func) {
    var step = new SummarizeStep(this._step, func, name);
    return new LazyDataFrame(step);
  }

  collect() {
    var finalStep = new NewDataFrameStep(this._step);
    var operation = finalStep.buildOperation();
    operation.setup();
    operation.addState();
    operation.run();
    var result = operation.complete();
    return new DataFrame(result);
  }

  toString() {
    return this.collect().toString();
  }

  inspect() {
    return this.collect().inspect();
  }
}

class DataFrame extends LazyDataFrame {
  constructor(options) {
    super(null);
    if (typeof options === "object") {
      if (options.columnNames !== undefined) {
        if (options.rows !== undefined) {
          this._fromRowObjects(options.rows, options.columnNames);
        } else if (options.columns !== undefined) {
          this._fromColumnArrays(options.columns, options.columnNames, options.columnWidths);
        } else {
          throw "Must specify either 'columns' or 'rows'.";
        }
      } else {
        throw "Must specify 'columnNames'."
      }
    } else {
      throw "Must pass an options object."
    }
    this._step = new FirstStep(this._columns, this._numRows);
  }

  _fromRowObjects(rowData, columnNames) {
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
    this._setWidths();
  }

  _fromColumnArrays(columnData, columnNames, columnWidths) {
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
    if (columnWidths) {
      this._columnWidths = columnWidths;
    } else {
      this._setWidths();
    }
  }

  _setWidths() {
    var numColumns = this._numColumns;
    var columnNames = Array.from(this._columns.keys());
    var columns = Array.from(this._columns.values());
    var numRows = this._numRows;
    var columnWidths = columnNames.map(function(name) { return name.length; });
    var cellWidth;
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

  toString() {
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

  inspect() {
    return this.toString();
  }
}

module.exports = DataFrame;
