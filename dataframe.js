"use strict";

var Operations = require("./operations.js");
var Expressions = require("./expressions.js");

var Container = Operations.Container;
var EvaluateOperation = Operations.EvaluateOperation;
var SelectOperation = Operations.SelectOperation;
var RenameOperation = Operations.RenameOperation;
var SliceOperation = Operations.SliceOperation;
var ArrangeOperation = Operations.ArrangeOperation;
var FilterOperation = Operations.FilterOperation;
var MutateOperation = Operations.MutateOperation;
var NewDataFrameOperation = Operations.NewDataFrameOperation;
var GenerateRowOperation = Operations.GenerateRowOperation;
var GroupByOperation = Operations.GroupByOperation;
var SummarizeOperation = Operations.SummarizeOperation;

var FunctionExpression = Expressions.FunctionExpression;
var SummaryFunctionExpression = Expressions.SummaryFunctionExpression;

class Step {
  constructor(previousStep) {
    this.previousStep = previousStep;
  }
}

class SliceStep extends Step {
  constructor(previousStep, begin, end) {
    this.begin = begin;
    this.end = end;
  }

  buildOperation(nextOperation) {
    var sliceOp = new SliceOperation(this.begin, this.end);
    sliceOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(sliceOp);
  }
}

class SelectStep extends Step {
  constructor(previousStep, arg) {
    super(previousStep);
    this.arg = arg;
  }

  buildOperation(nextOperation) {
    var selectOp = new SelectOperation(this.arg);
    selectOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(selectOp);
  }
}

class RenameStep extends Step {
  constructor(previousStep, arg) {
    super(previousStep);
    this.arg = arg;
  }

  buildOperation(nextOperation) {
    var renameOp = new RenameOperation(this.arg);
    renameOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(renameOp);
  }
}

class EvaluateStep extends Step {
   constructor(previousStep, arg) {
    super(previousStep);
    this.arg = arg;
  }

  buildOperation(nextOperation) {
    var container = new Container();
    var customOp = this.getOperation(container);
    customOp.setNextOperation(nextOperation);
    var evaluateOp = new EvaluateOperation(this.arg, container);
    evaluateOp.setNextOperation(customOp);
    return this.previousStep.buildOperation(evaluateOp);
  }
}

class MutateStep extends EvaluateStep {
  constructor(previousStep, arg, name) {
    super(previousStep, arg);
    this.name = name;
  }

  getOperation(container) {
    return new MutateOperation(container, this.name);
  }
}

class FilterStep extends EvaluateStep {
  getOperation(container) {
    return new FilterOperation(container);
  }
}

class GroupByStep extends EvaluateStep {
  constructor(arg, name) {
    super(arg);
    this.name = name;
  }

  getOperation(container) {
    return new GroupByOperation(container, this.name);
  }
}

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
    return new LazyDataFrame(this, step);
    return newDataFrame;
  }

  rename(obj) {
    var step = new RenameStep(this._step, obj);
    return new LazyDataFrame(this, step);
  }

  slice(begin, end) {
    var step = new SliceStep(this._step, begin, end);
    return new LazyDataFrame(this, step);
  }

  filter(func) {
    var container = new Container();
    var step = new FilterStep(this._step, func);
    return new LazyDataFrame(this, step);
  }

  mutate(name, func) {
    var step = new MutateStep(this._step, func, name);
    return new LazyDataFrame(this, step);
  }

  groupBy(name, arg) {
    var func;
    if (arg === undefined) {
      func = new Function("row", "return row." + name);
    } else {
      func = arg;
    }
    var step = new GroupByStep(this._step, func, name);
    return new LazyDataFrame(this, step);
  }

  summarize(name, func) {
    var step = new SummarizeStep(this._step, func, name);
    return new LazyDataFrame(this, step);
  }

  collect() {
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
    var requirements = operation.setup(df._columns);
    console.log(requirements);
    operation.addState();
    var numberOfRows = df._numRows;
    for (var rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
      operation.step(rowIndex);
    }
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
    var step = new Step(GenerateRowOperation, []);
    super(null, step);
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
