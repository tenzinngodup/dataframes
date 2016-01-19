var Expressions = require("./expressions.js");
var FunctionExpression = Expressions.FunctionExpression;
var SummaryFunctionExpression = Expressions.SummaryFunctionExpression;
var nullExp = Expressions.nullExp;

var Index = function() {
  this.value = 0;
}

var RowValues = function(propertyMap) {
  for (var keyval of propertyMap) {
    Object.defineProperty(this, keyval[0], keyval[1]);
  }
}

var Row = function(propertyMap, rowIndex, grouping) {
  this.propertyMap = propertyMap;
  this.rowIndex = rowIndex;
  this.grouping = grouping;
  this.values = new RowValues(propertyMap);
}

var Grouping = function(parentGrouping, groupName) {
  this.parentGrouping = parentGrouping;
  if (parentGrouping === null) {
    this.groupName = null;
    this.keys = null;
    this.parents = null;
  } else {
    this.groupName = groupName;
    this.keys = [];
    this.parents = [];    
  }
  this.index = new Index();
}

var Result = function() {
	this.groupings = [];
}

Result.prototype.pushGrouping = function(grouping) {
	this.groupings.push(grouping);
}

Result.prototype.popGrouping = function() {
	return this.groupings.pop();
}

var Operation = function() {
  this.nextOperation = null;
}

Operation.prototype.init = function() {
  return null;
}

Operation.prototype.setNextOperation = function(nextOperation) {
  this.nextOperation = nextOperation;
}

Operation.prototype.setup = function(row) {
  this.nextOperation.setup(row);
}

Operation.prototype.step = function(row) {
  var row = this.execute(row);
  this.nextOperation.step(row);
}

Operation.prototype.addState = function() {
  this.nextOperation.addState();
}

Operation.prototype.execute = function(row) {
  return row;
}

Operation.prototype.complete = function() {
  return this.nextOperation.complete();
}

var ExpressionOperation = function(exp) {
  this.expression = exp;
  this.statefulChain = exp.getStatefulChain();
  Operation.call(this);
}
ExpressionOperation.prototype = Object.create(Operation.prototype);
ExpressionOperation.prototype.constructor = ExpressionOperation;

ExpressionOperation.prototype.addState = function() {
  this.statefulChain.addState();
  this.nextOperation.addState();
}

var FunctionExpressionOperation = function(arg) {
  var exp = arg;
  if (typeof arg === "function") {
    exp = new FunctionExpression(arg);
  }
  ExpressionOperation.call(this, exp);
}
FunctionExpressionOperation.prototype = Object.create(ExpressionOperation.prototype);
FunctionExpressionOperation.prototype.constructor = FunctionExpressionOperation;

var GenerateRowOperation = function() {}

GenerateRowOperation.prototype = Object.create(Operation.prototype);
GenerateRowOperation.prototype.constructor = GenerateRowOperation;

GenerateRowOperation.prototype.generateProperty = function(column, rowIndex) {
  var getter = function() { return column[rowIndex.value]; };
  return { get: getter };
}

GenerateRowOperation.prototype.generatePropertyMap = function(columnMap, rowIndex) {
  var propertyMap = new Map();
  var column, columnName, prop;
  for (var keyvalue of columnMap) {
    columnName = keyvalue[0];
    column = keyvalue[1];
    prop = this.generateProperty(column, rowIndex);
    propertyMap.set(columnName, prop);
  }
  return propertyMap;
}

GenerateRowOperation.prototype.setup = function(columnMap) {
  this.rowIndex = new Index();
  this.grouping = new Grouping(null);
  this.groupIndex = this.grouping.index;
  var propertyMap = this.generatePropertyMap(columnMap, this.rowIndex);
  this.row = new Row(propertyMap, this.rowIndex, this.grouping);
  this.nextOperation.setup(this.row);
}

GenerateRowOperation.prototype.step = function(index) {
  this.rowIndex.value = index;
  this.groupIndex.value = 0;
  this.nextOperation.step();
}

var FilterOperation = function(arg) {
  FunctionExpressionOperation.call(this, arg);
}
FilterOperation.prototype = Object.create(ExpressionOperation.prototype);
FilterOperation.prototype.constructor = FilterOperation;

FilterOperation.prototype.setup = function(row) {
  this.row = row;
  this.nextOperation.setup(row);
}

FilterOperation.prototype.step = function() {
  if (this.expression.evaluate(this.row)) {
    this.nextOperation.step();
  }
}

var MutateOperation = function(name, arg) {
  FunctionExpressionOperation.call(this, arg);
  this.name = name;
}
MutateOperation.prototype = Object.create(ExpressionOperation.prototype);
MutateOperation.prototype.constructor = MutateOperation;

MutateOperation.prototype.generateExpressionProperty = function(expression, row) {
  var getter = function() { return expression.evaluate(row); };
  return { get: getter };
}

MutateOperation.prototype.setup = function(row) {
  var propertyMap = new Map(row.propertyMap);
  var rowIndex = row.rowIndex;
  var prop = this.generateExpressionProperty(this.expression, row);
  propertyMap.set(this.name, prop);
  this.row = new Row(propertyMap, rowIndex, row.grouping);
  this.nextOperation.setup(this.row);
}

MutateOperation.prototype.execute = function(row) {
  return row;
}

var SummarizeOperation = function(name, arg) {
  var exp = arg;
  if (typeof arg === "function") {
    exp = new SummaryFunctionExpression(arg);
  }
  ExpressionOperation.call(this, exp);
  this.name = name;
}
SummarizeOperation.prototype = Object.create(ExpressionOperation.prototype);
SummarizeOperation.prototype.constructor = SummarizeOperation;

SummarizeOperation.prototype.generateKeyProperty = function(grouping) {
  var groupIndex = grouping.index;
  var keys = grouping.keys;
  var getter = function() { return keys[groupIndex.value]; };
  return { get: getter };
}

SummarizeOperation.prototype.generateSummaryProperty = function(expression, grouping) {
  var groupIndex = grouping.index;
  var getter = function() { return expression.summarize(groupIndex.value); };
  return { get: getter };
}

SummarizeOperation.prototype.generatePropertyMap = function(grouping) {
  var propertyMap = new Map();
  var groupName = grouping.groupName;
  if (groupName !== null) {
    var keyProp = this.generateKeyProperty(grouping);
    propertyMap.set(groupName, keyProp);
  }
  var summaryProp = this.generateSummaryProperty(this.expression, grouping);
  propertyMap.set(this.name, summaryProp);
  return propertyMap;
}

SummarizeOperation.prototype.setup = function(row) {
  var grouping = row.grouping;
  var parentGrouping = grouping.parentGrouping || grouping;
  var propertyMap = this.generatePropertyMap(grouping);
  var newRowIndex = grouping.index;
  this.grouping = grouping;
  this.oldRow = row;
  this.nextRow = new Row(propertyMap, newRowIndex, parentGrouping);
  this.nextOperation.setup(this.nextRow);
}

SummarizeOperation.prototype.addState = function() {
  this.statefulChain.addState();
  return;
}

SummarizeOperation.prototype.step = function() {
  this.expression.accumulate(this.oldRow);
}

SummarizeOperation.prototype.complete = function() {
  var grouping = this.grouping;
  // set up subsequent states
  var parentGrouping = grouping.parentGrouping;
  if (parentGrouping !== null && parentGrouping.parentGrouping !== null) {
    for (var i = 0; i < numberOfParentGroups; i++) {
      this.nextOperation.addState();
    }      
  } else {
    this.nextOperation.addState();
  }
  var groupIndex = grouping.index;
  var numberOfGroups = grouping.keys ? grouping.keys.length : 1;
  for (var i = 0; i < numberOfGroups; i++) {
    groupIndex.value = i;
    this.nextOperation.step();  
  }
  return this.nextOperation.complete();
}

var GroupByOperation = function(name, arg) {
  FunctionExpressionOperation.call(this, arg);
  this.name = name;
  this.groupMappings = [];
}
GroupByOperation.prototype = Object.create(ExpressionOperation.prototype);
GroupByOperation.prototype.constructor = GroupByOperation;

GroupByOperation.prototype.setup = function(row) {
  this.oldRow = row;
  this.grouping = new Grouping(row.grouping, this.name);
  this.nextRow = new Row(row.propertyMap, row.rowIndex, this.grouping);
  this.nextOperation.setup(this.nextRow);
}

GroupByOperation.prototype.addState = function() {
  this.statefulChain.addState();
  this.groupMappings.push(new Map());
}

GroupByOperation.prototype.execute = function() {
  var grouping = this.grouping;
  var key = this.expression.evaluate(this.oldRow);
  var parentGroupIndexValue = grouping.parentGrouping.index.value;
  var groupMap = this.groupMappings[parentGroupIndexValue];
  var newGroupIndexValue = groupMap.get(key);
  if (newGroupIndexValue === undefined) {
    newGroupIndexValue = grouping.keys.length;
    groupMap.set(key, newGroupIndexValue);
    grouping.parents.push(parentGroupIndexValue);
    grouping.keys.push(key);
    this.nextOperation.addState();
  }
  grouping.index.value = newGroupIndexValue;
}

var Result = function(columnNames, columns) {
  this.columnNames = columnNames;
  this.columns = columns;
}

var NewDataFrameOperation = function() {}

NewDataFrameOperation.prototype = Object.create(Operation.prototype);
NewDataFrameOperation.prototype.constructor = NewDataFrameOperation;

NewDataFrameOperation.prototype.setup = function(row) {
  this.columnNames = Array.from(row.propertyMap.keys());
  this.numberOfColumns = this.columnNames.length;
  this.getters = Array.from(row.propertyMap.values()).map(function(prop) { return prop.get; });;
  this.columns = this.columnNames.map(function() { return []; });
  return;
}

NewDataFrameOperation.prototype.addState = function() {
  return;
}

NewDataFrameOperation.prototype.step = function() {
  var getter;
  for (var colIndex = 0; colIndex < this.numberOfColumns; colIndex++) {
    getter = this.getters[colIndex];
    this.columns[colIndex].push(getter()); 
  }
}

NewDataFrameOperation.prototype.complete = function() {
  return new Result(this.columnNames, this.columns);
}




var Operations = {};
Operations.FilterOperation = FilterOperation;
Operations.MutateOperation = MutateOperation;
Operations.GroupByOperation = GroupByOperation;
Operations.SummarizeOperation = SummarizeOperation;
Operations.NewDataFrameOperation = NewDataFrameOperation;
Operations.GenerateRowOperation = GenerateRowOperation;

module.exports = Operations;