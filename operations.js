var Expressions = require("./expressions.js");
var FunctionExpression = Expressions.FunctionExpression;
var nullExp = Expressions.nullExp;

var Row = function(df) {
  var columns = this._columns = df._columns;
  var columnNames = this._columnNames = df._columnNames;
  this._index = 0;
  this._groupIndex = 0;
  var numColumns = df._numColumns;
  for (var colIndex = 0; colIndex < numColumns; colIndex++) {
    // use immediately invoked arrow expression to capture value of colIndex
    var getter = ((c) => { return function() { return columns[c][this._index]; }; })(colIndex)
    Object.defineProperty(this, columnNames[colIndex], {
      get: getter
    });
  }
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

Operation.prototype.summarize = function(row) {
  this.nextOperation.summarize();
}

Operation.prototype.complete = function(result) {
  this.nextOperation.complete(result);
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

var GenerateRowOperation = function(dataframe) {
  var numColumns = dataframe._numColumns;
  var columnNames = dataframe._columnNames;
  this.row = new Row(dataframe);
}

GenerateRowOperation.prototype = Object.create(Operation.prototype);
GenerateRowOperation.prototype.constructor = GenerateRowOperation;

GenerateRowOperation.prototype.step = function(index) {
  this.row._index = index;
  this.nextOperation.step(this.row);
}

GenerateRowOperation.prototype.complete = function() {
  var result = new Result();
  this.nextOperation.complete(result);
}

var NullOperation = function() {}
NullOperation.prototype = Object.create(Operation.prototype);
NullOperation.prototype.constructor = NullOperation;

NullOperation.prototype.step = function(row) { return; }

NullOperation.prototype.addState = function() { return; }

NullOperation.prototype.summarize = function(result) { return; }

NullOperation.prototype.complete = function(result) { return; }

var nullOp = new NullOperation();

var LogOperation = function() {}

LogOperation.prototype = Object.create(Operation.prototype);
LogOperation.prototype.constructor = LogOperation;

LogOperation.prototype.execute = function(row) {
  console.log("Logging row:", row);
}

var TapOperation = function(func) {
  this.func = func;
}

TapOperation.prototype = Object.create(Operation.prototype);
TapOperation.prototype.constructor = TapOperation;

TapOperation.prototype.execute = function(row) {
  this.func(row);
  return row;
}

var FilterOperation = function(exp) {
  ExpressionOperation.call(this, exp);
}
FilterOperation.prototype = Object.create(ExpressionOperation.prototype);
FilterOperation.prototype.constructor = FilterOperation;

FilterOperation.prototype.step = function(row) {
  if (this.expression.evaluate(row)) {
    this.nextOperation.step(row);
  }
}

var MutateOperation = function(name, exp) {
  ExpressionOperation.call(this, exp);
  this.name = name;
}
MutateOperation.prototype = Object.create(ExpressionOperation.prototype);
MutateOperation.prototype.constructor = MutateOperation;

MutateOperation.prototype.execute = function(row) {
  var value = this.expression.evaluate(row);
  row[this.name] = value;
  return row;
}

var SummarizeOperation = function(name, exp) {
  ExpressionOperation.call(this, exp);
  this.name = name;
}
SummarizeOperation.prototype = Object.create(ExpressionOperation.prototype);
SummarizeOperation.prototype.constructor = SummarizeOperation;

SummarizeOperation.prototype.addState = function() {
  this.statefulChain.addState();
  return;
}

SummarizeOperation.prototype.addRegroupedState = function() {
  this.nextOperation.addState();
}

SummarizeOperation.prototype.step = function(row) {
  this.expression.accumulate(row);
}

SummarizeOperation.prototype.summarize = function(result) {
  var values = {}
  var summarizedGroup = result.popGroup();
  var topGroup = result.topGroup();
  if (summarizedGroup.groupName !== undefined) {
    values[summarizedGroup.groupName] = summarizedGroup.groupKey;    
  }
  
  var row = new Row(values, topGroup._groupIndex);
  this.nextOperation.step(row);
}

SummarizeOperation.prototype.complete = function(result) {
  var grouping = result.popGrouping();
  var summaryName = this.name;
  var values, row;
  if (grouping !== undefined) {
    var parents = grouping.parents;
    var keys = grouping.keys;
    var groupName = grouping.groupName;
    var numberOfGroups = parents.length;
    var key, parentGroupIndex, summary;
    for (var i = 0; i < numberOfGroups; i++) {
      key = keys[i];
      parentGroupIndex = parents[i];
      summary = this.expression.summarize(i);
      values = {}
      values[groupName] = key;
      values[summaryName] = summary;
      row = new Row(values, parentGroupIndex);
      this.nextOperation.step(row);
    }
  } else {
    // summary of entire dataframe
    values = {};
    values[this.name] = this.expression.summarize(0);
    row = new Row(values, 0);
    this.nextOperation.step(row);
  }
  this.nextOperation.complete(result);
}

var Grouping = function(groupName) {
	this.groupName = groupName;
	this.keys = [];
  this.parents = [];
}

var GroupByOperation = function(name, exp) {
  ExpressionOperation.call(this, exp);
  this.grouping = new Grouping(name);
  this.groupMappings = [];
  this.numberOfGroups = 0;
  this.summarizer = nullOp;
}
GroupByOperation.prototype = Object.create(ExpressionOperation.prototype);
GroupByOperation.prototype.constructor = GroupByOperation;

GroupByOperation.prototype.addState = function() {
  this.statefulChain.addState();
  this.summarizer.addRegroupedState();
  this.groupMappings.push(new Map());
}

GroupByOperation.prototype.execute = function(row) {
  var key = this.expression.evaluate(row);
  console.log("row", row);
  console.log("mappings", this.groupMappings);
  var groupMap = this.groupMappings[row._groupIndex];
  var newGroupIndex = groupMap.get(key);
  if (newGroupIndex === undefined) {
    groupMap.set(key, this.numberOfGroups);
    newGroupIndex = this.numberOfGroups;
    this.numberOfGroups++;
    this.grouping.parents.push(row._groupIndex);
    this.grouping.keys.push(key);
    this.nextOperation.addState();
  }
  row._groupIndex = newGroupIndex;
  return row;
}

GroupByOperation.prototype.complete = function(result) {
  result.pushGrouping(this.grouping);
  this.nextOperation.complete(result);
}

var Operations = {};
Operations.FilterOperation = FilterOperation;
Operations.MutateOperation = MutateOperation;
Operations.GroupByOperation = GroupByOperation;
Operations.SummarizeOperation = SummarizeOperation;
Operations.NullOperation = NullOperation;
Operations.LogOperation = LogOperation;
Operations.TapOperation = TapOperation;
Operations.GenerateRowOperation = GenerateRowOperation;

module.exports = Operations;