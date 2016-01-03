var Expressions = require("./expressions.js");
var FunctionExpression = Expressions.FunctionExpression;
var nullExp = Expressions.nullExp;


var Row = function(values, groupIndex) {
  this.values = values;
  this.groupIndex = groupIndex;
}

var Group = function(groupIndex, groupName, groupKey) {
  this.groupIndex = groupIndex;
	this.groupName = groupName;
	this.groupKey = groupKey;
}

var Result = function(group) {
	this.groups = [group];
}

Result.prototype.pushGroup = function(group) {
	this.groups.push(group);
}

Result.prototype.popGroup = function() {
	return this.groups.pop();
}

Result.prototype.topGroup = function() {
  return this.groups[this.groups.length - 1];
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

var GenerateRowOperation = function(data) {
  this.data = data;
}
GenerateRowOperation.prototype = Object.create(Operation.prototype);
GenerateRowOperation.prototype.constructor = GenerateRowOperation;

GenerateRowOperation.prototype.step = function(index) {
  var row = new Row(this.data[index], 0);
  this.nextOperation.step(row);
}

GenerateRowOperation.prototype.complete = function() {
  var group = new Group(0);
  var result = new Result(group);
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
  row.values[this.name] = value;
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
  values[this.name] = this.expression.summarize(summarizedGroup);
  var row = new Row(values, topGroup.groupIndex);
  this.nextOperation.step(row);
}

SummarizeOperation.prototype.complete = function(result) {
  this.nextOperation.summarize(result);
  this.nextOperation.complete();
}

var GroupByOperation = function(name, exp) {
  ExpressionOperation.call(this, exp);
  this.groupName = name;
  this.groupMappings = [];
  this.numberOfGroups = 0;
  this.parents = [];
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
  var value = this.expression.evaluate(row);
  var groupMap = this.groupMappings[row.groupIndex];
  var newGroupIndex = groupMap.get(value);
  if (newGroupIndex === undefined) {
    groupMap.set(value, this.numberOfGroups);
    newGroupIndex = this.numberOfGroups;
    this.parents.push(row.groupIndex);
    this.numberOfGroups++;
    this.nextOperation.addState();
  }
  row.groupIndex = newGroupIndex;
  return row;
}

GroupByOperation.prototype.complete = function(result) {
  var topGroup = result.topGroup();
  var groupMap = this.groupMappings[topGroup.groupIndex];
  var groupKeys = groupMap.keys();
  for (var key of groupKeys) {
    var groupIndex = groupMap.get(key);
    var groupName = this.groupName;
    var groupKey = key;
    var group = new Group(groupIndex, groupName, groupKey);
    result.pushGroup(group);
    this.nextOperation.summarize(result);
  }
  this.nextOperation.complete();
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