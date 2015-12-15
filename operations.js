var Expression = require("./expressions.js").Expression;
var Row = require("./row.js").Row;

var Operation = function(arg, filterLevel) {
  if (arg instanceof Expression) {
    this.expression = arg;    
  } else if (typeof arg === "function") {
    this.expression = new Expression(arg);
  }
  this.filterLevel = filterLevel;
  this.groupLevel = NaN;
  this.next = null;
  this.nextState = null;
  this.stateIndex = -1;
}

Operation.prototype.init = function() {
  return null;
}

Operation.prototype.setNext = function(next) {
  this.next = next;
}

Operation.prototype.step = function(row) {
  var row = this.execute(row);
  this.next.step(row);
}

Operation.prototype.build = function(next) {
  this.next.build();
}

var AccumulatorOperation = function(expression) {
  this.expression = expression;
  this.accumulatorChain = expression.accumulatorChain();
}
AccumulatorOperation.prototype = Object.create(Operation.prototype);
AccumulatorOperation.prototype.constructor = AccumulatorOperation;

AccumulatorOperation.prototype.execute = function(row) {
  this.accumulatorChain.update(row);
  return row;
}

AccumulatorOperation.prototype.build = function() {
  this.accumulatorChain.build();
  this.next.build();
}

var GenerateRowOperation = function(columns, data) {
  this.columns = columns;
  this.data = data;
}
GenerateRowOperation.prototype = Object.create(Operation.prototype);
GenerateRowOperation.prototype.constructor = GenerateRowOperation;

GenerateRowOperation.prototype.step = function(index) {
  var row = new Row(index, this.columns, this.data[index], this.states);
  this.next.step(row);
}

var NullOperation = function() {}
NullOperation.prototype = Object.create(Operation.prototype);
NullOperation.prototype.constructor = NullOperation;

NullOperation.prototype.step = function(row) { return; }

NullOperation.prototype.build = function() { return; }

var BlockOperation = function(filterLevel) {
  this.filterLevel = filterLevel;
}
BlockOperation.prototype = Object.create(Operation.prototype);
BlockOperation.prototype.constructor = BlockOperation;

BlockOperation.prototype.step = function(row) {
  if (row.filtersPassed < this.filterLevel) {
    return;
  } else {
    this.next.step(row);
  }
}

var LogOperation = function(filterLevel) {
  this.filterLevel = filterLevel;
}

LogOperation.prototype = Object.create(Operation.prototype);
LogOperation.prototype.constructor = LogOperation;

LogOperation.prototype.execute = function(row) {
  console.log("Logging row:", row);
}

var FilterOperation = function(arg, filterLevel) {
  Operation.call(this, arg, filterLevel);
}
FilterOperation.prototype = Object.create(Operation.prototype);
FilterOperation.prototype.constructor = FilterOperation;

FilterOperation.prototype.isKeep = function(row) {
  return this.expression.value(row);
}

FilterOperation.prototype.execute = function(row) {
  if (this.isKeep(row)) {
    row.filtersPassed++;
  }
  return row;
}

var MutateOperation = function(arg, filterLevel, name) {
  Operation.call(this, arg, filterLevel);
  this.name = name;
}
MutateOperation.prototype = Object.create(Operation.prototype);
MutateOperation.prototype.constructor = MutateOperation;

MutateOperation.prototype.execute = function(row) {
  row.values[this.name] = this.expression.value(row);
  return row;
}

var GroupByState = function() {
  this.groupMap = new Map();
  this.groupArray = [];
}

var GroupState = function(states) {
  this.states = states;
}

/*

GroupByOperation.prototype.init = function() {
  return new GroupByState();
}

GroupByOperation.prototype.getGroup = function(state, row) {
  var groupMap = state.groupMap;
  var groupArray = state.groupArray;
  var key = this.expression.value(tuple);
  var groupIndex = state.groups.get(key);
  var group;
  if (typeof groupIndex === "undefined") {
    group = new GroupState();
    var groupIndex = groupArray.push(group) - 1;
    groupMap.set(key, groupIndex);
  } else {
    group = groupArray[groupIndex];
  }
  row.groupIndex = groupIndex;
}

*/

var Operations = {};
Operations.FilterOperation = FilterOperation;
Operations.MutateOperation = MutateOperation;
Operations.NullOperation = NullOperation;
Operations.LogOperation = LogOperation;
Operations.BlockOperation = BlockOperation;
Operations.GenerateRowOperation = GenerateRowOperation;
Operations.AccumulatorOperation = AccumulatorOperation;

module.exports = Operations;