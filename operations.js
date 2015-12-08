var Expression = require("./expressions.js").Expression;

var FilterOperation = function(exp) {
  if (exp instanceof Expression) {
    this.expression = exp;    
  } else if (typeof exp === "function") {
    this.expression = new Expression(exp);
  }
}

FilterOperation.prototype.isKeep = function(row) {
  return this.expression.value(row);
}

FilterOperation.prototype.execute = function(row) {
  console.log(row);
  if (this.isKeep(row)) {
    row.filtersPassed++;
  }
  return row;
}

var MutateOperation = function(expression, name) {
  this.expression = expression;
  this.name = name;
}

MutateOperation.prototype.execute = function(row) {
  row.values[this.name] = this.expression.value(row);
  return row;
}

var GroupByState = function() {
  this.groupMap = new Map();
  this.groupArray = [];
}

var GroupState = function(groupedExpressions) {
  this.states = [];
}

var GroupByOperation = function(expression, parentGroup) {
  this.expression = expression;
  this.parentGroup = parentGroup;
  this.groupedExpressions = [];
}

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

var Operations = {};
Operations.FilterOperation = FilterOperation;
Operations.MutateOperation = MutateOperation;

module.exports = Operations;