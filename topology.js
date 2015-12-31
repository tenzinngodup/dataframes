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

var Topology = function(data) {
  this.data = data;
  this.operator = new GenerateRowOperation(data);
  this.lastOperation = this.operator;
}

Topology.prototype.addOperation = function(op) {
  this.lastOperation.setNextOperation(op);
  this.lastOperation = op;
}

Topology.prototype.addFilter = function(func) {
  var filterExp = new FunctionExpression(func);
  var filterOp = new FilterOperation(filterExp);
  this.addOperation(filterOp);
}

Topology.prototype.addMutation = function(name, func) {
  var mutateExp = new FunctionExpression(func);
  var mutateOp = new MutateOperation(name, mutateExp);
  this.addOperation(mutateOp);
}

Topology.prototype.addGroupBy = function(name, arg) {
  var func;
  if (arg === undefined) {
    func = new Function("row", "return row." + name);
  } else {
    func = arg;
  }
  var groupByExp = new FunctionExpression(func);
  var groupByOp = new GroupByOperation(name, groupByExp);
  this.addOperation(groupByOp);
}

Topology.prototype.addSummarize = function(name, func) {
  var summarizeExp = new SummaryFunctionExpression(func);
  var summarizeOp = new SummarizeOperation(name, summarizeExp);
  this.addOperation(summarizeOp);
}


Topology.prototype.addTap = function(func) {
  var tapOp = new TapOperation(func);
  this.addOperation(tapOp);
}

Topology.prototype.generate = function() {
  this.lastOperation.setNextOperation(new NullOperation());
  this.operator.addState();
}

Topology.prototype.execute = function() {
  var numberOfRows = this.data.length;
  for (var i = 0; i < numberOfRows; i++) {
    this.executeRow(i);
  }
  this.complete();
}

Topology.prototype.executeRow = function(index) {
  this.operator.step(index);
  console.log("Executed row", index);
}

Topology.prototype.complete = function(index) {
  this.operator.complete();
}

var exports = {};

exports.Topology = Topology;

module.exports = exports;

