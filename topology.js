var Operations = require("./operations.js");

var FilterOperation = Operations.FilterOperation;
var MutateOperation = Operations.MutateOperation;
var NullOperation = Operations.NullOperation;
var LogOperation = Operations.LogOperation;
var BlockOperation = Operations.BlockOperation;
var GenerateRowOperation = Operations.GenerateRowOperation;
var AccumulatorOperation = Operations.AccumulatorOperation;

var Topology = function() {
  this.columns = [];
  this.operations = [];
  this.numberOfFilters = 0;
  this.numberOfGroups = 0;
}

Topology.prototype.addFilter = function(arg) {
  var operation = new FilterOperation(arg, this.numberOfFilters);
  this.addDependencies(operation);
  this.numberOfFilters++;
  this.operations.push(new BlockOperation(this.numberOfFilters));
}

Topology.prototype.addMutation = function(arg, name) {
  var mutation = new MutateOperation(arg, this.numberOfFilters, name);
  var accumulator = new AccumulatorOperation(mutation.expression);
  this.operations.push(mutation);
  this.operations.push(accumulator);
}

Topology.prototype.generate = function(data) {
  // TO FIX
  this.columns = ["pres_number", "first_name", "party"];
  var operations = this.operations;
  operations.push(new LogOperation());
  var numOps = operations.length;
	for (var i = 0; i < numOps - 1; i++) {
    operations[i].setNext(operations[i + 1]);
  }
  operations[numOps - 1].setNext(new NullOperation());
  this.operator = new GenerateRowOperation(this.columns, data);
  this.operator.setNext(operations[0]);
  this.operator.build();
}

Topology.prototype.execute = function(index) {
  this.operator.step(index);
  console.log("Executed row", index);
}

var exports = {};

exports.Topology = Topology;

module.exports = exports;

