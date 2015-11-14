var Operation = function(schema) {}

Operation.prototype.onNext = function(val) {
  return this.subscriber.onNext(val);
}

Operation.prototype.onCompleted = function() {
  return this.subscriber.onCompleted();
}

Operation.prototype.onError = function(err) {
  throw err;
}

Operation.prototype.addChain = function(subsequentChain) {
  var subscriber = subsequentChain();
  this.subscribe(subscriber);
}

Operation.prototype.subscribe = function(observer) {
  this.subscriber = observer;
}

var DataFrameBuilder = function(schema) {
  this.schema = schema;
  this.rows = [];
}
DataFrameBuilder.prototype = Object.create(Operation.prototype);
DataFrameBuilder.prototype.constructor = DataFrameBuilder;

DataFrameBuilder.prototype.onNext = function(val) {
  this.rows.push(val);
  return true;
}

DataFrameBuilder.prototype.onCompleted = function() {
  return this.schema.createDataFrame(this.rows);
}

var Filter = function(schema, f) {
  this.f = f;
  this.row = new schema.RowObject();
}

Filter.prototype = Object.create(Operation.prototype);
Filter.prototype.constructor = Filter;

Filter.prototype.onNext = function(val) {
  var row = this.row;
  return this.f(row) ? this.subscriber.onNext(val) : true;
}

var Distinct = function(schema, f) {
  this.f = (f.length === 0) ? f : this.keyForColumns;
  this.objectMap = {};
  this.row = new schema.RowObject();
}
Distinct.prototype = Object.create(Operation.prototype);
Distinct.prototype.constructor = Distinct;

Distinct.prototype.onNext = function(val) {
  this.row.data = val;
  var rowKey = this.f(this.row);
  if (this.objectMap.hasOwnKey(rowKey)) {
    return true;
  } else {
    this.objectMap[rowKey] = true;
    return this.subscriber.onNext(val);
  }
}

Distinct.prototype.keyForColumns = function() {
  var rowKey = "";
  for (var i = 0; i < this.distinctColumns.length; i++) {
    colIndex = this.distinctColumnIndices[i];
    rowKey += rowArray[colIndex] + "|";
  }
  return rowKey;
}

var Mutate = function(schema, f) {
  this.f = f;
  this.row = new schema.RowObject();
}
Mutate.prototype = Object.create(Operation.prototype);
Mutate.prototype.constructor = Mutate;

Mutate.prototype.onNext = function(val) {
  var row = this.row
  row.data = val;
  var newValue = this.f(row);
  val.push(newValue);
  return this.subscriber.onNext(val);
}

var Reducer = function(schema, accumulatorChain) {
  this.accumulator = accumulatorChain();
  this.count = 0;
}
Reducer.prototype = Object.create(Operation.prototype);
Reducer.prototype.constructor = Reducer;

Reducer.prototype.onNext = function(val) {
  return this.accumulator.onNext(val);
}

Reducer.prototype.onCompleted = function() {
  var result = this.accumulator.onCompleted();
  this.subscriber.onNext(result);
  return this.subscriber.onCompleted();
}

var RowTransformer = function(schema) {
  this.schema = schema;
}
RowTransformer.prototype = Object.create(Operation.prototype);
RowTransformer.prototype.constructor = RowTransformer;

RowTransformer.prototype.onNext = function(val) {
  var columnNames = this.schema.getColumnNames();
  var columnName;
  var rowArray = [];
  for (var i = 0; i < columnNames.length; i++) {
    columnName = columnNames[i];
    rowArray.push(val[columnName]);
  }
  return this.subscriber.onNext(rowArray);
}

RowTransformer.prototype.onCompleted = function() {
  return this.subscriber.onCompleted();
}

var Accumulator = function(schema, key, acc, setup, cleanup) {
  this.key = key;
  this.acc = acc;
  this.result = (typeof(setup) !== "undefined") ? setup() : null;
  this.row = new schema.RowObject();
  if (cleanup) this.cleanup = cleanup;
}
Accumulator.prototype = Object.create(Operation.prototype);
Accumulator.prototype.constructor = Accumulator;

Accumulator.prototype.onNext = function(val) {
  var row = this.row;
  row.data = val;
  this.result = this.acc(this.result, row);
  return this.subscriber.onNext(val);
}

Accumulator.prototype.onCompleted = function() {
  var result = this.subscriber.onCompleted();
  if (this.hasOwnProperty("cleanup")) {
    this.result = this.cleanup(this.result);
  }
  result[this.key] = this.result;
  return result;
}

var AccumulatorEnd = function(schema) {}
AccumulatorEnd.prototype = Object.create(Operation.prototype);
AccumulatorEnd.prototype.constructor = AccumulatorEnd;

AccumulatorEnd.prototype.onNext = function(val) { return true; }

AccumulatorEnd.prototype.onCompleted = function() {
  return {}
}

GroupBy = function(schema, keyFunction, operations) {
  this.keyFunction = keyFunction;
  this.groups = {};
  var groupEnd = new GroupEnd();
  var final = function() { return groupEnd; }
  this.groupEnd = groupEnd;
  this.groupedChain = addOperations(final, operations);
  this.row = new schema.RowObject();
}

GroupBy.prototype = Object.create(Operation.prototype);
GroupBy.prototype.constructor = GroupBy;

GroupBy.prototype.onNext = function(val) {
  var row = this.row;
  row.data = val;
  var key = this.keyFunction(row);
  var group;
  if (this.groups.hasOwnProperty(key)) {
    group = this.groups[key];
  } else {
    group = this.groupedChain();
    this.groups[key] = group;
  }
  return group.onNext(val);
}

GroupBy.prototype.onCompleted = function() {
  var groupResults = {};
  for (var key in this.groups) {
    var group = this.groups[key];
    group.onCompleted();
  }
  return this.subscriber.onCompleted();
}

GroupBy.prototype.subscribe = function(observer) {
  this.subscriber = observer;
  this.groupEnd.subscribe(observer);
}

GroupEnd = function(schema) {}
GroupEnd.prototype = Object.create(Operation.prototype);
GroupEnd.prototype.constructor = GroupEnd;

GroupEnd.prototype.onCompleted = function() { return }

var addOperation = function(subsequentChain, operation) {
  return function() {
    var op = new operation;
    op.addChain(subsequentChain);
    return op;
  }
}

var addOperations = function(subsequentChain, operations) {
  var chain = subsequentChain;
  for (var i = operations.length - 1; i >= 0; i--) {
    var operation = operations[i]
    chain = addOperation(chain, operation);
  }
  return chain;
}


var Operations = {};
Operations.Operation = Operation;
Operations.DataFrameBuilder = DataFrameBuilder;
Operations.Reducer = Reducer;
Operations.RowTransformer = RowTransformer;
Operations.Accumulator = Accumulator;
Operations.AccumulatorEnd = AccumulatorEnd;
Operations.Distinct = Distinct;
Operations.Mutate = Mutate;
Operations.Filter = Filter;
Operations.GroupBy = GroupBy;
Operations.GroupEnd = GroupEnd;
Operations.addOperations = addOperations;
Operations.addOperation = addOperation;

module.exports = Operations;
