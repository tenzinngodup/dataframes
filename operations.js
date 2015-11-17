var RowIndex = function(start, end) {
  this.start = start;
  this.value = start;
  this.end = end;
}

var Operation = function(subscriber) {
  this.subscriber = subscriber;
}

var IterateOperation = function(subscriber, rowIndex) {
  this.rowIndex = rowIndex;
  this.subscriber = subscriber;
}
IterateOperation.prototype = Object.create(Operation.prototype);
IterateOperation.prototype.constructor = IterateOperation;

IterateOperation.prototype.onNext = function() {
  var rowIndex = this.rowIndex;
  var end = rowIndex.end;
  var subscriber = this.subscriber;
  for (rowIndex.value = rowIndex.start; rowIndex.value < end; rowIndex.value++) {
    subscriber.onNext();
  }
  return subscriber.onCompleted();
}

var TerminalOperation = function() {}
TerminalOperation.prototype = Object.create(Operation.prototype);
TerminalOperation.prototype.constructor = TerminalOperation;

TerminalOperation.prototype.onNext = function() {}

TerminalOperation.prototype.onCompleted = function() {}

var TapOperation = function(subscriber, expression, func) {
  this.subscriber = subscriber;
  this.expression = expression;
  this.func = func;
}
TapOperation.prototype = Object.create(Operation.prototype);
TapOperation.prototype.constructor = TapOperation;

TapOperation.prototype.onNext = function() {
  this.func(this.expression);
  this.subscriber.onNext();
}

TapOperation.prototype.onCompleted = function() {
  return this.subscriber.onCompleted();
}

var AccumulateOperation = function(subscriber, rowIndex, expression) {
  this.start = start;
  this.end = end
  this.expression = expression;
  this.rowIndex = rowIndex;
  this.indices = [];
}
AccumulateOperation.prototype = Object.create(Operation.prototype);
AccumulateOperation.prototype.constructor = AccumulateOperation;

AccumulateOperation.prototype.onNext = function() {
  this.indices.append(this.rowIndex.value);
  return this.expression.accumulate();
}

AccumulateOperation.prototype.onCompleted = function() {
  var rowIndex = this.rowIndex;
  var indices = this.indices;
  var expression = this.expression;
  while (!expression.accumulated) {
    for (var i = 0; i < indices.length; i++) {
      rowIndex.value = indices[i];
      expression.accumulate();
    }
  }
  for (var i = 0; i < indices.length; i++) {
    rowIndex.value = indices[i];
    this.subscriber.onNext();
  }
}


var FilterOperation = function(subscriber, expression) {
  this.subscriber = subscriber;
  this.expression = expression;
}
FilterOperation.prototype = Object.create(Operation.prototype);
FilterOperation.prototype.constructor = FilterOperation;

FilterOperation.prototype.onNext = function() {
  if (this.expression.value()) {
    return this.subscriber.onNext();
  } else {
    return;
  }
}

var Operations = {};
Operations.RowIndex = RowIndex;
Operations.IterateOperation = IterateOperation;
Operations.TapOperation = TapOperation;
Operations.TerminalOperation = TerminalOperation;

module.exports = Operations;
