var Operation = function(subscriber) {
  this.subscriber = subscriber;
}

var IterateOperation = function(subscriber, start, end) {
  this.start = start;
  this.end = end;
  this.subscriber = subscriber;
}
IterateOperation.prototype = Object.create(Operation.prototype);
IterateOperation.prototype.constructor = IterateOperation;

IterateOperation.prototype.onNext = function(index) {
  var end = this.end;
  var subscriber = this.subscriber;
  for (var index = this.start; index < end; index++) {
    subscriber.onNext(index);
  }
  return subscriber.onCompleted();
}

var TerminalOperation = function() {}
TerminalOperation.prototype = Object.create(Operation.prototype);
TerminalOperation.prototype.constructor = TerminalOperation;

TerminalOperation.prototype.onNext = function(index) {}

TerminalOperation.prototype.onCompleted = function() {}

var TapOperation = function(subscriber, expression, func) {
  this.subscriber = subscriber;
  this.expression = expression;
  this.func = func;
}
TapOperation.prototype = Object.create(Operation.prototype);
TapOperation.prototype.constructor = TapOperation;

TapOperation.prototype.onNext = function(index) {
  this.func(this.expression, index);
  this.subscriber.onNext(index);
}

TapOperation.prototype.onCompleted = function() {
  return this.subscriber.onCompleted();
}

var AccumulateOperation = function(subscriber, expression, start, end) {
  this.start = start;
  this.end = end;
  this.expression = expression;
  this.indices = [];
}
AccumulateOperation.prototype = Object.create(Operation.prototype);
AccumulateOperation.prototype.constructor = AccumulateOperation;

AccumulateOperation.prototype.onNext = function(index) {
  this.indices.append(index);
  return this.expression.accumulate(index);
}

AccumulateOperation.prototype.onCompleted = function() {
  var rowIndex = this.rowIndex;
  var indices = this.indices;
  var expression = this.expression;
  while (!expression.accumulated) {
    for (var i = 0; i < indices.length; i++) {
      expression.accumulate(indices[i]);
    }
  }
  for (var i = 0; i < indices.length; i++) {
    this.subscriber.onNext(indices[i]);
  }
}


var FilterOperation = function(subscriber, expression) {
  this.subscriber = subscriber;
  this.expression = expression;
}
FilterOperation.prototype = Object.create(Operation.prototype);
FilterOperation.prototype.constructor = FilterOperation;

FilterOperation.prototype.onNext = function(index) {
  if (this.expression.value(index)) {
    return this.subscriber.onNext(index);
  } else {
    return;
  }
}

var Operations = {};
Operations.IterateOperation = IterateOperation;
Operations.TapOperation = TapOperation;
Operations.TerminalOperation = TerminalOperation;

module.exports = Operations;
