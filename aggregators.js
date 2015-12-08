var SumState = function() {
  this.total = 0;
}

var SumAggregator = function(expression) {
  this.expression = expression;
}

SumAggregator.prototype.init = function() {
  return new SumState();
}

SumAggregator.prototype.aggregate = function(state, tuple, collector) {
  state.total += this.expression.value(tuple);
}

SumAggregator.prototype.complete = function(state, collector) {
  collector.emit(state.total);
}

var Agreggators = {};
Aggregators.SumAggregator = SumAggregator;

module.exports = Aggregators;