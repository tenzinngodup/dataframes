var ExpressionManager = function() {
  // global that manages function expressions
  this.builder = new ExpressionBuilder();
  this.evaluator = new ExpressionEvaluator();
  this.summarizer = new ExpressionSummarizer();
  this.default = new ExpressionDefault();
  this.handler = this.default;
}

ExpressionManager.prototype.evaluate = function(expression, row) {
  this.handler = this.evaluator;
  var result = this.handler.evaluate(expression, row);
  this.handler = this.default;
  return result;
}

ExpressionManager.prototype.summarize = function(expression, result) {
  this.handler = this.summarizer;
  var result = this.handler.summarize(expression, result);
  this.handler = this.default;
  return result;
}

ExpressionManager.prototype.build = function(expression) {
  this.handler = this.builder;
  var result = this.handler.build(expression);
  this.handler = this.default;
  return result;
}

ExpressionManager.prototype.get = function(expressionConstructor, argument) {
  return this.handler.get(expressionConstructor, argument);
}

var ExpressionBuilder = function() {
  this._lastExpression = null;
}

ExpressionBuilder.prototype.build = function(expression) {
  this._lastExpression = expression;
  expression.func(new Stub(["first_name", "pres_number", "party"]));
}

ExpressionBuilder.prototype.get = function(expressionConstructor, arg) {
  var newExpression = new expressionConstructor();
  this._lastExpression.nextExpression = newExpression;
  this._lastExpression = newExpression;
  console.log(newExpression);
  return newExpression.stubValue();
}

var ExpressionEvaluator = function() {
    this._nextExpression = null;
}

ExpressionEvaluator.prototype.evaluate = function(expression, row) {
  // evaluate a FunctionExpression
  this._nextExpression = expression.nextExpression;
  this._groupIndex = row.groupIndex;
  return expression.func(row.values);
}

ExpressionEvaluator.prototype.get = function(expressionConstructor, arg) {
  var val = this._nextExpression.evaluate(arg, this._groupIndex);
  this._nextExpression = this._nextExpression.nextExpression;
  return val;
}

var ExpressionSummarizer = function() {
    this._nextExpression = null;
}

ExpressionSummarizer.prototype.summarize = function(expression, result) {
  // complete a SummaryFunctionExpression
  this._nextExpression = expression.nextExpression;
  this._groupIndex = result.groupIndex;
  return expression.func(result);
}

ExpressionSummarizer.prototype.get = function(expressionConstructor, arg) {
  var val = this._nextExpression.finalValue(arg, this._groupIndex);
  this._nextExpression = this._nextExpression.nextExpression;
  return val;
}

var ExpressionDefault = function() {}

ExpressionDefault.prototype.get = function(expressionConstructor, argument) {
  throw "ExpressionDefault should not have been called"
}

var NullExpression = function() {}

NullExpression.prototype.getStatefulChain = function() { 
  return this;
}

NullExpression.prototype.addState = function() { 
  return;
}

var nullExp = new NullExpression();

var Expression = function() {
  this.nextExpression = nullExp;
}

Expression.prototype.stubValue = function() {
  return 0;
}

Expression.prototype.value = function() {
  return NaN;
}

Expression.prototype.finalValue = function() {
  return NaN;
}

Expression.prototype.evaluate = function(value) {
  return value;
}

Expression.prototype.getStatefulChain = function(chain) {
  return this.nextExpression.getStatefulChain();
}

Expression.prototype.finalValue = function(arg, groupIndex) {
  return NaN;
}

var StatefulExpression = function() {
  Expression.call(this);
  this.states = [];
}
StatefulExpression.prototype = Object.create(Expression.prototype);
StatefulExpression.prototype.constructor = AccumulatorExpression;

StatefulExpression.prototype.init = function() {
  return 0;
}

StatefulExpression.prototype.accumulate = function(value, state) {
  return state;
}

StatefulExpression.prototype.value = function(state) {
  return state;
}

StatefulExpression.prototype.addState = function() {
  this.states.push(this.init());
  this.nextStatefulExpression.addState();
}

StatefulExpression.prototype.getStatefulChain = function(chain) {
  this.nextStatefulExpression = this.nextExpression.getStatefulChain();
  return this;
}

StatefulExpression.prototype.evaluate = function(value, groupIndex) {
  var newState = this.states[groupIndex] = this.accumulate(value, this.states[groupIndex]);
  return this.value(newState);
}

StatefulExpression.prototype.finalValue = function(arg, groupIndex) {
  return this.value(this.states[groupIndex])
}

var AccumulatorExpression = function() {
  StatefulExpression.call(this);
}
AccumulatorExpression.prototype = Object.create(StatefulExpression.prototype);
AccumulatorExpression.prototype.constructor = AccumulatorExpression;

var SummaryExpression = function() {
  StatefulExpression.call(this);
}
SummaryExpression.prototype = Object.create(StatefulExpression.prototype);
SummaryExpression.prototype.constructor = SummaryExpression;

var SumExpression = function() {
  SummaryExpression.call(this);
}
SumExpression.prototype = Object.create(SummaryExpression.prototype);
SumExpression.prototype.constructor = SumExpression;

SumExpression.prototype.accumulate = function(value, state) {
  return value + state;
}

var CumulativeSumExpression = function() {
  AccumulatorExpression.call(this);
}
CumulativeSumExpression.prototype = Object.create(AccumulatorExpression.prototype);
CumulativeSumExpression.prototype.constructor = CumulativeSumExpression;

CumulativeSumExpression.prototype.accumulate = function(value, state) {
  return value + state;
}

NullExpression.build = function() { return; }

var Stub = function(columnNames) {
  for (var i = 0; i < columnNames.length; i++) {
    this[columnNames[i]] = 0;
  }
}

var expManager = new ExpressionManager();

var FunctionExpression = function(func) {
  this.func = func;
  this.nextExpression = nullExp;
  expManager.build(this);
}
FunctionExpression.prototype = Object.create(Expression.prototype);
FunctionExpression.prototype.constructor = FunctionExpression;

FunctionExpression.prototype.evaluate = function(row) {
  return expManager.evaluate(this, row);
}

var SummaryFunctionExpression = function(func) {
  this.func = func;
  this.nextExpression = nullExp;
  expManager.build(this);
}
SummaryFunctionExpression.prototype = Object.create(FunctionExpression.prototype);
SummaryFunctionExpression.prototype.constructor = SummaryFunctionExpression;

SummaryFunctionExpression.prototype.accumulate = function(row) {
  expManager.evaluate(this, row);
}

SummaryFunctionExpression.prototype.summarize = function(result) {
  return expManager.summarize(this, result);
}

var SquareExpression = function(arg) {
  Expression.call(this, arg);
}
SquareExpression.prototype = Object.create(Expression.prototype);
SquareExpression.prototype.constructor = SquareExpression;

SquareExpression.prototype.evaluate = function(arg) {
  return arg * arg;
}

var createFunction = function(expressionConstructor) {
  return function(arg) {
    return expManager.get(expressionConstructor, arg);
  }
}

var sum = createFunction(SumExpression);
var cumsum = createFunction(CumulativeSumExpression);
var square = createFunction(SquareExpression);

var Expressions = {};

Expressions.Expression = Expression;
Expressions.FunctionExpression = FunctionExpression;
Expressions.SummaryFunctionExpression = SummaryFunctionExpression;
Expressions.createFunction = createFunction;
Expressions.nullExp = nullExp;
Expressions.sum = sum;
Expressions.cumsum = cumsum;
Expressions.square = square;

module.exports = Expressions;


