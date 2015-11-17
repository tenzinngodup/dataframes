var ExpressionManager = function() {
  this.builder = new ExpressionBuilder(this);
  this.evaluator = new ExpressionEvaluator(this);
  this.creator = new ExpressionCreator(this);
  this.handler = this.creator;
}

ExpressionManager.prototype.evaluate = function(expression, func) {
  this.handler.evaluate(expression, func);
}

ExpressionManager.prototype.build = function(expression, argument) {
  return this.handler.build(expression, argument);
}

ExpressionManager.prototype.create = function(expression, argument) {
  return this.handler.create(expression, argument);
}

ExpressionManager.prototype.get = function(expression, argument) {
  return this.handler.get(expression, argument);
}

var ExpressionHandler = function() {}

ExpressionHandler.prototype.build = function(expression, arg) {
  // if this is called, switch to the builder
  var oldHandler = this.manager.handler;
  this.manager.handler = this.manager.builder;
  this.manager.builder.build(expression, func);
  this.manager.handler = oldHandler;
}

ExpressionHandler.prototype.evaluate = function(expression, arg) {
  // if this is called, switch to the evaluator
  var oldHandler = this.manager.handler;
  this.manager.handler = this.manager.evaluator;
  this.manager.evaluator.evaluate(expression, arg);
  this.manager.handler = oldHandler;
}

var ExpressionBuilder = function(manager) {
  this.manager = manager;
  this._expression = null;
}
ExpressionBuilder.prototype = Object.create(ExpressionHandler.prototype);
ExpressionBuilder.prototype.constructor = ExpressionBuilder;

ExpressionBuilder.prototype.build = function(expression, func) {
  var parentExpression = this._expression;
  this._expression = expression;
  var result = func();
  if (typeof result === "object" && result instanceof Expression) {
    this._expression.addExpression(result);
  }
  this._expression = parentExpression;
  return (parentExpression) ? expression.stubValue() : expression;
}

ExpressionBuilder.prototype.get = function(expression, argument) {
  var expression = new Expression(argument);
  this._expression.addExpression(expression);
  return expression.stubValue();
}

ExpressionEvaluator = function(manager) {
  this.manager = manager;
  this._expression = null;
}
ExpressionEvaluator.prototype = Object.create(ExpressionHandler.prototype);
ExpressionEvaluator.prototype.constructor = ExpressionEvaluator;

ExpressionEvaluator.prototype.evaluate = function(expression, func) {
  // if this is called, switch to the evaluator
  var oldHandler = this.manager.handler;
  this.manager.handler = this.manager.evaluator;
  this.manager.evaluator.evaluate(expression, func);
  this.manager.handler = oldHandler;
}

ExpressionEvaluator.prototype.build = function(expression, func) {
  // if this is called, switch to the builder
  var oldHandler = this.manager.handler;
  this.manager.handler = this.manager.builder;
  this.manager.builder.build(expression, func);
  this.manager.handler = oldHandler;
}

ExpressionEvaluator.prototype.evaluate = function(expression, func) {
  var oldGetter = this._getter;
  this._getter = new ExpressionValueGetter(expression);
  var value = func();
  this._getter = oldGetter;
  return value;
}

var ExpressionCreator = function(manager) {
  this.manager = manager;
}
ExpressionCreator.prototype = Object.create(ExpressionHandler.prototype);
ExpressionCreator.prototype.constructor = ExpressionCreator;


ExpressionCreator.prototype.get = function(expression, arg) {
  return new expression(arg);
}


ExpressionValueGetter = function(expression) {
  this.opIndex = 0;
  this.subExpressions = expression.subExpressions;
};

ExpressionValueGetter.prototype.valueOf = function() {
  return this.subExpressions[this.opIndex].value();
};

ExpressionEvaluator.prototype.get = function(expression, argument) {
  return this._expression.getSubExpression;
}

var Expression = function(arg) {
  this.accumulated = true;
  if (typeof arg === "function") {
    this.subExpression = new FunctionExpression(arg);
  } else if (typeof arg === "object") {
    if (arg instanceof Expression) {
      this.subExpression = arg;
    }
  }
}

Expression.prototype.valueOf = function() {
  return expManager.get(this);
}

Expression.prototype.build = function() {
}

Expression.prototype.stubValue = function() {
  return 0;
}

Expression.prototype.value = function() {
  return this.subExpression.value();
}

Expression.prototype.finishAccumulating = function() {
  this.accumulated = true;
}

var FunctionExpression = function(func) {
  this.func = func;
  this.subExpressions = [];
}

FunctionExpression.prototype.addExpression = function(expression) {
  this.subExpressions.push(expression);
}

FunctionExpression.prototype.value = function() {
  return expManager.evaluate(this, this.func);
}

var SquareExpression = function(arg) {
  Expression.call(this, arg);
}

SquareExpression.prototype.value = function() {
  var exp = this.subExpression;
  return exp.value() * exp.value();
}

var ColumnExpression = function(arg) {
  Expression.call(this, arg);
}
ColumnExpression.prototype = Object.create(Expression.prototype);
ColumnExpression.prototype.constructor = ColumnExpression;

var JSONColumnExpression = function(rowIndex, data, name) {
  this.rowIndex = rowIndex;
  this.data = data;
  this.name = name;
}
JSONColumnExpression.prototype = Object.create(ColumnExpression.prototype);
JSONColumnExpression.prototype.constructor = JSONColumnExpression;

JSONColumnExpression.prototype.value = function() {
  return this.data[this.rowIndex.value][this.name];
}

var ArrayColumnExpression = function(rowIndex, expression) {
  this.rowIndex = rowIndex;
  this.expression = expression;
  this.data = [];
}
ArrayColumnExpression.prototype = Object.create(ColumnExpression.prototype);
ArrayColumnExpression.prototype.constructor = ArrayColumnExpression;

ArrayColumnExpression.accumulate = function() {
  this.data.push(this.expression.value());
}

ArrayColumnExpression.value = function() {
  return this.data[this.rowIndex.value];
}

var expManager = new ExpressionManager();

console.log(expManager.handler)

var Expressions = {};

var square = function(arg) {
  return expManager.get(SquareExpression, arg);
}

Expressions.ArrayColumnExpression = ArrayColumnExpression;
Expressions.JSONColumnExpression = JSONColumnExpression;
Expressions.square = square;

module.exports = Expressions;
