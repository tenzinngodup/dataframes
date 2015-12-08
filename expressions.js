var ExpressionManager = function() {
  this.builder = new ExpressionBuilder(this);
  this.evaluator = new ExpressionEvaluator(this);
  this.default = new ExpressionDefault(this);
  this.handler = this.default;
}

ExpressionManager.prototype.evaluate = function(expression, row) {
  return this.handler.evaluate(expression, row);
}

ExpressionManager.prototype.build = function(expression) {
  return this.handler.build(expression, argument);
}

ExpressionManager.prototype.getValueOf = function(expression) {
  return this.handler.getValueOf(expression);
}

ExpressionManager.prototype.setup = function(expression) {
  return this.handler.setup(expression);
}

ExpressionManager.prototype.create = function(expression, argument) {
  return this.handler.create(expression, argument);
}

var ExpressionHandler = function() {}

ExpressionHandler.prototype.build = function(expression) {
  // if this is called, switch to the builder
  var oldHandler = this.manager.handler;
  this.manager.handler = this.manager.builder;
  this.manager.builder.build(expression);
  this.manager.handler = oldHandler;
}

ExpressionHandler.prototype.evaluate = function(expression, row) {
  // if this is called, switch to the evaluator
  var oldHandler = this.manager.handler;
  this.manager.handler = this.manager.evaluator;
  var value = this.manager.evaluator.evaluate(expression, row);
  this.manager.handler = oldHandler;
  return value;
}

ExpressionHandler.prototype.create = function(expressionConstructor, argument) {
  return new expressionConstructor(argument);
}

var ExpressionBuilder = function(manager) {
  this.manager = manager;
  this._expression = null;
}
ExpressionBuilder.prototype = Object.create(ExpressionHandler.prototype);
ExpressionBuilder.prototype.constructor = ExpressionBuilder;

ExpressionBuilder.prototype.build = function(expression) {
  var parentExpression = this._expression;
  this._expression = expression;
  var result = expression.func();
  if (typeof result === "object" && result instanceof Expression) {
    this._expression.addExpression(result);
  }
  this._expression = parentExpression;
}

ExpressionBuilder.prototype.setup = function(expression) {
  return this.build(expression);
}

ExpressionBuilder.prototype.getValueOf = function(expression) {
  this._expression.addExpression(expression);
  return expression.stubValue();
}

var ExpressionEvaluator = function(manager) {
  this.manager = manager;
  this._expression = null;
}
ExpressionEvaluator.prototype = Object.create(ExpressionHandler.prototype);
ExpressionEvaluator.prototype.constructor = ExpressionEvaluator;

ExpressionEvaluator.prototype.evaluate = function(expression, row) {
  var getter = expression.getter;
  var parentGetter = this._getter;
  this._getter = getter;
  var value = expression.func();
  while (value instanceof Expression) {
    value = value.value(row);
  }
  this._getter = getter;
  return value;
}

ExpressionEvaluator.prototype.getValueOf = function(expression) {
  return this._getter.valueOf();
}

ExpressionEvaluator.prototype.setup = function(expression) {
  return expression;
}

ExpressionEvaluator.prototype.create = function(expressionConstructor, argument) {
  return this._getter;
}

var ExpressionDefault = function(manager) {
  this.manager = manager;
}
ExpressionDefault.prototype = Object.create(ExpressionHandler.prototype);
ExpressionDefault.prototype.constructor = ExpressionDefault;

ExpressionDefault.prototype.getValueOf = function(expression) {
  return expression.value();
}

ExpressionDefault.prototype.setup = function(expression) {
  return this.build(expression);
}

var Expression = function(arg) {
  this.filterLevel = 0;
  this.groupLevel = 0;
  this.accumulated = true;
  if (typeof arg === "function") {
    this.subExpressions = [new FunctionExpression(arg)];
  } else if (typeof arg === "object") {
    if (arg instanceof Expression) {
      this.subExpressions = [arg];
    }
  }
  this.signature = this.getSignature();
}

Expression.prototype.operate = function(row) {
  row.operationValues.set(this.signature, this.value(row));
}

Expression.prototype.getSignature = function() {
  var sig = this.value.toString();
  for (var i = 0; i < this.subExpressions.length; i++) {
    sig += this.subExpressions[i].signature;
  }
  return sig;
}

Expression.prototype.fromSignature = function(row) {
  return row.operationValues.get(this.signature);
}

Expression.prototype.init = function() {
  return null;
}

Expression.prototype.dependencies = function() {
  return this.subExpressions;
}

Expression.prototype.valueOf = function() {
  return expManager.getValueOf(this);
}

Expression.prototype.stubValue = function() {
  return 0;
}

Expression.prototype.value = function(row) {
  return this.subExpressions[0].fromSignature(row);
}

Expression.prototype.finishAccumulating = function() {
  this.accumulated = true;
}

Expression.prototype.finishAccumulating = function() {
  this.accumulated = true;
}

var SubExpressionGetter = function(expression) {
  this.expIndex = 0;
  this.subExpressions = expression.subExpressions;
}

SubExpressionGetter.prototype.set = function(row) {
  this.row = row;
  this.expIndex = 0;
}

SubExpressionGetter.prototype.valueOf = function() {
  return this.subExpressions[this.expIndex++].fromSignature(this.row);
}

var FunctionExpression = function(func) {
  this.func = func;
  this.subExpressions = [];
  this.getter = new SubExpressionGetter(this);
  expManager.setup(this);
  this.signature = this.getSignature();
}
FunctionExpression.prototype = Object.create(Expression.prototype);
FunctionExpression.prototype.constructor = FunctionExpression;

FunctionExpression.prototype.getSignature = function() {
  return this.func.toString();
}

FunctionExpression.prototype.dependencies = function() {
  return this.subExpressions;
}

FunctionExpression.prototype.addExpression = function(expression) {
  this.subExpressions.push(expression);
}

FunctionExpression.prototype.value = function(row) {
  this.getter.set(row);
  return expManager.evaluate(this, row);
}

var SquareExpression = function(arg) {
  Expression.call(this, arg);
}
SquareExpression.prototype = Object.create(Expression.prototype);
SquareExpression.prototype.constructor = SquareExpression;

SquareExpression.prototype.value = function(row) {
  var exp = this.subExpressions[0];
  return exp.value(row) * exp.value(row);
}

var ColumnExpression = function(name) {
  this.name = name;
  this.signature = this.getSignature();
}
ColumnExpression.prototype = Object.create(Expression.prototype);
ColumnExpression.prototype.constructor = ColumnExpression;

ColumnExpression.prototype.dependencies = function() {
  return [];
}

ColumnExpression.prototype.getSignature = function() {
  return this.name;
}

ColumnExpression.prototype.value = function(row) {
  return row.values[this.name];
}

ColumnExpression.prototype.fromSignature = function(row) {
  return row.values[this.name];
}

var expManager = new ExpressionManager();

var Expressions = {};

var square = function(arg) {
  return expManager.create(SquareExpression, arg);
}

Expressions.ColumnExpression = ColumnExpression;
Expressions.square = square;
Expressions.Expression = Expression;

module.exports = Expressions;


