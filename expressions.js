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
  var result = expression.func(new Stub(["first_name", "pres_number", "party"]));
  if (typeof result === "object" && result instanceof Expression) {
    console.log("result is", result.toString());
    this._expression.addExpression(result);
  }
  this._expression = parentExpression;
}

ExpressionBuilder.prototype.setup = function(expression) {
  return this.build(expression);
}

ExpressionBuilder.prototype.getValueOf = function(expression) {
  if (!(expression instanceof ColumnExpression)) {
    this._expression.addExpression(expression);
  }
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
  var result = expression.func(row.values);
  if (typeof result === "object" && result instanceof SubExpressionGetter) {
    result = result.valueOf();
  }
  this._getter = parentGetter;
  return result;
}

ExpressionEvaluator.prototype.getValueOf = function(expression) {
  return this._getter.valueOf();
}

ExpressionEvaluator.prototype.setup = function(expression) {
  return expression;
}

ExpressionEvaluator.prototype.create = function(expressionConstructor, argument) {
  console.log("creating");
  return this._getter;
}

var ExpressionDefault = function(manager) {
  this.manager = manager;
}
ExpressionDefault.prototype = Object.create(ExpressionHandler.prototype);
ExpressionDefault.prototype.constructor = ExpressionDefault;

ExpressionDefault.prototype.getValueOf = function(expression) {
  return null;
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
}

Expression.prototype.accumulatorChain = function(chain) {
  for (var i = 0; i < this.subExpressions.length; i++) {
    chain = this.subExpressions[i].accumulatorChain(chain);
  }
  return chain;
}

Expression.prototype.valueOf = function() {
  return expManager.getValueOf(this);
}

Expression.prototype.stubValue = function() {
  return 0;
}

Expression.prototype.value = function(row) {
  return this.valueOf();
}

var AccumulatorExpression = function(arg) {
  Expression.call(this, arg);
  this.states = [];
}
AccumulatorExpression.prototype = Object.create(Expression.prototype);
AccumulatorExpression.prototype.constructor = AccumulatorExpression;

AccumulatorExpression.prototype.init = function() {
  return null;
}

AccumulatorExpression.prototype.accumulate = function(row, state) {
  return state;
}

AccumulatorExpression.prototype.update = function(row, state) {
  var groupIndex = row.groupIndex;
  this.states[groupIndex] = this.accumulate(row, this.states[groupIndex]);
}

AccumulatorExpression.prototype.build = function() {
  this.states.push(this.init());
}

AccumulatorExpression.prototype.value = function(state) {
  return state;
}

AccumulatorExpression.prototype.accumulatorChain = function(chain) {
  this.nextAccumulator = this.subExpressions[0].accumulatorChain(chain);
  return this;
}

var CumulativeSumExpression = function(arg) {
  AccumulatorExpression.call(this, arg);
}
CumulativeSumExpression.prototype = Object.create(AccumulatorExpression.prototype);
CumulativeSumExpression.prototype.constructor = CumulativeSumExpression;

CumulativeSumExpression.prototype.init = function() {
  return 0;
}

CumulativeSumExpression.prototype.accumulate = function(row, state) {
  return state + this.subExpressions[0].value(row);
}

var SubExpressionGetter = function(expression) {
  this.expIndex = 0;
  this.subExpressions = expression.subExpressions;
}

SubExpressionGetter.prototype.toString = function() {
  return "subExpression getter " + this.expIndex;
}

SubExpressionGetter.prototype.set = function(row) {
  this.row = row;
  this.expIndex = 0;
}

SubExpressionGetter.prototype.valueOf = function() {
  console.log("index", this.expIndex);
  var val = this.subExpressions[this.expIndex++].fromSignature(this.row);
  console.log(val);
  return val;
}

var Stub = function(columnNames) {
  for (var i = 0; i < columnNames.length; i++) {
    this[columnNames[i]] = new ColumnExpression(columnNames[i]);
  }
}

var FunctionParser = function() {}
/*
FunctionParser.prototype.parseFunction = function(func) {
  var funcSource = "var theFunction = " + func.toString();
  var tree = esprima.parse(funcSource);
  var functionBody = tree.body[0].declarations[0].init.body.body;
  if (functionBody.length > 1) {
    throw "Currently cannot support functions with more than one statement.";
  } else {
    this.parseStatement(functionBody[0]);
  }
}

FunctionParser.prototype.parseStatement = function(statement) {
}
*/

var FunctionExpression = function(func) {
  this.func = func;
  this.subExpressions = [];
  this.getter = new SubExpressionGetter(this);
  expManager.setup(this);
}
FunctionExpression.prototype = Object.create(Expression.prototype);
FunctionExpression.prototype.constructor = FunctionExpression;

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
}
ColumnExpression.prototype = Object.create(Expression.prototype);
ColumnExpression.prototype.constructor = ColumnExpression;

ColumnExpression.prototype.dependencies = function() {
  return [];
}

ColumnExpression.prototype.value = function(row) {
  return row.values[this.name];
}

ColumnExpression.prototype.fromSignature = function(row) {
  return row.values[this.name];
}

ColumnExpression.prototype.accumulatorChain = function(chain) {
  return chain;
}

var expManager = new ExpressionManager();

var Expressions = {};

var cumsum = function(arg) {
  return expManager.create(CumulativeSumExpression, arg);
}

var square = function(arg) {
  return expManager.create(SquareExpression, arg);
}

Expressions.Expression = Expression;
Expressions.ColumnExpression = ColumnExpression;
Expressions.cumsum = cumsum;
Expressions.square = square;

module.exports = Expressions;


