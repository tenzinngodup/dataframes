"use strict";

class ExpressionManager {
  constructor() {
    this.builder = new ExpressionBuilder();
    this.evaluator = new ExpressionEvaluator();
    this.summarizer = new ExpressionSummarizer();
    this.tester = new ExpressionTester();
    this.default = new ExpressionDefault();
    this.handler = this.default;    
  }

  evaluate(expression, row) {
    this.handler = this.evaluator;
    var result = this.evaluator.evaluate(expression, row);
    this.handler = this.default;
    return result;
  }

  summarize(expression, groupIndex) {
    this.handler = this.summarizer;
    var result = this.summarizer.summarize(expression, groupIndex);
    this.handler = this.default;
    return result;
  }

  build(expression) {
    this.handler = this.builder;
    var result = this.builder.build(expression);
    this.handler = this.default;
    return result;
  }

  test(expression, row) {
    this.handler = this.tester;
    var result = this.tester.test(expression, row);
    this.handler = this.default;
    return result;
  }

  get(expressionConstructor, argument) {
    return this.handler.get(expressionConstructor, argument);
  }
}


class ExpressionBuilder {
  constructor() {
    this._lastExpression = null;    
  }

  build(expression) {
    this._lastExpression = expression;
    expression.func({});
  }

  get(expressionConstructor, arg) {
    var newExpression = new expressionConstructor();
    this._lastExpression.nextExpression = newExpression;
    this._lastExpression = newExpression;
    return newExpression.stubValue();
  }
}

class ExpressionEvaluator {
  constructor() {
    this._nextExpression = null;
  }

  evaluate(expression, row) {
    this._nextExpression = expression.nextExpression;
    this._groupIndex = row.grouping.index.value;
    return expression.func(row.values);
  }

  get(expressionConstructor, arg) {
    var val = this._nextExpression.evaluate(arg, this._groupIndex);
    this._nextExpression = this._nextExpression.nextExpression;
    return val;
  }
}

class ExpressionSummarizer {
  constructor() {
    this._nextExpression = null;
  }

  summarize(expression, groupIndex) {
    // calculate result of a SummaryFunctionExpression
    this._nextExpression = expression.nextExpression;
    this._groupIndex = groupIndex;
    return expression.func({});
  }

  get(expressionConstructor, arg) {
    var val = this._nextExpression.finalValue(this._groupIndex);
    this._nextExpression = this._nextExpression.nextExpression;
    return val;
  }
}

class ExpressionTester {
  constructor() {}

  test(expression, row) {
    var stub = row.getStub();
    expression.func(stub);
    return stub.requirements;
  }

  get(expressionConstructor, arg) {
    return NaN;
  }
}

class ExpressionDefault {
  constructor() {}

  get(expressionConstructor, argument) {
    throw "ExpressionDefault should not have been called";
  }
}

class NullExpression {
  constructor() {}

  addState() { return; }

  build() { return; }
}

var nullExp = new NullExpression();

class Expression {
  constructor() {
    this.nextExpression = nullExp;
  }

  stubValue() {
    return 0;
  }

  value() {
    return NaN;
  }

  evaluate(value) {
    return value;
  }

  requirements(row) {
    return new Set();
  }

  addState() {
    return this.nextExpression.addState();
  }

  finalValue(groupIndex) {
    return NaN;
  }
}

class StatefulExpression extends Expression {
  constructor() {
    super();
    this.states = [];
  }

  init() {
    return 0;
  }

  accumulate(value, state) {
    return state;
  }

  value(state) {
    return state;
  }

  addState() {
    this.states.push(this.init());
    this.nextExpression.addState();
  }

  evaluate(value, groupIndex) {
    var newState = this.states[groupIndex] = this.accumulate(value, this.states[groupIndex]);
    return this.value(newState);
  }

  finalValue(groupIndex) {
    return this.value(this.states[groupIndex])
  }
}

class AccumulatorExpression extends StatefulExpression {

}

class SummaryExpression extends StatefulExpression {

}

class SumExpression extends SummaryExpression {

  accumulate(value, state) {
    return value + state;
  }

}

class CumulativeSumExpression extends AccumulatorExpression {
  
  accumulate(value, state) {
    return value + state;
  }

}

var expManager = new ExpressionManager();

class FunctionExpression extends Expression {
  constructor(func) {
    super();
    this.func = func;
    this.nextExpression = nullExp;
    expManager.build(this);
  }

  evaluate(row) {
    return expManager.evaluate(this, row);
  }

  requirements(row) {
    return expManager.test(this, row);
  }
}

class SummaryFunctionExpression extends Expression {
  constructor(func) {
    super();
    this.func = func;
    this.nextExpression = nullExp;
    expManager.build(this);
  }

  accumulate(row) {
    expManager.evaluate(this, row);
  }

  summarize(groupIndex) {
    return expManager.summarize(this, groupIndex);
  }
}

var createFunction = function(expressionConstructor) {
  return function(arg) {
    return expManager.get(expressionConstructor, arg);
  }
}

var sum = createFunction(SumExpression);
var cumsum = createFunction(CumulativeSumExpression);

var Expressions = {};

Expressions.Expression = Expression;
Expressions.FunctionExpression = FunctionExpression;
Expressions.SummaryFunctionExpression = SummaryFunctionExpression;
Expressions.createFunction = createFunction;
Expressions.nullExp = nullExp;
Expressions.sum = sum;
Expressions.cumsum = cumsum;

module.exports = Expressions;


