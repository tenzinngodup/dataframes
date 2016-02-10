"use strict";

class FormulaManager {
  constructor() {
    this.builder = new FormulaBuilder();
    this.evaluator = new FormulaEvaluator();
    this.summarizer = new FormulaSummarizer();
    this.tester = new FormulaTester();
    this.default = new FormulaDefault();
    this.handler = this.default;
  }

  evaluate(formula, row) {
    this.handler = this.evaluator;
    var result = this.evaluator.evaluate(formula, row);
    this.handler = this.default;
    return result;
  }

  summarize(formula, row) {
    this.handler = this.summarizer;
    var result = this.summarizer.summarize(formula, row);
    this.handler = this.default;
    return result;
  }

  build(formula) {
    this.handler = this.builder;
    var result = this.builder.build(formula);
    this.handler = this.default;
    return result;
  }

  test(formula, row) {
    this.handler = this.tester;
    var result = this.tester.test(formula, row);
    this.handler = this.default;
    return result;
  }

  get(formulaConstructor, argument) {
    return this.handler.get(formulaConstructor, argument);
  }
}


class FormulaBuilder {
  constructor() {
    this._lastFormula = null;
  }

  build(formula) {
    this._lastFormula = formula;
    formula.func({});
  }

  get(formulaConstructor, arg) {
    var newFormula = new formulaConstructor();
    this._lastFormula.nextFormula = newFormula;
    this._lastFormula = newFormula;
    return newFormula.stubValue();
  }
}

class FormulaEvaluator {
  constructor() {
    this._nextFormula = null;
  }

  evaluate(formula, row) {
    this._nextFormula = formula.nextFormula;
    this._groupIndex = row.grouping.index.value;
    return formula.func(row.values);
  }

  get(formulaConstructor, arg) {
    var val = this._nextFormula.evaluate(arg, this._groupIndex);
    this._nextFormula = this._nextFormula.nextFormula;
    return val;
  }
}

class FormulaSummarizer {
  constructor() {
    this._nextFormula = null;
  }

  summarize(formula, row) {
    // calculate result of a SummaryFunctionFormula
    this._nextFormula = formula.nextFormula;
    this._groupIndex = row.rowIndex.value;
    var res = formula.func({});
    return res;
  }

  get(formulaConstructor, arg) {
    var val = this._nextFormula.finalValue(this._groupIndex);
    this._nextFormula = this._nextFormula.nextFormula;
    return val;
  }
}

class FormulaTester {
  constructor() {}

  test(formula, row) {
    var stub = row.getStub();
    formula.func(stub);
    return stub.requirements;
  }

  get(formulaConstructor, arg) {
    return NaN;
  }
}

class FormulaDefault {
  constructor() {}

  get(formulaConstructor, argument) {
    throw "FormulaDefault should not have been called";
  }
}

class NullFormula {
  constructor() {}

  addState() { return; }

  build() { return; }
}

var nullExp = new NullFormula();

class Formula {
  constructor() {
    this.nextFormula = nullExp;
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
    return this.nextFormula.addState();
  }

  finalValue(groupIndex) {
    return NaN;
  }
}

class StatefulFormula extends Formula {
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
    this.nextFormula.addState();
  }

  evaluate(value, groupIndex) {
    var newState = this.states[groupIndex] = this.accumulate(value, this.states[groupIndex]);
    return this.value(newState);
  }

  finalValue(groupIndex) {
    var val = this.value(this.states[groupIndex]);
    return val;
  }
}

class AccumulatorFormula extends StatefulFormula {

}

class SummaryFormula extends StatefulFormula {

}

class SumFormula extends SummaryFormula {
  accumulate(value, state) {
    return value + state;
  }
}

class CumulativeSumFormula extends AccumulatorFormula {
  accumulate(value, state) {
    return value + state;
  }
}

var expManager = new FormulaManager();

class FunctionFormula extends Formula {
  constructor(func) {
    super();
    this.func = func;
    this.nextFormula = nullExp;
    expManager.build(this);
  }

  evaluate(row) {
    return expManager.evaluate(this, row);
  }

  requirements(row) {
    return expManager.test(this, row);
  }
}

class SummaryFunctionFormula extends Formula {
  constructor(func) {
    super();
    this.func = func;
    this.nextFormula = nullExp;
    expManager.build(this);
  }

  accumulate(row) {
    expManager.evaluate(this, row);
  }

  summarize(row) {
    return expManager.summarize(this, row);
  }
}

var createFunction = function(formulaConstructor) {
  return function(arg) {
    return expManager.get(formulaConstructor, arg);
  }
}

var sum = createFunction(SumFormula);
var cumsum = createFunction(CumulativeSumFormula);

var Formulas = {};

Formulas.Formula = Formula;
Formulas.FunctionFormula = FunctionFormula;
Formulas.SummaryFunctionFormula = SummaryFunctionFormula;
Formulas.createFunction = createFunction;
Formulas.nullExp = nullExp;
Formulas.sum = sum;
Formulas.cumsum = cumsum;

module.exports = Formulas;
