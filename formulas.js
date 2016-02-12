"use strict";

/*

Plan to add the following formulas:

i / n
first / last
nth
sampleN / sampleFrac
distinct / unique
sum / cumSum
min / cumMin
max / cumMax
mean / cumMean
median
sd
lead / lag
any / cumAny
all / cumAll
minRank / denseRank
percentRank / cumeDist
ntile


*/

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

  complete() {
    return;
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
    var newFormula = new formulaConstructor(arg);
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
    // calculate result of a CustomSummaryFormula
    this._nextFormula = formula.nextFormula;
    this._groupIndex = row.grouping.index.value;
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

  setRow(row) { return; }
}

var nullFormula = new NullFormula();

class Formula {
  constructor() {
    this.nextFormula = nullFormula;
    this.row = null;
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

  setRow(row) {
    this.row = row;
    this.nextFormula.setRow(row);
  }

  addState() {
    return this.nextFormula.addState();
  }

  finalValue(groupIndex) {
    return NaN;
  }

  complete() {
    return;
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

class LoopFormula extends StatefulFormula {

}

class SummaryFormula extends StatefulFormula {

}

class Comparator {
  constructor(formula, nextComparator) {
    this.values = [];
    this.formula = formula;
    this.nextComparator = nextComparator;
  }

  add() {
    this.values.push(this.formula.evaluate());
  }

  setRow(row) {
    this.formula.setRow(row);
    this.nextComparator.setRow(row);
  }

  compare(a, b) {
    var valA = this.values[a];
    var valB = this.values[b];
    var result;
    if (valA === valB) {
      result = this.nextComparator.compare(a, b); 
    } else if (valA > valB) {
      result = 1;
    } else {
      result = -1;
    }
    return result;
  }
}

class NullComparator {
  constructor() {}

  add() { return; }

  compare(a, b) { return 0; } 

  setRow(row) { return; }
}

class OrderFormula extends Formula {
  constructor(formulas) {
    super();
    var comparator = new NullComparator();
    for (var i = formulas.length - 1; i >= 0; i--) {
      comparator = new Comparator(formulas[i], comparator);
    }
    this.comparator = comparator;
    this.indices = [];
  }

  accumulate() {
    this.indices.push(this.indices.length);
    this.comparator.add();
  }

  setRow(row) {
    this.comparator.setRow(row);
  }

  complete() {
    var comparator = this.comparator;
    this.indices.sort(function(a, b) {
      return comparator.compare(a, b);
    });
  }

  finalValue() {
    return this.indices;
  }
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

class CustomFormula extends Formula {
  constructor(func) {
    super();
    this.func = func;
    this.nextFormula = nullFormula;
    expManager.build(this);
  }

  evaluate() {
    return expManager.evaluate(this, this.row);
  }

  requirements(row) {
    return expManager.test(this, row);
  }
}

class CustomSummaryFormula extends Formula {
  constructor(func) {
    super();
    this.func = func;
    this.nextFormula = nullFormula;
    expManager.build(this);
  }

  accumulate() {
    expManager.evaluate(this, this.row);
  }

  summarize() {
    return expManager.summarize(this, this.row);
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

Formulas.CustomFormula = CustomFormula;
Formulas.OrderFormula = OrderFormula;
Formulas.CustomSummaryFormula = CustomSummaryFormula;
Formulas.createFunction = createFunction;
Formulas.sum = sum;
Formulas.cumsum = cumsum;

module.exports = Formulas;
