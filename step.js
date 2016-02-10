"use strict";

var Tools = require("./tools.js");

var Operations = require("./operations.js");
var Container = Tools.Container;

var Formulas = require("./formulas.js");
var FunctionFormula = Formulas.FunctionFormula;
var SummaryFunctionFormula = Formulas.SummaryFunctionFormula;

var EvaluateOperation = Operations.EvaluateOperation;
var SelectOperation = Operations.SelectOperation;
var RenameOperation = Operations.RenameOperation;
var SliceOperation = Operations.SliceOperation;
var ArrangeOperation = Operations.ArrangeOperation;
var EvaluateOperation = Operations.EvaluateOperation;
var SummaryEvaluateOperation = Operations.SummaryEvaluateOperation;
var AccumulateOperation = Operations.AccumulateOperation;
var FilterOperation = Operations.FilterOperation;
var MutateOperation = Operations.MutateOperation;
var NewDataFrameOperation = Operations.NewDataFrameOperation;
var GenerateRowOperation = Operations.GenerateRowOperation;
var GroupByOperation = Operations.GroupByOperation;
var SummarizeOperation = Operations.SummarizeOperation;

class Step {
  constructor(previousStep) {
    this.previousStep = previousStep;
  }
}

class FirstStep {
  constructor(columns, numRows) {
    this.columns = columns;
    this.numRows = numRows;
  }

  buildOperation(nextOperation) {
    var firstOp = new GenerateRowOperation(this.columns, this.numRows);
    firstOp.setNextOperation(nextOperation);
    return firstOp;
  }
}

class NewDataFrameStep extends Step {
  buildOperation() {
    var lastOp = new NewDataFrameOperation();
    return this.previousStep.buildOperation(lastOp);
  }
}

class SliceStep extends Step {
  constructor(previousStep, begin, end) {
    this.begin = begin;
    this.end = end;
  }

  buildOperation(nextOperation) {
    var sliceOp = new SliceOperation(this.begin, this.end);
    sliceOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(sliceOp);
  }
}

class SelectStep extends Step {
  constructor(previousStep, arg) {
    super(previousStep);
    this.arg = arg;
  }

  buildOperation(nextOperation) {
    var selectOp = new SelectOperation(this.arg);
    selectOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(selectOp);
  }
}

class RenameStep extends Step {
  constructor(previousStep, arg) {
    super(previousStep);
    this.arg = arg;
  }

  buildOperation(nextOperation) {
    var renameOp = new RenameOperation(this.arg);
    renameOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(renameOp);
  }
}

class ArrangeStep extends Step {
  constructor(previousStep, arg) {
    super(previousStep, arg);
  }

  buildOperation(container) {
    var formula = new FunctionFormula(this.arg);
    var container = new Container();
    var arrangeOp = new ArrangeOperation(container);
    customOp.setNextOperation(nextOperation);
    var evaluateOp = new EvaluateOperation(container, formula);
    evaluateOp.setNextOperation(customOp);
    return this.previousStep.buildOperation(evaluateOp);
  }
}

class EvaluateStep extends Step {
   constructor(previousStep, arg) {
    super(previousStep);
    this.arg = arg;
  }

  buildOperation(nextOperation) {
    var formula = new FunctionFormula(this.arg);
    var container = new Container();
    var customOp = this.getOperation(container);
    customOp.setNextOperation(nextOperation);
    var evaluateOp = new EvaluateOperation(container, formula);
    evaluateOp.setNextOperation(customOp);
    return this.previousStep.buildOperation(evaluateOp);
  }
}

class SummarizeStep extends Step {
  constructor(previousStep, arg, name) {
    super(previousStep);
    this.arg = arg;
    this.name = name;
  }

  buildOperation(nextOperation) {
    var container = new Container();
    var formula = new SummaryFunctionFormula(this.arg);
    var accOp = new AccumulateOperation(formula);
    var summarizeOp = new SummarizeOperation();
    var sumEvOp = new SummaryEvaluateOperation(container, formula);
    var mutateOp = new MutateOperation(container, this.name);
    accOp.setNextOperation(summarizeOp);
    summarizeOp.setNextOperation(sumEvOp);
    sumEvOp.setNextOperation(mutateOp);
    mutateOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(accOp);
  }
}

class MutateStep extends EvaluateStep {
  constructor(previousStep, arg, name) {
    super(previousStep, arg);
    this.name = name;
  }

  getOperation(container) {
    return new MutateOperation(container, this.name);
  }
}

class FilterStep extends EvaluateStep {
  getOperation(container) {
    return new FilterOperation(container);
  }
}

class GroupByStep extends EvaluateStep {
  constructor(previousStep, arg, name) {
    super(previousStep, arg);
    this.name = name;
  }

  getOperation(container) {
    return new GroupByOperation(container, this.name);
  }
}

var Steps = {};

Steps.Step = Step;
Steps.FirstStep = FirstStep;
Steps.SliceStep = SliceStep;
Steps.SelectStep = SelectStep;
Steps.RenameStep = RenameStep;
Steps.EvaluateStep = EvaluateStep;
Steps.SummarizeStep = SummarizeStep;
Steps.MutateStep = MutateStep;
Steps.FilterStep = FilterStep;
Steps.GroupByStep = GroupByStep;
Steps.NewDataFrameStep = NewDataFrameStep;

module.exports = Steps;
