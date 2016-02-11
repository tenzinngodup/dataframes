"use strict";

var Operations = require("./operations.js");

var Formulas = require("./formulas.js");
var CustomFormula = Formulas.CustomFormula;
var CustomSummaryFormula = Formulas.CustomSummaryFormula;
var OrderFormula = Formulas.OrderFormula;

var SelectOperation = Operations.SelectOperation;
var RenameOperation = Operations.RenameOperation;
var SliceOperation = Operations.SliceOperation;
var ArrangeOperation = Operations.ArrangeOperation;
var OrderOperation = Operations.OrderOperation;
var SummaryMutateOperation = Operations.SummaryMutateOperation;
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

class FormulaStep extends Step {
   constructor(previousStep, arg) {
    super(previousStep);
    this.arg = arg;
  }

  buildOperation(nextOperation) {
    var formula = new CustomFormula();
    var op = this.getOperation(formula);
    op.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(op);
  }
}

class SummarizeStep extends Step {
  constructor(previousStep, arg, name) {
    super(previousStep);
    this.arg = arg;
    this.name = name;
  }

  buildOperation(nextOperation) {
    var formula = new CustomSummaryFormula(this.arg);
    var accOp = new AccumulateOperation(formula);
    var summarizeOp = new SummarizeOperation();
    var sumMutateOp = new SummaryMutateOperation(formula, this.name);
    accOp.setNextOperation(summarizeOp);
    summarizeOp.setNextOperation(sumMutateOp);
    sumMutateOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(accOp);
  }
}

class MutateStep extends FormulaStep {
  constructor(previousStep, arg, name) {
    super(previousStep, arg);
    this.name = name;
  }

  getOperation(formula) {
    return new MutateOperation(formula, this.name);
  }
}

class ArrangeStep extends FormulaStep {
  buildOperation(nextOperation) {
    var orderingFormula = new CustomFormula(this.arg);
    var formula = new OrderFormula([orderingFormula]);
    var arrangeOp = new ArrangeOperation(formula);
    arrangeOp.setNextOperation(nextOperation);
    return this.previousStep.buildOperation(arrangeOp);
  }
}

class FilterStep extends FormulaStep {
  getOperation(formula) {
    return new FilterOperation(formula);
  }
}

class GroupByStep extends FormulaStep {
  constructor(previousStep, arg, name) {
    super(previousStep, arg);
    this.name = name;
  }

  getOperation(formula) {
    return new GroupByOperation(formula, this.name);
  }
}

var Steps = {};

Steps.Step = Step;
Steps.FirstStep = FirstStep;
Steps.SliceStep = SliceStep;
Steps.SelectStep = SelectStep;
Steps.RenameStep = RenameStep;
Steps.ArrangeStep = ArrangeStep;
Steps.SummarizeStep = SummarizeStep;
Steps.MutateStep = MutateStep;
Steps.FilterStep = FilterStep;
Steps.GroupByStep = GroupByStep;
Steps.NewDataFrameStep = NewDataFrameStep;

module.exports = Steps;
