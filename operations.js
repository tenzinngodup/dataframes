"use strict";

var Tools = require("./tools.js");
var Index = Tools.Index;
var SubIndex = Tools.SubIndex;
var Container = Tools.Container;
var ContainerProperty = Tools.ContainerProperty;
var IndexedProperty = Tools.IndexedProperty;
var Row = Tools.Row;
var Grouping = Tools.Grouping;


class Operation {
  constructor() {
    this.nextOperation = null;
    this.oldRow = null;
    this.newRow = null;
  }

  setNextOperation(nextOperation) {
    this.nextOperation = nextOperation;
  }

  setup(row) {
    this.oldRow = row;
    this.newRow = this.setupRow(row);
    this.nextOperation.setup(this.newRow);
  }

  setupRow(row) {
    var rowIndex = this.setupRowIndex(row);
    var propertyMap = this.setupPropertyMap(row);
    var grouping = this.setupGrouping(row);
    return new Row(rowIndex, propertyMap, grouping);
  }

  setupRowIndex(row) {
    return row.index;
  }

  setupPropertyMap(row) {
    return row.propertyMap;
  }

  setupGrouping(row) {
    return row.grouping;
  }

  step() {
    this.nextOperation.step();
  }

  addState() {
    this.nextOperation.addState();
  }

  complete() {
    return this.nextOperation.complete();
  }
}

class GenerateRowOperation extends Operation {
  constructor(columns, numRows) {
    super();
    this.columns = columns;
    this.numRows = numRows;
  }

  setupPropertyMap(columnMap, rowIndex) {
    var propertyMap = new Map();
    var column, columnName, prop;
    for (var keyvalue of columnMap) {
      columnName = keyvalue[0];
      column = keyvalue[1];
      prop = new IndexedProperty(rowIndex, column);
      propertyMap.set(columnName, prop);
    }
    return propertyMap;
  }

  setup() {
    var columnMap = this.columns;
    var rowIndex = new Index();
    var grouping = new Grouping(null);
    var propertyMap = this.setupPropertyMap(columnMap, rowIndex);
    var newRow = new Row(rowIndex, propertyMap, grouping);
    this.rowIndex = rowIndex;
    this.groupIndex = grouping.index;
    return this.nextOperation.setup(newRow);
  }

  run() {
    var numRows = this.numRows;
    for (var i = 0; i < numRows; i++) {
      this.rowIndex.value = i;
      this.groupIndex.value = 0;
      this.nextOperation.step();
    }
  }
}

class ReIndexer {
  constructor(formula, values) {
    this.formula = formula;
    this.values = values;
  }

  add() {
    this.values.push(this.formula.value);
  }
}

class FormulaOperation extends Operation {
  constructor(formula) {
    super();
    this.formula = formula;
  }

  setup(row) {
    this.formula.setRow(row);
    super.setup(row);
  }

  addState() {
    this.formula.addState();
    this.nextOperation.addState();
  }
}

class FilterOperation extends FormulaOperation {
  step() {
    if (this.formula.evaluate()) {
      this.nextOperation.step();
    }
  }
}

class SelectOperation extends Operation {
  constructor(arg) {
    super();
    this.arg = arg;
  }

  setupPropertyMap(row) {
    var oldPropertyMap = row.propertyMap;
    var names = this.getNames(oldPropertyMap);
    var newPropertyMap = new Map();
    var name, prop;
    for (var i = 0; i < names.length; i++) {
      name = names[i];
      prop = oldPropertyMap.get(name);
      newPropertyMap.set(name, prop);
    }
    return newPropertyMap;
  }

  getNames(oldPropertyMap) {
    var arg = this.arg;
    var names;
    if (typeof arg === "object") {
      if (Array.isArray(arg)) {
        names = arg.filter((name) => oldPropertyMap.has(name));
      } else if (arg instanceof RegExp) {
        var oldNames = Array.from(oldPropertyMap.keys());
        names = oldNames.filter((name) => arg.test(name));
      } else {
        // the SelectOperation just drops the columns
        // renaming is done in a subsequent RenameOperation
        var objectKeys = Object.keys(arg);
        names = objectKeys.filter((name) => oldPropertyMap.has(name));
      }
    } else if (typeof arg === "string") {
      names = [arg];
    }
    return names;
  }
}

class RenameOperation extends Operation {
  constructor(obj) {
    super();
    if (typeof obj !== "object") {
      throw "rename() must be given an object.";
    }
    this.obj = obj;
  }

  setupPropertyMap(row) {
    var obj = this.obj;
    var propertyMap = row.propertyMap;
    var newPropertyMap = new Map();
    var oldNames = Array.from(propertyMap.keys());
    var oldName, newName, prop;
    for (var i = 0; i < oldNames.length; i++) {
      oldName = oldNames[i];
      prop = propertyMap.get(oldName);
      if (obj.hasOwnProperty(oldName)) {
        newName = obj[oldName];
        newPropertyMap.set(newName, prop);
      } else {
        newPropertyMap.set(oldName, prop);
      }
    }
    return newPropertyMap;
  }

}

class SliceOperation extends Operation {
  constructor(begin, end) {
    super();
    this.begin = begin;
    this.end = (end !== undefined) ? end : Infinity;
    this.states = [];
  }

  setupGrouping(row) {
    this.groupIndex = row.grouping.index;
    return row.grouping;
  }

  addState() {
    this.states.push(0);
    this.nextOperation.addState();
  }

  step() {
    var groupIndexValue = this.groupIndex.value;
    var position = this.states[groupIndexValue];
    if (position >= this.begin && position < this.end) {
      this.nextOperation.step();
    }
    this.states[groupIndexValue]++;
  }
}

class ReIndexOperation extends Operation {
  constructor() {
    super();
    this.subIndex = null;
  }

  setupRowIndex(row) {
    this.subIndex = new SubIndex(row.index);
    return this.subIndex;
  }

  step() {
    this.subIndex.add();
    this.nextOperation.step();
  }
}

class PropertyReIndexOperation extends ReIndexOperation {
  constructor() {
    super();
    this.reIndexerMap = new Map();
    this.reIndexers = [];
  }

  setupPropertyMap(row) {
    var subIndex = this.subIndex;
    var propertyMap = row.propertyMap;
    var newPropertyMap = new Map();
    var prop, name;
    for (var keyvalue of propertyMap) {
      name = keyvalue[0];
      prop = keyvalue[1];
      if (prop.formula) {
        var values = [];
        var reIndexer = new ReIndexer(prop.formula, values);
        var reIndexedProp = new IndexedProperty(subIndex, values);
        newPropertyMap.set(name, reIndexedProp);
        this.reIndexerMap.set(name, reIndexer);
      } else {
        newPropertyMap.set(name, prop);
      }
    }
    return newPropertyMap;
  }

  step() {
    var reIndexer;
    for (var i = 0; i < this.reIndexers.length; i++) {
      reIndexer = this.reIndexers[i];
      reIndexer.add();
    }
    super.step();
  }
}

/*

class ColumnBuildOperation extends FormulaOperation {
  constructor(formula, columnArray) {
    super();
    this.formula = formula;
    this.column = columnArray;
  }

  step() {
    this.column.push(this.formula.evaluate());
  }
}

*/


class MutateOperation extends FormulaOperation {
  constructor(formula, name) {
    super(formula);
    this.name = name;
    this.container = new Container();
  }

  setupPropertyMap(row) {
    var propertyMap = new Map(row.propertyMap);
    var prop = new ContainerProperty(this.container);
    propertyMap.set(this.name, prop);
    return propertyMap;
  }

  step() {
    this.container.value = this.formula.evaluate();
    super.step();
  }
}

class SummaryMutateOperation extends MutateOperation {

  setup(row) {
    // don't change the formula's row
    this.oldRow = row;
    this.newRow = this.setupRow(row);
    this.nextOperation.setup(this.newRow);
  }

  step() {
    this.container.value = this.formula.summarize();
    this.nextOperation.step();
  }

  addState() {
    this.nextOperation.addState();
  }
}

class AccumulateOperation extends FormulaOperation {
  step() {
    this.formula.accumulate();
    this.nextOperation.step();
  }

  complete() {
    this.formula.complete();
    return this.nextOperation.complete();
  }
}

class ArrangeOperation extends FormulaOperation {
  setupRowIndex(row) {
    this.subIndex = new SubIndex(row.index);
    return this.subIndex;
  }

  step() {
    this.subIndex.add();
    this.formula.accumulate();
    return;
  }

  complete() {
    this.formula.complete();
    var indices = this.formula.finalValue();
    var subIndex = this.subIndex;
    subIndex.setParentIndices(indices);
    var numberOfRows = subIndex.length();
    for (var i = 0; i < numberOfRows; i++) {
      subIndex.set(i);
      this.nextOperation.step();
    }
    return this.nextOperation.complete();
  }
}

class SummarizeOperation extends Operation {
  constructor() {
    super();
  }

  setupPropertyMap(row) {
    var propertyMap = new Map();
    var grouping = row.grouping;
    var groupName = grouping.groupName;
    if (groupName !== null) {
      var keyProp = new IndexedProperty(grouping.index, grouping.keys);
      propertyMap.set(groupName, keyProp);
    }
    return propertyMap;
  }

  setupRowIndex(row) {
    return row.grouping.index;
  }

  setupGrouping(row) {
    var grouping = row.grouping;
    var parentGrouping = grouping.parentGrouping || grouping;
    return parentGrouping;
  }

  addState() {
    return;
  }

  step() {
    return;
  }

  complete() {
    // add states to subsequent operations
    var newGrouping = this.newRow.grouping;
    var numberOfGroups = newGrouping.keys ? newGrouping.keys.length : 1;
    for (var i = 0; i < numberOfGroups; i++) {
      this.nextOperation.addState();
    }
    var rowIndex = this.newRow.index;
    // step with new rows
    var oldGrouping = this.oldRow.grouping;
    var numberOfRows = oldGrouping.keys ? oldGrouping.keys.length : 1;
    for (rowIndex.value = 0; rowIndex.value < numberOfRows; rowIndex.value++) {
      this.nextOperation.step();
    }
    return this.nextOperation.complete();
  }
}

class GroupByOperation extends FormulaOperation {
  constructor(formula, name) {
    super(formula);
    this.name = name;
    this.groupMappings = [];
    this.grouping = null;
  }

  setupGrouping(row) {
    return new Grouping(row.grouping, this.name);
  }

  addState() {
    this.groupMappings.push(new Map());
  }

  step() {
    var grouping = this.newRow.grouping;
    var key = this.formula.evaluate();
    var parentGroupIndexValue = grouping.parentGrouping.index.value;
    var groupMap = this.groupMappings[parentGroupIndexValue];
    var newGroupIndexValue = groupMap.get(key);
    if (newGroupIndexValue === undefined) {
      newGroupIndexValue = grouping.keys.length;
      groupMap.set(key, newGroupIndexValue);
      grouping.parents.push(parentGroupIndexValue);
      grouping.keys.push(key);
      this.nextOperation.addState();
    }
    grouping.index.value = newGroupIndexValue;
    this.nextOperation.step();
  }
}

class NewDataFrameOperation extends Operation {
  setup(row) {
    this.columnNames = Array.from(row.propertyMap.keys());
    this.numberOfColumns = this.columnNames.length;
    this.getters = Array.from(row.propertyMap.values()).map(function(prop) { return prop.descriptor.get; });
    this.columns = this.columnNames.map(function() { return []; });
    this.columnWidths = this.columnNames.map(function(name) { return name.length; });
    return new Set(this.columnNames);
  }

  addState() {
    return;
  }

  step() {
    var getter, val, columnWidth;
    var getters = this.getters;
    var columns = this.columns;
    var numberOfColumns = this.numberOfColumns;
    var columnWidths = this.columnWidths;
    for (var colIndex = 0; colIndex < numberOfColumns; colIndex++) {
      getter = getters[colIndex];
      val = getter();
      columnWidth = val.toString().length;
      columns[colIndex].push(val);
      if (columnWidth > columnWidths[colIndex]) {
        columnWidths[colIndex] = columnWidth;
      }
    }
  }

  complete() {
    return {columnNames: this.columnNames, columns: this.columns, columnWidths: this.columnWidths};
  }
}

var Operations = {};
Operations.SelectOperation = SelectOperation;
Operations.RenameOperation = RenameOperation;
Operations.SliceOperation = SliceOperation;
Operations.ArrangeOperation = ArrangeOperation;
Operations.FilterOperation = FilterOperation;
Operations.MutateOperation = MutateOperation;
Operations.GroupByOperation = GroupByOperation;
Operations.SummaryMutateOperation = SummaryMutateOperation;
Operations.AccumulateOperation = AccumulateOperation;
Operations.SummarizeOperation = SummarizeOperation;
Operations.GenerateRowOperation = GenerateRowOperation;
Operations.NewDataFrameOperation = NewDataFrameOperation;
Operations.PropertyReIndexOperation = PropertyReIndexOperation;

module.exports = Operations;
