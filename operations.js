"use strict";


var Tools = require("./tools.js");
var Index = Tools.Index;
var SubIndex = Tools.SubIndex;
var Container = Tools.Container;
var Property = Tools.Property;
var ContainerProperty = Tools.ContainerProperty;
var IndexedProperty = Tools.IndexedProperty;
var Row = Tools.Row;
var RowValues = Tools.RowValues;
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
    return row.rowIndex;
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
  constructor(container, values) {
    this.container = container;
    this.values = values;
  }

  add() {
    this.values.push(container.value);
  }
}

class EvaluateOperation extends Operation {
  constructor(container, exp) {
    super();
    this.expression = exp;
    this.container = container;
  }

  step() {
    this.container.value = this.expression.evaluate(this.oldRow);
    this.nextOperation.step();
  }

  addState() {
    this.expression.addState();
    this.nextOperation.addState();
  }

}

class SummaryEvaluateOperation extends EvaluateOperation {
  step() {
    this.container.value = this.expression.summarize(this.oldRow);
    this.nextOperation.step();
  }

  addState() {
    this.nextOperation.addState();
  }
}

class ExpressionOperation extends Operation {
  constructor(container) {
    super();
    this.container = container;
  }
}

class FilterOperation extends ExpressionOperation {
  step() {
    if (this.container.value) {
      this.nextOperation.step();
    }
  }
}

class SelectOperation extends Operation {
  constructor(arg) {
    super();
    this.arg = arg;
  }

  setupPropertyMap(oldPropertyMap) {
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
    var newPropertyMap = new Map();
    var inverseNameMap = new Map();
    var oldNames = Object.keys(obj);
    var oldName, newName, prop;
    for (var i = 0; i < oldNames.length; i++) {
      oldName = oldNames[i];
      prop = propertyMap.get(oldName);
      if (prop !== undefined) {
        newName = obj[oldName];
        newPropertyMap.set(newName, prop);
        inverseNameMap.set(newName, oldName);
      }
    }
    this.inverseNameMap = inverseNameMap;
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
    this.subIndex = new SubIndex(row.rowIndex);
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
      if (prop.container) {
        var values = [];
        var reIndexer = new ReIndexer(prop.container, values);
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

class ColumnBuildOperation extends ExpressionOperation {
  constructor(container, columnArray) {
    super();
    this.container = container;
    this.column = columnArray;
  }

  step() {
    this.column.push(this.container.value);
  }
}

class ArrangeOperation extends Operation {
  constructor(container, column, name) {
    super(container);
    this.column = column;
    this.name = name;
  }

  setupRowIndex(row) {
    this.subIndex = new SubIndex(row.rowIndex);
    return;
  }

  step() {
    this.subIndex.add();
  }

  complete() {
    var numRows = this.subIndex.numberOfRows();
    // to be continued
  }
}

class MutateOperation extends ExpressionOperation {
  constructor(container, name) {
    super(container);
    this.name = name;
  }

  setupPropertyMap(row) {
    var propertyMap = new Map(row.propertyMap);
    var prop = new ContainerProperty(this.container);
    propertyMap.set(this.name, prop);
    return propertyMap;
  }
}

class AccumulateOperation extends EvaluateOperation {
  constructor(exp) {
    super();
    this.expression = exp;
  }

  step() {
    this.expression.accumulate(this.oldRow);
    this.nextOperation.step();
  }
}

class SummarizeOperation extends Operation {
  constructor() {
    super();
    this.container = new Container();
  }

  setupPropertyMap(row) {
    var propertyMap = new Map();
    var grouping = row.grouping;
    var groupName = grouping.groupName;
    if (groupName !== null) {
      var keyProp = new ContainerProperty(this.container);
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
    var oldGrouping = this.oldRow.grouping;
    // set up subsequent states
    var newGrouping = this.newRow.grouping;
    if (newGrouping !== null && newGrouping.parentGrouping !== null) {
      var numberOfNewGroups = newGrouping.keys.length;
      for (var i = 0; i < numberOfParentGroups; i++) {
        this.nextOperation.addState();
      }
    } else {
      this.nextOperation.addState();
    }
    var groupIndex = oldGrouping.index;
    var keys = oldGrouping.keys;
    var numberOfGroups = oldGrouping.keys ? oldGrouping.keys.length : 1;
    if (keys) {
      for (var i = 0; i < numberOfGroups; i++) {
        groupIndex.value = i;
        this.container.value = keys[i];
        this.nextOperation.step();
      }
    } else {
      groupIndex.value = 0;
      this.nextOperation.step();
    }
    return this.nextOperation.complete();
  }
}

class GroupByOperation extends ExpressionOperation {
  constructor(container, name) {
    super(container);
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
    var key = this.container.value;
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
    this.getters = Array.from(row.propertyMap.values()).map(function(prop) { return prop.descriptor.get; });;
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
Operations.Container = Container;
Operations.SelectOperation = SelectOperation;
Operations.RenameOperation = RenameOperation;
Operations.SliceOperation = SliceOperation;
Operations.ArrangeOperation = ArrangeOperation;
Operations.FilterOperation = FilterOperation;
Operations.MutateOperation = MutateOperation;
Operations.GroupByOperation = GroupByOperation;
Operations.EvaluateOperation = EvaluateOperation;
Operations.SummaryEvaluateOperation = SummaryEvaluateOperation;
Operations.AccumulateOperation = AccumulateOperation;
Operations.SummarizeOperation = SummarizeOperation;
Operations.GenerateRowOperation = GenerateRowOperation;
Operations.NewDataFrameOperation = NewDataFrameOperation;

module.exports = Operations;
