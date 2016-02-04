"use strict";

var Expressions = require("./expressions.js");
var FunctionExpression = Expressions.FunctionExpression;
var SummaryFunctionExpression = Expressions.SummaryFunctionExpression;
var nullExp = Expressions.nullExp;

class Index {
  constructor() {
    this.value = 0;
  }

  set(value) {
    this.value = value;
  }
}

class SubIndex extends Index {
  constructor(parentIndex) {
    super();
    this.parentIndex = parentIndex;
    this.parentIndexValues = [];
  }

  set(value) {
    this.value = value;
    var parentIndexValue = this.parentIndexValues[value];
    this.parentIndex.set(parentIndexValue);
  }

  add() {
    var parentIndexValue = this.parentIndexValues[value];
    this.value = this.parentIndexValues.push(parentIndexValue) - 1;
  }

  numberOfRows() {
    return this.parentIndexValues.length;
  }
}

class Container {
  constructor() {
    this.value = NaN;
  }
}

class Property {
  constructor(descriptor) {
    this.descriptor = descriptor;
  }
}

class ContainerProperty extends Property {
  constructor(container) {
    var getter = function() { return container.value; };
    var descriptor = { get: getter };
    super(descriptor);
    this.container = container;
  }
}

class IndexedProperty extends Property {
  constructor(index, values) {
    var getter = function() { return values[index.value]; };
    var descriptor = { get: getter };
    super(descriptor);
    this.values = values;
    this.index = index;
  }
}

class RowValues {
  constructor(propertyMap) {
    for (var keyval of propertyMap) {
      var name = keyval[0];
      var prop = keyval[1];
      Object.defineProperty(this, name, prop.descriptor);
    }
  }
}

class StubValues {
  constructor(columnNames) {
    for (var columnName of columnNames) {
      var descriptor = { get: this.getGetter(columnName) };
      Object.defineProperty(this, columnName, descriptor);
    }
    this.requirements = new Set();
  }

  getGetter(name) {
    return () => { this.requirements.add(name); return NaN; };
  }
}

class Row {
  constructor(rowIndex, propertyMap, grouping) {
    this.rowIndex = rowIndex;
    this.propertyMap = propertyMap;
    this.grouping = grouping;
    this.values = new RowValues(propertyMap);
  }
}

class Grouping {
  constructor(parentGrouping, groupName) {
    this.parentGrouping = parentGrouping;
    if (parentGrouping === null) {
      this.groupName = null;
      this.keys = null;
      this.parents = null;
    } else {
      this.groupName = groupName;
      this.keys = [];
      this.parents = [];
    }
    this.index = new Index();
  }
}

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
    var subsequentRequirements = this.nextOperation.setup(this.newRow);
    var requirements = this.setupRequirements(subsequentRequirements, this.oldRow);
    return requirements;
  }

  setupRequirements(subsequentRequirements) {
    return subsequentRequirements;
  }

  checkRequirements(stub) {
    return this.nextOperation.checkRequirements(stub);
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

  checkRequirements(columnMap) {
    var columnSet = Set(columnMap.keys());
    this.nextOperation.checkRequirements(columnSet);
  }

  setup(columnMap) {
    var rowIndex = new Index();
    var grouping = new Grouping(null);
    var propertyMap = this.setupPropertyMap(columnMap, rowIndex);
    var newRow = new Row(rowIndex, propertyMap, grouping);
    this.rowIndex = rowIndex;
    this.groupIndex = grouping.index;
    return this.nextOperation.setup(newRow);
  }

  step(index) {
    this.rowIndex.value = index;
    this.groupIndex.value = 0;
    this.nextOperation.step();
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
  constructor(arg, container) {
    super();
    var exp = arg;
    if (typeof arg === "function") {
      exp = new FunctionExpression(arg);
    }
    this.expression = exp;
    this.container = container;
  }

  checkRequirements(columnSet) {
    var stub = new StubValues(columnSet);
    var subsequentRequirements = this.nextOperation.checkRequirements(columnSet);
    var expressionRequirements = this.expression.requirements(stub);
    for (var req of expressionRequirements) {
      subsequentRequirements.add(req);
    }
    return subsequentRequirements;
  }

  step() {
    this.container.value = this.expression.evaluate(this.row);
    this.nextOperation.step();
  }

  addState() {
    this.expression.addState();
    this.nextOperation.addState();
  }

}

class SummaryEvaluateOperation extends Operation {

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

  setupRequirements(subsequentRequirements) {
    var requirements = new Set();
    var inverseNameMap = this.inverseNameMap;
    for (var newName of subsequentRequirements) {
      requirements.add(inverseNameMap.get(newName) || newName);
    }
    return requirements;
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

  setupRequirements(subsequentRequirements) {
    for (var keyvalue of reIndexerMap) {
      var name = keyvalue[0];
      var reIndexer;
      if (subsequentRequirements.has(name)) {
        reIndexer = keyvalue[1];
        this.reIndexers.push(reIndexer);
      }
    }
    return subsequentRequirements;
  }

  step() {
    super.step();
    var reIndexer;
    for (var i = 0; i < this.reIndexers.length; i++) {
      reIndexer = this.reIndexers[i];
      reIndexer.add();
    }
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

  setupRequirements(subsequentRequirements, row) {
    subsequentRequirements.delete(this.name);
    return super.setupRequirements(subsequentRequirements, row);
  }
}

class AccumulateOperation extends Operation {
  constructor(arg) {
    super();
    var exp = arg;
    if (typeof arg === "function") {
      exp = new SummaryFunctionExpression(arg);
    }
    this.expression = exp;
  }

  step() {
    this.expression.evalute(this.row);
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
    return row.grouping.groupIndex;
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
Operations.FilterOperation = FilterOperation;
Operations.MutateOperation = MutateOperation;
Operations.GroupByOperation = GroupByOperation;
Operations.SummarizeOperation = SummarizeOperation;
Operations.GenerateRowOperation = GenerateRowOperation;
Operations.NewDataFrameOperation = NewDataFrameOperation;

module.exports = Operations;
