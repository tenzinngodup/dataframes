"use strict";

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

var Tools = {};

Tools.Index = Index;
Tools.SubIndex = SubIndex;
Tools.Container = Container;
Tools.Property = Property;
Tools.ContainerProperty = ContainerProperty;
Tools.IndexedProperty = IndexedProperty;
Tools.Row = Row;
Tools.RowValues = RowValues;
Tools.Grouping = Grouping;

module.exports = Tools;
