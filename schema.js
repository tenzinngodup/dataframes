var Schema = function(df, source, names, namesToChange, columnsToAdd) {
  this.df = df;
  this.list = [];
  if (source instanceof Schema) {
    this.copySchema(source, names, namesToChange);
  } else if (source) {
    this.generateSchema(source, names);
  }
  if (columnsToAdd) {
    this.width += columnsToAdd.length;
    this.addColumns(columnsToAdd, namesToChange, false);    
  }
  this.defineRowObject();
}

Schema.prototype.addColumns = function(columnsToAdd, namesToChange, copy) {
  var column, name, newColumn;
  var list = this.list;
  var df = this.df;
  for (var i = 0; i < columnsToAdd.length; i++) {
    column = columnsToAdd[i];
    name = column.name;
    if (namesToChange && namesToChange.hasOwnProperty(name)) {
      name = namesToChange[name];
    }
    if (copy) {
      newColumn = new Column(df, name, column.index, column.type) 
    } else {
      if (namesToChange && namesToChange.hasOwnProperty(name)) {
        column.name = namesToChange[name];
      }
      newColumn = column;
    }
    list.push(newColumn);
    this[name] = newColumn;
  }
}

Schema.prototype.generateSchema = function(source, selectedNames) {
  var df = this.df;
  var list = this.list;
  var columnNames = selectedNames || Object.keys(source);
  var name, type, column;
  for (var i = 0; i < columnNames.length; i++) {
    name = columnNames[i];
    type = typeof source[name];
    column = new Column(df, name, i, type);
    list.push(column);
    this[name] = column;
  }
  this.width = this.list.length;
}

Schema.prototype.defineRowObject = function() {
  var RowObject = function() {}
  RowObject.prototype = Object.create(Row);
  RowObject.prototype.constructor = RowObject;
  RowObject.prototype.schema = this;
  var list = this.list;
  var column;
  var generateGetter = function(index) {
    return function() {
      return this.data[index]; 
    }
  }
  for (var i = 0; i < list.length; i++) {
    column = list[i];
    var getter = generateGetter(column.index);
    Object.defineProperty(RowObject.prototype, column.name, { 
      get: getter
    });
  }
  this.RowObject = RowObject;
}

Schema.prototype.copySchema = function(schema, selectedNames, namesToChange) {
  var column, newColumn, name;
  var selectedNameMap = {};
  if (selectedNames) {
    var selectedColumns = [];
    for (var i = 0; i < selectedNames.length; i++) {
      name = selectedNames[i];
      if (schema.hasOwnProperty(name)) {
        column = schema[name];
        selectedColumns.push(column);
        selectedNameMap[name] = true;
      }
    }
    var changingNames = Object.keys(namesToChange);
    for (var i = 0; i < changingNames.length; i++) {
      name = changingNames[i];
      if (schema.hasOwnProperty(name) && !selectedNameMap.hasOwnProperty(name)) {
        column = schema[name];
        selectedColumns.push(column);
      }
    }
    this.addColumns(selectedColumns, namesToChange, true);
  }
  else {
    this.addColumns(schema.list, namesToChange, true);
  }
}

Schema.prototype.createDataFrame = function(rows) {
  return this.df._new(rows, this);
}

Schema.prototype.toString = function() {
  var list = this.list;
  var columnNameString = "";
  for (var i = 0; i < list.length; i++) {
    columnNameString += list[i].name + " ";
  }
  return columnNameString
}

Schema.prototype.getColumnNames = function() {
  var columnNames = [];
  for (var i = 0; i < this.list.length; i++) {
    columnNames.push(this.list[i].name)
  }
  return columnNames;
}


var Row = function(rowData, schema, changedNames) {
  this.schema = schema;
  var columnNames = schema.columnNames;
  var columnTypes = schema.columnTypes;
  var name, type, value;
  if (changedNames) {
    var changed;
    for (var i = 0; i < columnNames.length; i++) {
      name = columnNames[i];
      type = columnTypes[i];
      changed = changedNames[name];
      value = changed ? rowData[changed] : rowData[name];
      if (typeof value === type) {
        this[name] = value;
      } else {
        this[name] = null;
      }
    }
  } else {
    for (var i = 0; i < columnNames.length; i++) {
      var name = columnNames[i];
      var type = columnTypes[i];
      var value = rowData[name];
      if (typeof value === type) {
        this[name] = value;
      } else {
        this[name] = null;
      }
    }
  }
}

Row.prototype.get = function(colIdentifier) {
  return typeof colIdentifier === "number" ? this.getByIndex(colIdentifier) : this[colIdentifier];
}

Row.prototype.getByIndex = function(colIndex) {
  var schema = this.schema;
  var name = schema.columnNames[colIndex];
  return this[name];
}

Row.prototype.toString = function(columns) {
  var columnNames = columns || this.schema.list.map(function(column) { return column.name; })
  var rowString = "";
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    rowString += this[name] + " ";
  }
  return rowString;
}


var RowIterator = function(df) {
  this.df = df;
  this.index = 0;
}

RowIterator.prototype.next = function() {
  var rowArray = this.df.rowArray;
  if (this.index < rowArray.length) {
    var result = {done: false, value: rowArray[this.index]};
    this.index++;
    return result;
  } else {
    return {done: true}
  }
}

RowIterator.prototype["@@iterator"] = function() {
  return this;
}

var Column = function(df, name, index, type) {
  this.df = df;
  this.name = name;
  this.index = index;
  this.type = type;
}

Column.prototype.get = function(rowIndex) {
  var row = this.df.row(rowIndex)
  return row[this.name];
}

Column.prototype.toArray = function() {
  var columnArray = [];
  var columnIterator = this["@@iterator"]();
  var next = columnIterator.next();
  while (!next.done) {
    columnArray.push(next.value);
    next = columnIterator.next();
  }
  return columnArray;
}

Column.prototype["@@iterator"] = function() {
  return new SingleColumnIterator(this);
}

var SingleColumnIterator = function(column) {
  var df = column.df;
  this.columnName = column.name;
  this.rowIterator = df["@@iterator"]();
}

SingleColumnIterator.prototype.next = function() {
  var nextRowValue = this.rowIterator.next();
  if (nextRowValue.done) {
    return {done: true};
  } else {
    var row = nextRowValue.value;
    return {done: false, value: row[this.columnName]}
  }
}

Schema.Column = Column;
Schema.Row = Row;

module.exports = Schema;
