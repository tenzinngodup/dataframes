var copySchema = function(schema, selectedNames, changedNames) {
  var columnNames = schema.columnNames;
  var columnTypes = schema.columnTypes;
  var newColumnNames = [];
  var newColumnTypes = [];
  var changedNameKeys = (typeof changedNames === "object") ? Object.keys(changedNames) : undefined;
  for (var i = 0; i < columnNames.length; i++) {
    var columnName = columnNames[i];
    if (changedNames && changedNameKeys.indexOf(columnName) > -1) {
      newColumnNames.push(changedNames[columnName]);
      newColumnTypes.push(columnTypes[i]);
    } else if (!selectedNames || selectedNames.indexOf(columnName) > -1) {
      newColumnNames.push(columnName);
      newColumnTypes.push(columnTypes[i]);      
    } 
  }
  return {columnNames: newColumnNames, columnTypes: newColumnTypes};
}

var clone = function(original) {
  // hackish but close enough for now
  if (typeof original === "object") {
    var copy = {}
    for (var key in original) {
      copy[key] = original[key];
    }
    return copy;
  } else if (Array.isArray(original)) {
    copy = [];
    for (var i = 0; i < init.length; i++) {
      copy.push(original[i]);
    }
  } else {
    return init;
  }
}

var addOperation = function(subsequentChain, operation) {
  return function() {
    var op = new operation;
    op.addChain(subsequentChain);
    return op;
  }
}

var addOperations = function(subsequentChain, operations) {
  var chain = subsequentChain;
  for (var i = operations.length - 1; i >= 0; i--) {
    var operation = operations[i]
    chain = addOperation(chain, operation);
  }
  return chain;
}

var Operation = function() {}

Operation.prototype.onNext = function(val) {
  return this.subscriber.onNext(val);
}

Operation.prototype.onCompleted = function() {
  return this.subscriber.onCompleted();
}

Operation.prototype.onError = function(err) {
  throw err;
}

Operation.prototype.addChain = function(subsequentChain) {
  this.subscribe(subsequentChain());
}

Operation.prototype.subscribe = function(observer) {
  this.subscriber = observer;
}

var DataFrameBuilder = function() {
  this.rows = [];
}
DataFrameBuilder.prototype = Object.create(Operation.prototype);
DataFrameBuilder.prototype.constructor = DataFrameBuilder;

DataFrameBuilder.prototype.onNext = function(val) {
  this.rows.push(val);
  return true;
}

DataFrameBuilder.prototype.onCompleted = function() {
  return new DataFrame(this.rows);
}

var Filter = function(f) {
  this.f = f;
}

Filter.prototype = Object.create(Operation.prototype);
Filter.prototype.constructor = Filter;

Filter.prototype.onNext = function(val) {
  return this.f(val) ? this.subscriber.onNext(val) : true;
}


var Map = function(f) {
  this.f = f;
}
Map.prototype = Object.create(Operation.prototype);
Map.prototype.constructor = Map;

Map.prototype.onNext = function(val) {
  return this.subscriber.onNext(this.f(val));
}

var Reducer = function(accumulatorChain) {
  this.accumulator = accumulatorChain();
  this.count = 0;
}
Reducer.prototype = Object.create(Operation.prototype);
Reducer.prototype.constructor = Reducer;

Reducer.prototype.onNext = function(val) {
  this.count++;
  return this.accumulator.onNext(val);
}

Reducer.prototype.onCompleted = function() {
  var result = this.accumulator.onCompleted();
  this.subscriber.onNext(result);
  return this.subscriber.onCompleted();
}

var RowTransformer = function() {}
RowTransformer.prototype = Object.create(Operation.prototype);
RowTransformer.prototype.constructor = RowTransformer;

RowTransformer.prototype.onNext = function(val) {
  var row;
  if (!this.hasOwnProperty("schema")) this.schema = Row.getSchema(val);
  row = new Row(val, this.schema)
  return this.subscriber.onNext(row);
}

RowTransformer.prototype.onCompleted = function() {
  return this.subscriber.onCompleted();
}

var Accumulator = function(key, acc, setup, cleanup) {
  this.key = key;
  this.acc = acc;
  this.result = (typeof(setup) !== "undefined") ? setup() : null;
  if (cleanup) this.cleanup = cleanup;
}
Accumulator.prototype = Object.create(Operation.prototype);
Accumulator.prototype.constructor = Accumulator;

Accumulator.prototype.onNext = function(val) {
  this.result = this.acc(this.result, val);
  console.log("val", val);
  console.log("result", this.result);
  return this.subscriber.onNext(val);
}

Accumulator.prototype.onCompleted = function() {
  var result = this.subscriber.onCompleted();
  if (this.hasOwnProperty("cleanup")) {
    this.result = this.cleanup(this.result);
  }
  result[this.key] = this.result;
  return result;
}

var AccumulatorEnd = function() {}
AccumulatorEnd.prototype = Object.create(Operation.prototype);
AccumulatorEnd.prototype.constructor = AccumulatorEnd;

AccumulatorEnd.prototype.onNext = function(val) { return true; }

AccumulatorEnd.prototype.onCompleted = function() {
  return {}
}

GroupByOperation = function(keyFunction, operations) {
  this.keyFunction = keyFunction;
  this.groups = {};
  var groupEnd = new GroupEndOperation();
  var final = function() { return groupEnd; }
  this.groupEnd = groupEnd;
  this.groupedChain = addOperations(final, operations);
}

GroupByOperation.prototype = Object.create(Operation.prototype);
GroupByOperation.prototype.constructor = GroupByOperation;

GroupByOperation.prototype.onNext = function(val) {
  var key = this.keyFunction(val);
  var group;
  if (this.groups.hasOwnProperty(key)) {
    group = this.groups[key];
  } else {
    group = this.groupedChain();
    this.groups[key] = group;
  }
  return group.onNext(val);
}

GroupByOperation.prototype.onCompleted = function() {
  var groupResults = {};
  for (var key in this.groups) {
    var group = this.groups[key];
    group.onCompleted();
  }
  return this.subscriber.onCompleted();
}

GroupByOperation.prototype.subscribe = function(observer) {
  this.subscriber = observer;
  this.groupEnd.subscribe(observer);
}


GroupEndOperation = function() {}
GroupEndOperation.prototype = Object.create(Operation.prototype);
GroupEndOperation.prototype.constructor = GroupEndOperation;

GroupEndOperation.prototype.onCompleted = function() { return }

var swap = function(obj) {
  // swap keys and values

  var swapped = {};
  for (var key in obj) {
    swapped[obj[key]] = key;
  }
  return swapped;
}

var DataFrame = function(data, options) {
  options = options || {};
  var rowArray = [];
  if (data.length) {
    var schema = options.schema || Row.getSchema(data[0]);
    if (!schema.hasOwnProperty("columnTypes")) {
      schema.columnTypes = this.getColumnTypes(data[0], schema.columnNames);
    }
    this.schema = schema;
    this.setColumns();
    if (options.safe) {
      rowArray = data;
    } else {
      for (var i = 0; i < data.length; i++) {
        rowArray.push(new Row(data[i], schema));
      }
    }
  } else {
    this.schema = {"columnNames": [], "columnTypes": []};
  }
  this.rowArray = rowArray;
  this.operations = [];
  this.groupedOperations = [];
}

DataFrame.prototype.filter = function(func) {
  this.addOperation(Filter, func);
  return this;
}

DataFrame.prototype.mutate = function(newColumnName, func) {
  var newSchema = undefined;
  var newType = undefined;
  this.map(function(row) {
    var value = func(row);
    var newRow = Object.create(row);
    if (typeof newSchema === "undefined") {
      newSchema = copySchema(row.schema);
      newSchema.columnNames.push(newColumnName);
      newType = typeof value;
      newSchema.columnTypes.push(newType);
    }
    newRow.schema = newSchema;
    newRow[newColumnName] = (typeof value === newType) ? value : null;
    return newRow;
  });
  return this;
}

DataFrame.prototype.select = function() {
  var newNames = [];
  var namesToChange = {};
  for (var i = 0; i < arguments.length; i++) {
    var arg = arguments[i];
    var argType = typeof arg;
    if (argType === "string") {
      newNames.push(arg);
    } else if (argType === "object") {
      for (var columnName in arg) {
        namesToChange[columnName] = arg[columnName];
      }
    }
  }
  var newSchema = undefined;
  this.map(function(row) {
    if (typeof newSchema === "undefined") {
      newSchema = copySchema(row.schema, newNames, namesToChange);
    }
    var changedNames = swap(namesToChange);
    var newRow = new Row(row, newSchema, changedNames);
    return newRow;
  });
  return this;
}

DataFrame.prototype.rename = function(namesToChange) {
  if (typeof namesToChange !== "object") {
    return this;
  }
  var newSchema = undefined;
  this.map(function(row) {
    if (typeof newSchema === "undefined") {
      newSchema = copySchema(row.schema, undefined, namesToChange);
    }
    var changedNames = swap(namesToChange);
    var newRow = new Row(row, newSchema, changedNames);
    return newRow;
  });
  return this;
}


DataFrame.prototype.distinct = function() {
  var distinctObjects = Object.create(null);
  var distinctColumns = arguments.length > 0 ? [] : undefined;
  for (var i = 0; i < arguments.length; i++) {
    var arg = arguments[i];
    var argType = typeof arg;
    if (argType === "string") {
      distinctColumns.push(arg);      
    }
  }
  var filteredDistinctColumns = undefined;
  this.filter(function(row) {
    if (filteredDistinctColumns == undefined) {
      filteredDistinctColumns = [];
      for (var i = 0; i < distinctColumns.length; i++) {
        var distinctColumnName = distinctColumns[i];
        if (row.schema.columnNames.indexOf(distinctColumnName) > -1) {
          filteredDistinctColumns.push(distinctColumnName);
        }
      }
    }
    if (filteredDistinctColumns.length == 0) {
      return true;
    }
    var uniqueString = row.toUniqueString(filteredDistinctColumns);
    if (distinctObjects[uniqueString] !== undefined) {
      return false;
    } else {
      distinctObjects[uniqueString] = true;
      return true;
    }
  });
  return this;
}

DataFrame.prototype.summarize = function(summaries) {
  if (typeof(summaries) !== "object") {
    return this;
  }
  var summaryColumns = Object.keys(summaries);
  var accumulatorChain = function() { return new AccumulatorEnd(); }
  for (var i = 0; i < summaryColumns.length; i++) {
    var name = summaryColumns[i];
    var obj = summaries[name];
    var constructor;
    if (obj.prototype instanceof Operation) {
      constructor = obj;
      arguments = [obj, name];
    } else {
      constructor = Accumulator;
      arguments = [constructor, name];
      if (Array.isArray(obj)) {arguments.concat(obj);} else {arguments.push(obj);}
    }
    var newConstructor = constructor.bind.apply(constructor, arguments);
    accumulatorChain = addOperation(accumulatorChain, newConstructor);
  }
  this.addOperation(Reducer, accumulatorChain);
  this.addOperation(RowTransformer);
  this.combine();
  return this;
}

DataFrame.prototype.summarise = DataFrame.prototype.summarize;

DataFrame.prototype.groupBy = function(key) {
  var keyFunc;
  if (typeof key === "function") {
    keyFunc = key;
  } else if (typeof key === "string") {
    keyFunc = function(row) {return row[key];}
  } else if (typeof key === "number") {
    if (key % 0 !== 0 || (key > this.schema.columnNames.length)) {
      console.log("Error: bad key");
      return;
    } else {
      keyFunc = function(row) { return row.getByIndex(key) }
    }
  } else if (Array.isArray(key)) {
    keyfunc = function(row) {
      keyString = "";
      for (var i = 0; i < key.length; i++) {
        keyString += row[key];
      }
      return keyString;
    }
  }
  this.groupedOperations.push([keyFunc, []]);
  return this;
}

DataFrame.prototype.map = function(f) {
  this.addOperation(Map, f);
}

DataFrame.prototype.addOperation = function(constructor) {
  if (!constructor) return;
  var newConstructor = constructor.bind.apply(constructor, arguments);
  if (this.groupedOperations.length) {
    this.groupedOperations[this.groupedOperations.length - 1][1].push(newConstructor);
  } else {
    this.operations.push(newConstructor);
  }
}

DataFrame.prototype.combine = function() {
  groupByArray = this.groupedOperations.pop();
  this.addOperation(GroupByOperation, groupByArray[0], groupByArray[1]);
}

DataFrame.prototype.collect = function() {
  var final = function() { return new DataFrameBuilder() }
  var chain = addOperations(final, this.operations)
  var operation = chain();
  for (var i = 0; i < this.rowArray.length; i++) {
    row = this.rowArray[i];
    var result = operation.onNext(row);
    if (!result) break;
  }
  return operation.onCompleted();
}

DataFrame.prototype["@@iterator"] = function() {
  return new RowIterator(this);
}

DataFrame.prototype.setColumns = function() {
  var columns = [];
  var columnNames = this.schema.columnNames;
  var columnTypes = this.schema.columnTypes;
  var name, type;
  for (var i = 0; i < columnNames.length; i++) {
    name = columnNames[i];
    type = columnTypes[i];
    var column = new Column(this, name, type, i);
    columns[name] = column;
    columns.push(column);
  }
  this.columns = columns;
}

DataFrame.prototype.row = function(rowIndex) {
  return this.rowArray[rowIndex];
}

DataFrame.prototype.cell = function(rowIndex, colIndex) {
  var row = this.rowArray[rowIndex];
  return typeof row !== "undefined" ? row.get(colIndex) : undefined;
}

DataFrame.prototype.slice = function(startIndex, endIndex) {
  return new DataFrame(this.rowArray.slice(startIndex, endIndex), this.schema);
}

DataFrame.prototype.show = function() {
  var columnNames = this.schema.columnNames;
  console.log(columnNames.join(" "));
  for (var rowIndex = 0; rowIndex < this.rowArray.length; rowIndex++) {
    console.log(this.rowArray[rowIndex].toString());
  }
}

function Column(df, name, type, index) {
  this.df = df;
  this.name = name;
  this.type = type;
  this.index = index;
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

Row.getSchema = function(rowData, columnNames) {
  var columnNames = columnNames || Object.keys(rowData);
  var index = columnNames.indexOf("schema");
  if (index > -1) { columnNames.splice(index, 1) }
  var columnTypes = this.getColumnTypes(rowData, columnNames);
  return {columnNames: columnNames, columnTypes: columnTypes};
}

Row.getColumnTypes = function(rowData, columnNames) {
  var columnTypes = [];
  var name, value, valueType;
  for (var i = 0; i < columnNames.length; i++) {
    name = columnNames[i];
    value = rowData[name];
    valueType = typeof value;
    columnTypes.push(valueType);
  }
  return columnTypes;
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
  var columnNames = columns || this.schema.columnNames;
  var rowString = "";
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    rowString += this[name] + " ";
  }
  return rowString;
}

Row.prototype.toUniqueString = function(columns) {
  var columnNames = columns || this.schema.columnNames;
  var rowString = "";
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    rowString += this[name] + "|";
  }
  return rowString;
}

module.exports = DataFrame;

/*  var DataFrameRow = function(data) {
    Row.call(this, data);
  }
  DataFrameRow.prototype = Object.create(Row.prototype);
  DataFrameRow.prototype.columnNames = this.columnNames;
  DataFrameRow.prototype.columnTypes = this.columnTypes;
*/






