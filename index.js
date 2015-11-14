var Schema = require("./schema");
var Operations = require("./operations");

var Operation = Operations.Operation;
var DataFrameBuilder = Operations.DataFrameBuilder;
var Reducer = Operations.Reducer;
var RowTransformer = Operations.RowTransformer;
var Accumulator = Operations.Accumulator;
var AccumulatorEnd = Operations.AccumulatorEnd;
var Distinct = Operations.Distinct;
var Mutate = Operations.Mutate;
var Filter = Operations.Filter;
var GroupBy = Operations.GroupBy;
var GroupEnd = Operations.GroupEnd;
var addOperation = Operations.addOperation;
var addOperations = Operations.addOperations;

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

var DataFrame = function(data, schema) {
  var dataArray = [];
  if (data.length) {
    if (Array.isArray(data[0])) {
      this.dataArray = data;
      this.schema = schema;
    } else {
      this.schema = schema || new Schema(this, data[0]);
      var columnNames = this.schema.getColumnNames();
      var rowObject, rowArray, name;
      for (var rowIndex = 0; rowIndex < data.length; rowIndex++) {
        rowObject = data[rowIndex];     
        rowArray = [];
        for (var colIndex = 0; colIndex < columnNames.length; colIndex++) {
          name = columnNames[colIndex];
          rowArray.push(rowObject[name]);
        }
        dataArray.push(rowArray);
        this.dataArray = dataArray;
      }
    }
  } else {
    this.schema = new Schema(this, {});
  }
  this.newSchema = this.schema;
  this.operations = [];
  this.groupedOperations = [];
}

DataFrame.prototype.filter = function(func) {
  this.addOperation(Filter, func);
  return this;
}

DataFrame.prototype._new = function(rows, schema) {
  return new DataFrame(rows, schema);
}

DataFrame.prototype.mutate = function(newColumnName, func) {
  this.addOperation(Mutate, func);
  var newWidth = this.newSchema.width;
  var newColumn = new Schema.Column(this, newColumnName, newWidth, "number");
  this.newSchema = new Schema(this, this.newSchema, null, null, [newColumn]);
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
  var newSchema = new Schema(this, this.newSchema, newNames, namesToChange);
  this.newSchema = newSchema;
  return this;
}

DataFrame.prototype.rename = function(namesToChange) {
  if (typeof namesToChange !== "object") {
    return this;
  }
  var newSchema = new Schema(this, this.newSchema, null, namesToChange);
  this.newSchema = newSchema;
  return this;
}


DataFrame.prototype.distinct = function() {
  var distinctObjects = Object.create(null);
  var distinctColumns = [];
  var newSchema = this.newSchema;
  var keyFunction;
  if (arguments.length === 1 && typeof arguments[0] === "function") {
    keyFunction = arguments[0];
  } else {
    if (arguments.length === 0) {
      var list = newSchema.list;
      for (var i = 0; i < list.length; i++) {
        distinctColumns.push(newSchema.list[i].index);
      }
    } else {
      for (var i = 0; i < arguments.length; i++) {
        var arg = arguments[i];
        var argType = typeof arg;
        if (argType === "string" && newSchema.hasOwnKey(arg)) {
          distinctColumns.push(newSchema[arg].index);      
        }
      }
    }
    this.addOperation(DistinctColumns);
  }
  return this;
}

DataFrame.prototype.summarize = function(summaries) {
  if (typeof(summaries) !== "object") {
    return this;
  }
  var summaryColumnNames = Object.keys(summaries);
  var accumulatorChain = function() { return new AccumulatorEnd(); }
  var summaryColumns = [];
  for (var i = 0; i < summaryColumnNames.length; i++) {
    var name = summaryColumnNames[i];
    summaryColumns.push(new Schema.Column(this, name, "number", i));
    var obj = summaries[name];
    var constructor;
    if (obj.prototype instanceof Operation) {
      constructor = obj;
      arguments = [obj, this.newSchema, name];
    } else {
      constructor = Accumulator;
      arguments = [constructor, this.newSchema, name];
      if (Array.isArray(obj)) {arguments.concat(obj);} else {arguments.push(obj);}
    }
    var newConstructor = constructor.bind.apply(constructor, arguments);
    accumulatorChain = addOperation(accumulatorChain, newConstructor);
  }
  this.addOperation(Reducer, accumulatorChain);
  var newSchema = new Schema(this, null, null, null, summaryColumns);
  this.newSchema = newSchema;
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
  this.groupedOperations.unshift([this.newSchema, keyFunc, []]);
  return this;
}

DataFrame.prototype.addOperation = function(constructor) {
  var argLength = arguments.length;
  if (argLength === 0) return;
  var args = [constructor, this.newSchema];
  for (var i = 1; i < argLength; i++) {
    args.push(arguments[i]);
  }
  var newConstructor = constructor.bind.apply(constructor, args);
  if (this.groupedOperations.length) {
    this.groupedOperations[0][2].push(newConstructor);
  } else {
    this.operations.push(newConstructor);
  }
}

DataFrame.prototype.combine = function() {
  var groupByArray = this.groupedOperations.shift();
  var schema = groupByArray[0]
  var keyFunction = groupByArray[1];
  var groupedOperations = groupByArray[2];
  var newConstructor = constructor.bind.apply(GroupBy, [GroupBy, schema, keyFunction, groupedOperations]);
  this.operations.push(newConstructor);
}

DataFrame.prototype.collect = function() {
  var newSchema = this.newSchema;
  var final = function() { return new DataFrameBuilder(newSchema); }
  var chain = addOperations(final, this.operations)
  var operation = chain();
  for (var i = 0; i < this.dataArray.length; i++) {
    row = this.dataArray[i];
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
  return this.dataArray[rowIndex];
}

DataFrame.prototype.cell = function(rowIndex, colIndex) {
  var row = this.dataArray[rowIndex];
  return typeof row !== "undefined" ? row.get(colIndex) : undefined;
}

DataFrame.prototype.slice = function(startIndex, endIndex) {
  return new DataFrame(this.dataArray.slice(startIndex, endIndex), this.schema);
}

DataFrame.prototype.show = function() {
  console.log(this.schema.toString());
  for (var rowIndex = 0; rowIndex < this.dataArray.length; rowIndex++) {
    console.log(this.dataArray[rowIndex]);
  }
}


module.exports = DataFrame;



