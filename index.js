var Promise = require("bluebird");
var Rx = require("rx");


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
  } else if (typeof original === "array") {
    copy = [];
    for (var i = 0; i < init.length; i++) {
      copy.push(original[i]);
    }
  } else {
    return init;
  }
}


/* Credited to jlongster: https://github.com/jlongster/transducers.js/blob/master/transducers.js */
function compose(funcs) {
  return function(r) {
    var value = r;
    for(var i=funcs.length-1; i>=0; i--) {
      value = funcs[i](value);
    }
    return value;
  }
}

function Filter(f, xform) {
  this.xform = xform;
  this.f = f;
}

Filter.prototype['@@transducer/init'] = function(init) {
  return this.xform['@@transducer/init'](init);
};

Filter.prototype['@@transducer/result'] = function(res) {
  return this.xform['@@transducer/result'](res);
};

Filter.prototype['@@transducer/step'] = function(res, input) {
  if(this.f(input)) {
    return this.xform['@@transducer/step'](res, input);
  }
  return res;
};

function filter(f) {
  return function(xform) {
    return new Filter(f, xform);
  };
}

function Map(f, xform) {
  this.xform = xform;
  this.f = f;
}

Map.prototype['@@transducer/init'] = function() {
  return this.xform['@@transducer/init']();
};

Map.prototype['@@transducer/result'] = function(v) {
  return this.xform['@@transducer/result'](v);
};

Map.prototype['@@transducer/step'] = function(res, input) {
  return this.xform['@@transducer/step'](res, this.f(input));
};

function map(f) {
  return function(xform) {
    return new Map(f, xform);
  }
}

var DataFrameBuilder = function(xform) {
  this._xform = xform;
}

DataFrameBuilder.prototype['@@transducer/init'] = function(init) {
  return this._xform["@@transducer/init"](init);
};

DataFrameBuilder.prototype['@@transducer/result'] = function(res) {
  res = this._xform["@@transducer/result"](res);
  return new DataFrame([res], {schema: res.schema, safe: true});
};

DataFrameBuilder.prototype['@@transducer/step'] = function(res, input) {
  return this._xform["@@transducer/step"](res, input);
};

var GroupByTransformer = function(keyFunction, groupTransformer, xform) {
  this._keyFunction = keyFunction;
  this._groupTransformer = groupTransformer;
  this._xform = xform;
}

GroupByTransformer.prototype['@@transducer/init'] = function(init) {
  this._init = init;
  return this._xform["@@transducer/init"]({});
};

GroupByTransformer.prototype['@@transducer/result'] = function(res) {
  res[this._name] = this._singleXform["@@transducer/result"](res[this._name]);
  return this._xform["@@transducer/result"](res);
};

GroupByTransformer.prototype['@@transducer/step'] = function(res, input) {
  var key = this._keyFunction(input);
  var groupRes;
  if (res.hasOwnKey(key)) {
    groupRes = res[key];
  } else {
    groupRes = this._groupTransformer["@@transducer/init"](clone(this._init));
  }
  res[key] = this._groupTransformer["@@transducer/step"](groupRes, input);
  return this._xform["@@transducer/step"](res, input);
};

var SummarizerTransformer = function(summaryTransformer, xform) {
  this._summaryTransformer = summaryTransformer;
  this._xform = xform;
}

GroupByTransformer.prototype['@@transducer/init'] = function(init) {
  this._init = init;
  return this._xform["@@transducer/init"]({});
};

GroupByTransformer.prototype['@@transducer/result'] = function(res) {
  res[this._name] = this._singleXform["@@transducer/result"](res[this._name]);
  return this._xform["@@transducer/result"](res);
};

GroupByTransformer.prototype['@@transducer/step'] = function(res, input) {
  var key = this._keyFunction(input);
  var groupRes;
  if (res.hasOwnKey(key)) {
    groupRes = res[key];
  } else {
    groupRes = this._groupTransformer["@@transducer/init"](clone(this._init));
  }
  res[key] = this._groupTransformer["@@transducer/step"](groupRes, input);
  return this._xform["@@transducer/step"](res, input);
};


/*
Initializes with an object.
*/


var ParallelTransformer = function(xform) {
  this._xform = xform;
}

ParallelTransformer.prototype['@@transducer/init'] = function(init) {
  return this._xform["@@transducer/init"]({});
};

ParallelTransformer.prototype['@@transducer/result'] = function(res) {
  return this._xform["@@transducer/result"](res);
};

ParallelTransformer.prototype['@@transducer/step'] = function(res, input) {
  return this._xform["@@transducer/step"](res, input);
};

/*
SingleTransformer: Saves the results of a transformation under a particular key.
*/

var SingleTransformer = function(name, singleXform, xform) {
  this._name = name;
  this._singleXform = singleXform;
  this._xform = xform;
}

SingleTransformer.prototype['@@transducer/init'] = function(init) {
  init[this._name] = this._singleXform["@@transducer/init"](init);
  return this._xform["@@transducer/init"](init);
};

SingleTransformer.prototype['@@transducer/result'] = function(res) {
  res[this._name] = this._singleXform["@@transducer/result"](res[this._name]);
  return this._xform["@@transducer/result"](res);
};

SingleTransformer.prototype['@@transducer/step'] = function(res, input) {
  res[this._name] = this._singleXform["@@transducer/step"](res[this._name], input);
  return this._xform["@@transducer/step"](res, input);
};

/*
RowReducer: Turns an object into a row.
*/

var RowReducer = function(xform) {
  this._xform = xform;
}

RowReducer.prototype['@@transducer/init'] = function(init) {
  return this._xform["@@transducer/init"](init);
};

RowReducer.prototype['@@transducer/result'] = function(res) {
  var row = new Row(res, Row.getSchema(res));
  return this._xform["@@transducer/result"](row);
};

RowReducer.prototype['@@transducer/step'] = function(res, input) {
  return this._xform["@@transducer/step"](res, input);
};


var NonTransformer = function(xform) {
  this._xform = xform;
}

NonTransformer = function() {};

NonTransformer.prototype['@@transducer/init'] = function(init) {
  return init;
};

NonTransformer.prototype['@@transducer/result'] = function(res) {
  return res;
};

NonTransformer.prototype['@@transducer/step'] = function(res, input) {
  return res;
};

var GenericReducer = function(accumulator, xform) {
  this._accumulator = accumulator;
  this._xform = xform;
}

GenericReducer.prototype['@@transducer/init'] = function(init) {
  return null;
};

GenericReducer.prototype['@@transducer/result'] = function(res) {
  return res;
};

GenericReducer.prototype['@@transducer/step'] = function(res, input) {
  return this._accumulator(res, input);
};

var parallelTransform = function(xform) {
  return new ParallelTransformer(xform);
}

var singleTransform = function(name, singleXform) {
  return function(xform) {
    return new SingleTransformer(name, singleXform, xform);    
  }
}

var rowReduce = function(xform) {
  return new RowReducer(xform);
}

var dataFrameBuild = function(xform) {
  return new DataFrameBuilder(xform);
}

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
  }
  this.rowArray = rowArray;
  this.transformer = new NonTransformer();
  this.transducers = [];
}

DataFrame.prototype.filter = function(func) {
  this.transducers.push(filter(func));
  return this;
}

DataFrame.prototype.mutate = function(newColumnName, func) {
  var newSchema = undefined;
  var newType = undefined;
  var transducer = map(function(row) {
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
  this.transducers.push(transducer);
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
  var transducer = map(function(row) {
    if (typeof newSchema === "undefined") {
      newSchema = copySchema(row.schema, newNames, namesToChange);
    }
    var changedNames = swap(namesToChange);
    var newRow = new Row(row, newSchema, changedNames);
    return newRow;
  });
  this.transducers.push(transducer);
  return this;
}

DataFrame.prototype.rename = function(namesToChange) {
  if (typeof namesToChange !== "object") {
    return this;
  }
  var newSchema = undefined;
  var transducer = map(function(row) {
    if (typeof newSchema === "undefined") {
      newSchema = copySchema(row.schema, undefined, namesToChange);
    }
    var changedNames = swap(namesToChange);
    var newRow = new Row(row, newSchema, changedNames);
    return newRow;
  });
  this.transducers.push(transducer);
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
  var transducer = filter(function(row) {
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
  this.transducers.push(transducer);
  return this;
}

DataFrame.prototype.summarize = function(summaries) {
  if (typeof(summaries) !== "object") {
    return this;
  }
  var summaryColumns = Object.keys(summaries);
  this.transducers.push(parallelTransform);
  for (var i = 0; i < summaryColumns.length; i++) {
    var name = summaryColumns[i];
    var obj = summaries[name];
    var columnTransformer;
    if (!obj["@@transducer/step"]) {
      // this is not a transformer, so make one
      // assuming for now that it's an accumulator function
      columnTransformer = new GenericReducer(obj);
    } else {
      columnTransformer = obj;
    }
    this.transducers.push(singleTransform(name, columnTransformer));
  }
  this.transducers.push(rowReduce);
  return this;
}

DataFrame.prototype.summarise = DataFrame.prototype.summarize;

DataFrame.prototype.collect = function() {
  this.transducers.push(dataFrameBuild);
  var transducer = compose(this.transducers);
  var transformer = transducer(this.transformer);
  var result = transformer["@@transducer/init"]();
  var row;
  for (var i = 0; i < this.rowArray.length; i++) {
    row = this.rowArray[i];
    result = transformer["@@transducer/step"](result, row);
  }
  return transformer["@@transducer/result"](result);
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






