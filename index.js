var DataFrame = function(rowData, columnNames) {
  var columns = [];
  var rowIndex = new RowIndex;
  if (rowData.length) {
    if (typeof rowData[0] === "object") {
      this.withJSONData(rowData, columnNames, rowIndex);
    }
  }
  this.rowIndex = rowIndex;
  this.columns = columns;
}

DataFrame.prototype.withJSONData = function(rowData, columnNames, rowIndex) {
  var columns = [];
  var name;
  for (var colIndex = 0; colIndex < columnNames.length; colIndex++) {
    name = columnNames[colIndex];
    var columnExpression = new JSONColumnExpression(rowIndex, rowData, name);
    var column = new Column(name, columnExpression);
    columns.push(column);
    this[name] = column;
  }
  this.columns = columns;
}

DataFrame.prototype.collect() = function() {
  
}

var Column = function(name, columnExpression) {
  this.name = name;
  this.expression = new columnExpression;
}

Column.prototype.valueOf = function() {
  return this.expression.valueOf();
}

DataFrame.prototype.filter = function(func) {
  this.addExpression(Filter, func);
  return this;
}

DataFrame.prototype.mutate = function(newColumnName, arg) {
  var newColumn = new Column(this, newColumnName, newWidth, "number");
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
    this.addExpression(DistinctColumns);
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
    if (obj.prototype instanceof Expression) {
      constructor = obj;
      arguments = [obj, this.newSchema, name];
    } else {
      constructor = Accumulator;
      arguments = [constructor, this.newSchema, name];
      if (Array.isArray(obj)) {arguments.concat(obj);} else {arguments.push(obj);}
    }
    var newConstructor = constructor.bind.apply(constructor, arguments);
    accumulatorChain = addExpression(accumulatorChain, newConstructor);
  }
  this.addExpression(Reducer, accumulatorChain);
  var newSchema = new Schema(this, null, null, null, summaryColumns);
  this.newSchema = newSchema;
  this.addExpression(RowTransformer);
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
  this.groupedExpressions.unshift([this.newSchema, keyFunc, []]);
  return this;
}

DataFrame.prototype.addExpression = function(constructor) {
  var argLength = arguments.length;
  if (argLength === 0) return;
  var args = [constructor, this.newSchema];
  for (var i = 1; i < argLength; i++) {
    args.push(arguments[i]);
  }
  var newConstructor = constructor.bind.apply(constructor, args);
  if (this.groupedExpressions.length) {
    this.groupedExpressions[0][2].push(newConstructor);
  } else {
    this.expressions.push(newConstructor);
  }
}

DataFrame.prototype.combine = function() {
  var groupByArray = this.groupedExpressions.shift();
  var schema = groupByArray[0]
  var keyFunction = groupByArray[1];
  var groupedExpressions = groupByArray[2];
  var newConstructor = constructor.bind.apply(GroupBy, [GroupBy, schema, keyFunction, groupedExpressions]);
  this.expressions.push(newConstructor);
}

DataFrame.prototype.collect = function() {
  var newSchema = this.newSchema;
  var final = function() { return new DataFrameBuilder(newSchema); }
  var chain = addExpressions(final, this.expressions)
  var Expression = chain();
  for (var i = 0; i < this.dataArray.length; i++) {
    row = this.dataArray[i];
    var result = Expression.onNext(row);
    if (!result) break;
  }
  return Expression.onCompleted();
}


module.exports = DataFrame;



