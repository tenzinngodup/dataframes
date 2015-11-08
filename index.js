var jStat = require("jStat").jStat;
require('console.table');

function retNull() { return null; }

jStat.nulls = function(rows, cols) {
  if (!jStat.utils.isNumber(cols))
    cols = rows;
  return jStat.create(rows, cols, retNull);
}

jStat.emptyArrays = function(rows, cols) {
  if (!jStat.utils.isNumber(cols)) {
    cols = rows;
  }
  var emptyArray = new Array(rows);
  for (var rowIndex = 0; rowIndex < rows; rowIndex++) {
    emptyArray[rowIndex] = new Array(cols);
  }
  return emptyArray;
}

function RowIterator(df) {
  this.dataFrame = df;
  this.numRows = df.numRows;
  this.rowIndex = 0;
}

RowIterator.prototype.next() {
  if (this.index <= this.numRows) {
    return { done: false, value: RowPointer(this.rowIndex) }
  } else {
    return { done: true }
  }
}

function RowPointer = function(df, rowIndex) {
  this.df = df;
}

function DataFrame(data, options) {
  if (data.length) {
    if (options && typeof options == "object" && options.type) {
      switch (options.type) {
        case "columns":
          this.initWithColumns(data);          
          break;
        case "rows":
          this.initWithRows(data);
          break;
      }
    } else {
      this.initWithRows(data);          
    }    
  } else {
    this.numRows = 0;
    this.columns = [];
  }
}

DataFrame.prototype.initWithColumns = function(data) {
  this.numRows = data[0].length;
  this.columns = new Array(data.length);
  for (var columnIndex in data) {
    var column = data[columnIndex];
    if (column.length != this.numRows) {
      var emptyColumn = newArray(this.numRows);
      for (var i = 0; i < column.length; i++) {
        newColumn[i] = null;
      }
      this.columns[columnIndex] = emptyColumn;
    } else {
      this.columns[columnIndex] = column;     
    }
  }
}
  
DataFrame.prototype.initWithRows = function(data) {
  this.numRows = data.length;
  var numCols = this.numRows > 0 ? data[0].length : 0;
  this.columns = jStat.emptyArrays(numCols, this.numRows);
  for (var rowIndex = 0; rowIndex < data.length; rowIndex++) {
    var row = data[rowIndex];
    while (row.length > this.columns.length) {
      this.columns.push(jStat.nulls(this.numRows, 1));
    }
    var colIndex = 0;
    while (colIndex < row.length) {
      this.columns[colIndex][rowIndex] = row[colIndex];
      colIndex++;
    }
    while (colIndex < this.columns.length) {
      this.columns[colIndex] = null;
      colIndex++;
    }
  }
}

DataFrame.prototype.addColumn = function(column) {
  if(this.columnArray.length === 0) {
    this.numRows = 0;
  } else {
    if (columnArray.length !== this.numRows) {
      console.log("Error: new column has wrong number of rows.");
      return;
    }
  }
  this.columns.push(columnArray);
}
  
DataFrame.prototype.applymap = function(func) {
  // apply a function to every element
  return this.apply(function(column) { column.map(func) });
}

DataFrame.prototype.apply = function(func, axis) {
  // apply a function to every column or row
  if (axis === 1) {
    var newRows = new Array(this.numRows);
    for (var i = 0; i < this.numRows; i++) {
      newRows[i] = (func(this.row(i)), i);
    }
    var newDf = new DataFrame(newRows, {type: "rows"});
    return newDf;
  } else {
    var newCols = this.columns.map(func);
    var newDf = new DataFrame(newCols, {type: "cols"});
    return newDf;
  }
}

DataFrame.prototype.row = function(rowIndex) {
  var row = this.columns.map(function(column) { return column[rowIndex] });
  return row;
}

DataFrame.prototype.slice = function(startIndex, endIndex) {
  var newCols = this.columns.map(function(column) { return column.slice(startIndex, endIndex) });
  var newDF = new DataFrame(newCols, {type: "columns"});
  return newDF;
}

DataFrame.prototype.col = function(colIndex) {
  return this.columns[colIndex];
}

DataFrame.prototype.cell = function(rowIndex, colIndex) {
  return this.columns[colIndex][rowIndex];
}

DataFrame.prototype.show = function() {
  console.log("Rows: %d", this.numRows);
  console.table(jStat.transpose(this.columns));
  console.log(this.columns[0][4]);
}



module.exports = DataFrame;







