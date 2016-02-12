"use strict";

var DataFrame = require("./dataframe.js");
var Formulas = require("./formulas.js");

var sum = Formulas.sum;
var cumsum = Formulas.cumsum;

var data = [
  {
    "first_name": "George",
    "last_name": "Washington",
    "party": "No Party",
    "pres_number": 1,
    "death_year": 1799,
    "inauguration_year": 1789,
    "last_year": 1797
  },
  {
    "first_name": "John",
    "last_name": "Adams",
    "party": "Federalist",
    "pres_number": 2,
    "death_year": 1826,
    "inauguration_year": 1797,
    "last_year": 1801
  },
  {
    "first_name": "Thomas",
    "last_name": "Jefferson",
    "party": "Democratic-Republican",
    "pres_number": 3,
    "death_year": 1826,
    "inauguration_year": 1801,
    "last_year": 1809
  },
  {
    "first_name": "James",
    "last_name": "Madison",
    "party": "Democratic-Republican",
    "pres_number": 4,
    "death_year": 1836,
    "inauguration_year": 1809,
    "last_year": 1817
  }
];

var columnNames = ["first_name",
                   "last_name",
                   "party",
                   "pres_number",
                   "death_year",
                   "inauguration_year",
                   "last_year"];

var df = new DataFrame({rows: data, columnNames: columnNames})
  .arrange("last_name")


console.log(df);

