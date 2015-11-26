var Rx = require("rx");

var IterateSequence = function(end) {
  sequence = Rx.Observable.range(0, end);
}


var Operations = {};
module.exports = Operations;
