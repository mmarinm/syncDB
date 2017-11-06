'use strict';

function createEndBreak(value) {
	var instance = Object.create(Error.prototype);
	instance.value = value;
	instance.__isEndBreak = true;
	return instance;
}

module.exports = function (val) {
	var err = createEndBreak(val);
	throw err;
};

module.exports.end = function (err) {
	if (err.__isEndBreak) {
		return err.value;
	}

	throw err;
};
