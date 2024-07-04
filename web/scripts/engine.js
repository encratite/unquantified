class ScriptingEngine {
	pattern = {
		// Statements
		assignment: /^\s*\$([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)\s*$/,
		call: /^\s*([a-z][A-Za-z0-9])\s+(.+?)\s*$/,
		// Values
		numeric: /^-?\d+(?:\.\d+)?$/,
		date: /^(\d{4})-(\d{2})-(\d{2})$/,
		dateTime: /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::?(\d{2}))$/,
		timeFrame: /^(\d+)(m|h)$/,
		offset: /^((?:\+|-)\d+)(m|h|d|w|mo|y)$/,
		ticker: /^[A-Z][A-Z0-9]+$/,
		array: /^\[(.+?)\]$/,
		variable: /^\$[A-Za-z0-9_]+$/
	};

	constructor(callHandlers) {
		this.variables = {};
		this.callHandlers = callHandlers;
	}

	run(script) {
		let input = script.trim();
		while (input.length > 0) {
			let assignment = pattern.assignment.exec(input);
			let call = pattern.call.exec(input);
			if (assignment != null) {
				this.processAssignment(assignment);
				input = input.substr(assignment.index);
			}
			else if (call != null) {
				this.processCall(call);
				input = input.substr(call.index);
			}
			else {
				const linePattern = /^.+/;
				const match = linePattern.exec(input)
				const line = match[0];
				throw new Error(`Unable to parse line: ${line}`);
			}
			input = input.trim();
		}
	}

	processAssignment(match) {
		const variable = match[1];
		const valueString = match[2];
		const value = this.getValueFromString(valueString);
		this.variables[variable] = value;
	}

	processCall(match) {
		throw new Error("Not implemented");
	}

	getValueFromString(valueString) {
		const parsers = [
			this.getNumeric,
			this.getDate,
			this.getDateTime,
			this.getTimeFrame,
			this.getOffset,
			this.getTicker,
			this.getArray,
			this.getVariable
		];
		for (const parser of parsers) {
			const value = parser(valueString);
			if (value != null)
				return value;
		}
		throw new Error(`Unable to parse value: ${valueString}`);
	}

	getNumeric(valueString) {
		const match = pattern.numeric.match(valueString);
		if (match != null) {
			const value = parseFloat(valueString);
			return value;
		}
		return null;
	}

	getDate(valueString) {
		const match = pattern.date.match(valueString)
		if (match != null) {
			const year = parseInt(match[1]);
			const month = parseInt(match[2]);
			const day = parseInt(match[3]);
			const value = new Date(year, month - 1, day);
			return value;
		}
		return null;
	}

	getDateTime(valueString) {
		const match = pattern.dateTime.match(valueString);
		if (match != null) {
			const year = parseInt(match[1]);
			const month = parseInt(match[2]);
			const day = parseInt(match[3]);
			const hours = parseInt(match[4]);
			const minutes = parseInt(match[5]);
			const secondsMatch = match[6];
			const seconds = secondsMatch != null ? parseInt(secondsMatch) : 0;
			const value = new Date(year, month - 1, day, hours, minutes, seconds);
			return value;
		}
		return null;
	}

	getTimeFrame(valueString) {
		const match = pattern.timeFrame.match(valueString);
		if (match != null) {
			throw new Error("Not implemented");
		}
		return null;
	}

	getOffset(valueString) {
		const match = pattern.offset.match(valueString);
		if (match != null) {
			throw new Error("Not implemented");
		}
		return null;
	}

	getTicker(valueString) {
		const match = pattern.ticker.match(valueString);
		if (match != null) {
			return match[0];
		}
		return null;
	}

	getArray(valueString) {
		const array = pattern.array.match(valueString);
		if (array != null) {
			throw new Error("Not implemented");
		}
		return null;
	}

	getVariable(valueString) {
		const variable = pattern.offset.match(valueString);
		if (variable != null) {
			const variableName = variable[1];
			const value = this.variables[variableName];
			if (value == null) {
				throw new Error(`Unknown variable: ${valueString}`);
			}
			return value;
		}
		return null;
	}
}