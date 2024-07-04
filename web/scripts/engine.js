class Value {
	getJsonValue() {
		throw new Error("Not implemented");
	}
}

class BasicValue {
	constructor(value) {
		this.value = value;
	}

	getJsonValue() {
		return this.value;
	}
}

class Numeric extends BasicValue {
	constructor(value) {
		super(value);
	}
}

class Bool extends BasicValue {
	constructor(value) {
		super(value);
	}
}

// Either a Date object or one of the special keywords "first", "last", "now"
class DateTime extends BasicValue {
	constructor(value) {
		super(value);
	}

	getJsonValue() {
		if (this.value instanceof Date) {
			return this.value.toISOString();
		}
		else {
			return this.value;
		}
	}
}

class TimeFrame extends Value {
	constructor(minutes) {
		super();
		this.minutes = minutes;
	}
}

class Offset extends Value {
	constructor(offset, unit) {
		super();
		this.offset = offset;
		this.unit = unit;
	}

	getJsonValue() {
		throw new Error("Offsets cannot be directly evaluated");
	}
}

// Either a ticker like "ES", "NQ" or the special keyword "all" 
class Ticker extends BasicValue {
	constructor(value) {
		super(value);
	}
}

class Array extends BasicValue {
	constructor(value) {
		super(value);
	}

	getJsonValue() {
		return this.value.map(x => x.getJsonValue());
	}
}

class FileName extends BasicValue {
	constructor(value) {
		super(value);
	}
}

export default class ScriptingEngine {
	pattern = {
		// Statements
		assignment: /^\$([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)\s*?(?:\n|$)/,
		arrayOnlyCall: /^([a-z][A-Za-z0-9]*)\s+(\[.+?\])\s*?(?:\n|$)/,
		arrayCall: /^([a-z][A-Za-z0-9]*)\s+(\[.+?\]\s*,\s*)?(.+?)\s*?(?:\n|$)/,
		call: /^([a-z][A-Za-z0-9]*)\s+(.+?)\s*?(?:\n|$)/,
		// Values
		numeric: /^-?\d+(?:\.\d+)?$/,
		date: /^(?:(\d{4})-(\d{2})-(\d{2})|first|last|now)$/,
		dateTime: /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2}))?$/,
		timeFrame: /^(?:(\d+)(m|h)|daily)$/,
		offset: /^((?:\+|-)\d+)(m|h|d|w|mo|y)$/,
		ticker: /^(?:[A-Z][A-Z0-9]+|all)$/,
		array: /^\[(.+?)\]$/,
		variable: /^\$([A-Za-z_][A-Za-z0-9_]*)$/,
		bool: /^(?:true|false)$/,
		fileName: /^[a-z][a-z0-9_]*$/,
	};

	constructor(callHandlers) {
		this.variables = {};
		this.callHandlers = callHandlers;
	}

	async run(script) {
		let input = script.trim();
		const matchPatterns = [
			[this.pattern.assignment, this.processAssignment.bind(this)],
			[this.pattern.arrayOnlyCall, this.processArrayOnlyCall.bind(this)],
			[this.pattern.arrayCall, this.processArrayCall.bind(this)],
			[this.pattern.call, this.processCall.bind(this)]
		];
		while (input.length > 0) {
			let matched = false;
			for (let tuple of matchPatterns) {
				const pattern = tuple[0];
				const handler = tuple[1];
				const match = pattern.exec(input);
				if (match != null) {
					await handler(match);
					input = input.substr(match[0].length).trim();
					matched = true;
					break;
				}
			}
			if (!matched) {
				const linePattern = /^.+/;
				const match = linePattern.exec(input)
				const line = match[0];
				throw new Error(`Unable to parse line: ${line}`);
			}
		}
	}

	async processAssignment(match) {
		const variable = match[1];
		const valueString = match[2];
		const value = this.getValueFromString(valueString);
		this.variables[variable] = value;
	}

	async processArrayOnlyCall(match) {
		const command = match[1];
		const array = this.getArray(match[2]);
		await this.performCall(command, [array]);
	}

	async processArrayCall(match) {
		const command = match[1];
		const array = this.getArray(match[2]);
		const otherArguments = this.getCallArguments(match[3]);
		const callArguments = [array].concat(otherArguments);
		await this.performCall(command, callArguments);
	}

	async processCall(match) {
		const command = match[1];
		const callArguments = this.getCallArguments(match[2]);
		await this.performCall(command, callArguments);
	}

	getValueFromString(valueString) {
		const parsers = [
			this.getNumeric.bind(this),
			this.getDate.bind(this),
			this.getDateTime.bind(this),
			this.getTimeFrame.bind(this),
			this.getOffset.bind(this),
			this.getTicker.bind(this),
			this.getArray.bind(this),
			this.getVariable.bind(this),
			this.getBool.bind(this),
			this.getFileName.bind(this),
		];
		for (const parser of parsers) {
			const value = parser(valueString);
			if (value != null)
				return value;
		}
		throw new Error(`Unable to parse value: ${valueString}`);
	}

	getNumeric(valueString) {
		const match = this.pattern.numeric.exec(valueString);
		if (match != null) {
			const value = parseFloat(valueString);
			const numeric = new Numeric(value);
			return numeric;
		}
		return null;
	}

	getDate(valueString) {
		const match = this.pattern.date.exec(valueString)
		if (match != null) {
			const keyword = match[0];
			if (
				keyword === "first" ||
				keyword === "last" ||
				keyword === "now"
			) {
				const dateTime = new DateTime(keyword);
				return dateTime;
			}
			else {
				const year = parseInt(match[1]);
				const month = parseInt(match[2]);
				const day = parseInt(match[3]);
				const value = new Date(year, month - 1, day);
				const dateTime = new DateTime(value);
				return dateTime;
			}
		}
		return null;
	}

	getDateTime(valueString) {
		const match = this.pattern.dateTime.exec(valueString);
		if (match != null) {
			const year = parseInt(match[1]);
			const month = parseInt(match[2]);
			const day = parseInt(match[3]);
			const hours = parseInt(match[4]);
			const minutes = parseInt(match[5]);
			const secondsMatch = match[6];
			const seconds = secondsMatch != null ? parseInt(secondsMatch) : 0;
			const value = new Date(year, month - 1, day, hours, minutes, seconds);
			const dateTime = new DateTime(value);
			return dateTime;
		}
		return null;
	}

	getTimeFrame(valueString) {
		const match = this.pattern.timeFrame.exec(valueString);
		if (match != null) {
			const value = parseInt(match[1]);
			const unit = match[2];
			let minutes = null;
			if (unit === "m") {
				minutes = value;
			}
			else if (unit === "h") {
				minutes = 60 * value;
			}
			else if (match[0] === "daily") {
				minutes = 1440;
			}
			else {
				throw new Error(`Unable to parse time frame: ${valueString}`);
			}
			const timeFrame = new TimeFrame(minutes);
			return timeFrame;
		}
		return null;
	}

	getOffset(valueString) {
		const match = this.pattern.offset.exec(valueString);
		if (match != null) {
			const value = parseInt(match[1]);
			const unit = match[2];
			const offset = new Offset(value, unit);
			return offset;
		}
		return null;
	}

	getTicker(valueString) {
		const match = this.pattern.ticker.exec(valueString);
		if (match != null) {
			const ticker = new Ticker(match[0]);
			return ticker;
		}
		return null;
	}

	getArray(valueString) {
		const match = this.pattern.array.exec(valueString);
		if (match != null) {
			const values = match[1]
				.split(",")
				.map(x => this.getValueFromString(x.trim()));
			const array = new Array(values);
			return values;
		}
		return null;
	}

	getVariable(valueString) {
		const match = this.pattern.variable.exec(valueString);
		if (match != null) {
			const variableName = match[1];
			const value = this.variables[variableName];
			if (value == null) {
				throw new Error(`Unknown variable: ${valueString}`);
			}
			return value;
		}
		return null;
	}

	getBool(valueString) {
		const match = this.pattern.bool.exec(valueString);
		if (match != null) {
			const boolean = match[0] === "true";
			const value = new Bool(boolean);
			return boolean;
		}
		return null;
	}

	getFileName(valueString) {
		const match = this.pattern.fileName.exec(valueString);
		if (match != null) {
			const value = new FileName(match[0]);
			return value;
		}
		return null;
	}

	async performCall(command, commandArguments) {
		const handler = this.callHandlers[command];
		if (handler == null) {
			throw new Error(`Unknown command: ${command}`);
		}
		await handler(commandArguments);
	}

	getCallArguments(argumentString) {
		const callArguments = argumentString
			.split(",")
			.map(x => this.getValueFromString(x.trim()));
		return callArguments;
	}
}