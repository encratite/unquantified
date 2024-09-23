export const SECONDS_PER_DAY = 1440;

export const Keyword = {
	TRUE: "true",
	FALSE: "false",
	FIRST: "first",
	LAST: "last",
	NOW: "now",
	DAILY: "daily",
	ALL: "all"
};

export class Assignment {
	constructor(variable, value) {
		this.variable = variable;
		this.value = value;
	}
}

export class Call {
	constructor(name, callArguments) {
		this.name = name;
		this.callArguments = callArguments;
	}
}

export class Variable {
	constructor(name) {
		this.name = name;
	}
}

export class Value {
	getJsonValue() {
		throw new Error("Not implemented");
	}
}

export class BasicValue {
	constructor(value) {
		this.value = value;
	}

	getJsonValue() {
		return this.value;
	}
}

export class Numeric extends BasicValue {
	constructor(value) {
		super(value);
	}
}

export class Bool extends BasicValue {
	constructor(value) {
		super(value);
	}
}

// Either a Date object or one of the special keywords "first", "last", "now"
export class TimeParameter extends BasicValue {
	constructor(value) {
		super(value);
	}

	getJsonValue() {
		if (this.value instanceof luxon.DateTime) {
			return {
				date: TimeParameter.toFormat(this.value),
				offset: null,
				offsetUnit: null,
				specialKeyword: null
			};
		} else {
			return {
				date: null,
				offset: null,
				offsetUnit: null,
				specialKeyword: this.value
			};
		}
	}

	static toFormat(dateTime) {
		return dateTime.toFormat("yyyy-MM-dd'T'HH:mm:ss");
	}
}

export class TimeFrame extends BasicValue {
	constructor(value) {
		super(value);
	}
}

export class Offset extends Value {
	constructor(offset, unit) {
		super();
		this.offset = offset;
		this.unit = unit;
	}

	getJsonValue() {
		return {
			date: null,
			offset: this.offset,
			offsetUnit: this.unit,
			specialKeyword: null
		}
	}
}

// Either a symbol like "ES", "NQ" or the special keyword "all" 
export class Symbol extends BasicValue {
	constructor(value) {
		super(value);
		this.separator = false;
	}

	getJsonValue() {
		if (this.value === Keyword.ALL) {
			return [this.value];
		} else {
			return this.value;
		}
	}
}

export class SymbolArray extends BasicValue {
	constructor(value) {
		super(value);
	}

	getJsonValue() {
		return this.value.map(x => x.getJsonValue());
	}
}

export class String extends BasicValue {
	constructor(value) {
		super(value);
	}
}

export class Parameters extends BasicValue {
	constructor(value) {
		super(value);
	}
}

export class Parameter extends Value {
	constructor(name, value, limit, increment, values, boolValue, stringValue) {
		super();
		this.name = name;
		this.value = value;
		this.limit = limit;
		this.increment = increment;
		this.values = values;
		this.boolValue = boolValue;
		this.stringValue = stringValue;
	}
}

export class ScriptingEngine {
	constructor(callHandlers) {
		this.variables = {};
		this.callHandlers = callHandlers;
		this.grammar = null;
		this.semantics = null;
	}

	async initialize() {
		const grammarSource = await fetch("./scripts/unquantified.ohm")
			.then(response => response.text());
		this.grammar = ohm.grammar(grammarSource);
		this.semantics = this.grammar.createSemantics();
		this.semantics.addOperation("eval", {
			Program: statements => {
				return statements.eval();
			},
			Statement: (statement, _) => {
				return statement.eval();
			},
			Assignment: (variable, _, value) => {
				const assignment = new Assignment(variable.eval().name, value.eval());
				return assignment;
			},
			Call: (identifier, firstValue, _, otherValues) => {
				const callArguments = [firstValue.eval()].concat(otherValues.eval());
				const call = new Call(identifier.sourceString, callArguments);
				return call;
			},
			SimpleCall: (identifier) => {
				const call = new Call(identifier.sourceString, []);
				return call;
			},
			Parameters: (_, __, first, ___, ____, others, _____, ______) => {
				const parameterArray = [first.eval()].concat(others.eval());
				const parameters = new Parameters(parameterArray);
				return parameters;
			},
			Parameter: (identifier, _, parameterValue) => {
				const parameter = parameterValue.eval();
				parameter.name = identifier.eval();
				return parameter;
			},
			ValueRangeParameter: (value, __, limit, ___, increment) => {
				const getValue = x => {
					let value = x.sourceString !== "" ? x.eval() : null;
					while (value instanceof Array) {
						if (value.length > 0) {
							value = value[0];
						} else {
							value = null;
						}
					}
					return value;
				};
				const parameter = new Parameter(null, value.eval(), getValue(limit), getValue(increment), null, null, null);
				return parameter;
			},
			MultiValueParameter: (_, first, __, others, ___) => {
				const values = [first.eval()].concat(others.eval());
				const parameter = new Parameter(null, null, null, null, values, null, null);
				return parameter;
			},
			SymbolArray: (_, first, others, __) => {
				const elements = [first.eval()].concat(others.eval());
				const array = new SymbolArray(elements);
				return array;
			},
			SeparatedSymbol: (separator, symbol) => {
				const output = symbol.eval();
				if (separator.sourceString === "|") {
					output.separator = true;
				}
				return output;
			},
			identifier: (first, others) => {
				return first.sourceString + others.sourceString;
			},
			variable: (_, identifier) => {
				const name = identifier.eval();
				const variable = new Variable(name);
				return variable;
			},
			numeric: (first, _, fractional) => {
				let numericString = first.sourceString + fractional.sourceString;
				return parseFloat(numericString);
			},
			numericPrefix: (negative, first, others) => {
				const numericString = negative + first + others;
				return numericString;
			},
			date: (year1, year2, year3, year4, _, month1, month2, __, day1, day2) => {
				const yearString = year1.sourceString + year2.sourceString + year3.sourceString + year4.sourceString;
				const monthString = month1.sourceString + month2.sourceString;
				const dayString = day1.sourceString + day2.sourceString;
				const yearInt = parseInt(yearString);
				const monthInt = parseInt(monthString);
				const dayInt = parseInt(dayString);
				const date = this.createDate(yearInt, monthInt, dayInt);
				const dateTime = new TimeParameter(date);
				return dateTime;
			},
			dateTime: (date, _, hour1, hour2, __, minute1, minute2, ___, second1, second2) => {
				const dateTime = date.eval().value;
				const hoursString = hour1.sourceString + hour2.sourceString;
				const minutesString = minute1.sourceString + minute2.sourceString;
				const secondsString = second1 ? (second1.sourceString + second2.sourceString) : null;
				const hours = parseInt(hoursString);
				const minutes = parseInt(minutesString);
				const seconds = secondsString ? parseInt(secondsString) : 0;
				const hms = {
					hours: hours,
					minutes: minutes,
					seconds
				};
				dateTime.set(hms);
				return dateTime;
			},
			offset: (sign, first, others, unit) => {
				const offsetString = sign.sourceString + first.sourceString + (others ? others.sourceString : "");
				const offsetInt = parseInt(offsetString);
				const offset = new Offset(offsetInt, unit.sourceString);
				return offset;
			},
			timeFrame: (first, others, unit) => {
				const timeFrameString = first.sourceString + (others ? others.sourceString : "");
				let timeFrameInt = parseInt(timeFrameString);
				if (unit.sourceString === "h") {
					timeFrameInt *= 60;
				}
				const timeFrame = new TimeFrame(timeFrameInt);
				return timeFrame;
			},
			symbol: (first, others) => {
				const symbol = first.sourceString + others.sourceString;
				return new Symbol(symbol);
			},
			string: (_, content, __) => {
				return content.sourceString;
			},
			stringValue: content => {
				const stringValue = content.eval();
				const fileName = new String(stringValue);
				return fileName;
			},
			stringParameter: content => {
				const stringValue = content.eval();
				const parameter = new Parameter(null, null, null, null, null, null, stringValue);
				return parameter;
			},
			keyword: keyword => {
				const string = keyword.sourceString;
				switch (string) {
					case Keyword.TRUE:
						return new Bool(true);
					case Keyword.FALSE:
						return new Bool(false);
					case Keyword.FIRST:
					case Keyword.LAST:
					case Keyword.NOW:
						return new TimeParameter(string);
					case Keyword.DAILY:
						return new TimeFrame(SECONDS_PER_DAY);
					case Keyword.ALL:
						return new Symbol(string);
				}
				throw new Error(`Unknown keyword: ${keyword.sourceString}`);
			},
			bool: value => {
				const boolValue = value.sourceString === "true";
				const parameter = new Parameter(null, null, null, null, null, boolValue, null);
				return parameter;
			},
			whitespace: _ => {
				return null;
			},
			eol: (_, __) => {
				return null;
			},
			_iter: (...children) => {
				return children.map(item => item.eval());
			}
		});
		this.initializeSerialization();
	}

	initializeSerialization() {
		luxon.DateTime.prototype.toKVIN = (o, kvin) => {
			return {args: [TimeParameter.toFormat(o)]};
		};
		const types = [
			Variable,
			Numeric,
			Bool,
			TimeParameter,
			TimeFrame,
			Offset,
			Symbol,
			SymbolArray,
			String,
			Parameter
		];
		for (const i in types) {
			const type = types[i];
			KVIN.userCtors[type.name] = type;
		}
		KVIN.userCtors.DateTime = (isoString) => {
			const dateTime = luxon.DateTime.fromISO(isoString);
			return dateTime;
		};
	}

	async run(script) {
		const match = this.grammar.match(script);
		if (!match.succeeded()) {
			const errorPosition = match.getRightmostFailurePosition();
			throw new Error(`Failed to parse script (position ${errorPosition})`);
		}
		const syntaxTree = this.semantics(match).eval();
		for (const statement of syntaxTree) {
			if (statement instanceof Assignment) {
				const values = this.substituteVariables([statement.value]);
				this.variables[statement.variable] = values[0];
			} else if (statement instanceof Call) {
				const handler = this.callHandlers[statement.name];
				if (handler == null) {
					throw new Error(`Unknown call: ${statement.name}`);
				}
				const callArguments = this.substituteVariables(statement.callArguments);
				await handler(callArguments);
			} else {
				throw new Error("Unknown statement");
			}
		}
	}

	serializeVariables() {
		return KVIN.serialize(this.variables);
	}

	deserializeVariables(data) {
		this.variables = KVIN.deserialize(data);
	}

	substituteVariables(input) {
		const output = input.map(x => {
			if (x instanceof Variable) {
				const value = this.variables[x.name];
				if (value == null) {
					throw new Error(`Unknown variable: ${x.name}`);
				}
				return value;
			} else {
				return x;
			}
		});
		return output;
	}

	createDate(year, month, day) {
		const ymd = {
			year: year,
			month: month,
			day: day
		};
		const dateTime = luxon.DateTime.fromObject(ymd);
		return dateTime;
	}
}