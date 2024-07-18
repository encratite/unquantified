export const SecondsPerDay = 1440;

export const Keyword = {
	True: "true",
	False: "false",
	First: "first",
	Last: "last",
	Now: "now",
	Daily: "daily",
	All: "all"
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
export class DateTime extends BasicValue {
	constructor(value) {
		super(value);
	}

	getJsonValue() {
		if (this.value instanceof luxon.DateTime) {
			return {
				date: this.value.toISO(),
				offset: null,
				offsetUnit: null,
				specialKeyword: null
			};
		}
		else {
			return {
				date: null,
				offset: null,
				offsetUnit: null,
				specialKeyword: this.value
			};
		}
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
		if (this.value === Keyword.All) {
			return [this.value];
		}
		else {
			return this.value;
		}
	}
}

export class Array extends BasicValue {
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
	constructor(name, value, limit, increment) {
		super();
		this.name = name;
		this.value = value;
		this.limit = limit;
		this.increment = increment;
	}
}

export class ScriptingEngine {
	constructor(callHandlers) {
		this.variables = {};
		this.callHandlers = callHandlers;
		this.grammar = null;
		this.semantics = null;
		this.timezone = null;
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
			Parameters: (_, __, first, ___, ____, others, _____, ______) => {
				const parameters = new Parameters([first.eval()].concat(others.eval()));
				return parameters;
			},
			Parameter: (identifier, _, from, __, to, ___, step) => {
				const parameter = new Parameter(identifier.eval(), from.eval(), to.eval(), step.eval());
				return parameter;
			},
			Array: (_, first, others, __) => {
				const elements = [first.eval()].concat(others.eval());
				const array = new Array(elements);
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
			numeric: (negative, first, others, _, fractional) => {
				let numericString = (negative ? negative.sourceString : "") + first.sourceString + (others ? others.sourceString : "");
				if (fractional) {
					numericString = `${numericString}.${fractional.sourceString}`;
				}
				return parseFloat(numericString);
			},
			date: (year1, year2, year3, year4, _, month1, month2, __, day1, day2) => {
				const yearString = year1.sourceString + year2.sourceString + year3.sourceString + year4.sourceString;
				const monthString = month1.sourceString + month2.sourceString;
				const dayString = day1.sourceString + day2.sourceString;
				const yearInt = parseInt(yearString);
				const monthInt = parseInt(monthString);
				const dayInt = parseInt(dayString);
				const date = this.createDate(yearInt, monthInt, dayInt);
				const dateTime = new DateTime(date);
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
			symbol: chars => {
				return new Symbol(chars.sourceString);
			},
			string: (_, content, __) => {
				const fileName = new String(content.sourceString);
				return fileName;
			},
			keyword: keyword => {
				const string = keyword.sourceString;
				switch (string) {
					case Keyword.True:
						return new Bool(true);
					case Keyword.False:
						return new Bool(false);
					case Keyword.First:
					case Keyword.Last:
					case Keyword.Now:
						return new DateTime(string);
					case Keyword.Daily:
						return new TimeFrame(SecondsPerDay);
					case Keyword.All:
						return new Symbol(string);
				}
				throw new Error(`Unknown keyword: ${keyword.sourceString}`);
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
			}
			else if (statement instanceof Call) {
				const handler = this.callHandlers[statement.name];
				if (handler == null) {
					throw new Error(`Unknown call: ${statement.name}`);
				}
				const callArguments = this.substituteVariables(statement.callArguments);
				await handler(callArguments);
			}
			else {
				throw new Error("Unknown statement");
			}
		}
	}

	setTimezone(timezone) {
		this.timezone = timezone;
	}

	substituteVariables(input) {
		const output = input.map(x => {
			if (x instanceof Variable) {
				const value = this.variables[x.name];
				if (value == null) {
					throw new Error(`Unknown variable: ${x.name}`);
				}
				return value;
			}
			else {
				return x;
			}
		});
		return output;
	}

	createDate(year, month, day) {
		const options = {};
		if (this.timezone != null) {
			options.zone = this.timezone;
		}
		const ymd = {
			year: year,
			month: month,
			day: day
		};
		const dateTime = luxon.DateTime.fromObject(ymd, options);
		return dateTime;
	}
}