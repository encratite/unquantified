export const SecondsPerDay = 1440;

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
		if (this.value instanceof Date) {
			return this.value.toISOString();
		}
		else {
			return this.value;
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
		throw new Error("Offsets cannot be directly evaluated");
	}
}

// Either a ticker like "ES", "NQ" or the special keyword "all" 
export class Ticker extends BasicValue {
	constructor(value) {
		super(value);
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

export class FileName extends BasicValue {
	constructor(value) {
		super(value);
	}
}

export class Parameters extends BasicValue {
	constructor(value) {
		super(value);
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
			Program: (commands) => {
				commands.eval();
			},
			Command: (command, _) => {
				command.eval();
			},
			Assignment: (variable, _, value) => {
				console.log("Assignment", variable.eval(), value.eval());
			},
			Call: (identifier, firstValue, _, otherValues) => {
				console.log("Call", identifier.eval(), firstValue.eval(), otherValues.eval());
			},
			Parameters: (_, __, first, ___, ____, others, _____, ______) => {
				return [first.eval()].concat(others.eval());
			},
			Parameter: (identifier, _, from, __, to, ___, step) => {
				return {
					identifier: identifier.eval(),
					from: from.eval(),
					to: to.eval(),
					step: step.eval()
				};
			},
			Array: (_, first, __, others, ___) => {
				return [first.eval()].concat(others.eval());
			},
			identifier: (first, others) => {
				return first.sourceString + others.sourceString;
			},
			variable: (_, identifier) => {
				return identifier.eval();
			},
			numeric: (negative, first, others, _, fractional) => {
				let numericString = (negative ? negative.sourceString : "") + first.sourceString + (others ? others.sourceString : "");
				if (fractional) {
					numericString += `.${fractional.sourceString}`;
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
				const date = new Date(yearInt, monthInt - 1, dayInt);
				return date;
			},
			dateTime: (date, _, hour1, hour2, __, minute1, minute2, ___, second1, second2) => {
				const dateObject = date.eval();
				const hoursString = hour1.sourceString + hour2.sourceString;
				const minutesString = minute1.sourceString + minute2.sourceString;
				const secondsString = second1 ? (second1.sourceString + second2.sourceString) : null;
				const hours = parseInt(hoursString);
				const minutes = parseInt(minutesString);
				const seconds = secondsString ? parseInt(secondsString) : 0;
				const dateTime = new Date(dateObject.getYear(), dateObject.getMonth(), dateObject.getDate(), hours, minutes, seconds);
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
				const timeFrameInt = parseInt(timeFrameString);
				if (unit.sourceString === "h") {
					timeFrameInt *= 60;
				}
				const timeFrame = new TimeFrame(timeFrameInt);
				return timeFrame;
			},
			ticker: chars => {
				return new Ticker(chars.sourceString);
			},
			string: (_, content, __) => {
				return content.sourceString;
			},
			keyword: keyword => {
				switch (keyword.sourceString) {
					case "true":
						return new Bool(true);
					case "false":
						return new Bool(false);
					case "first":
						throw new Error("Not implemented");
					case "last":
						throw new Error("Not implemented");
					case "now":
						return new DateTime(new Date());
					case "daily":
						return new TimeFrame(SecondsPerDay);
					case "all":
						throw new Error("Not implemented");
				}
				throw new Error(`Unknown keyword: ${keyword.sourceString}`);
			},
			whitespace: _ => {
				return null;
			},
			eol: _ => {
				return null;
			},
			_iter: items => {
				return items.children.map(item => item.eval());
			}
		});
	}

	async run(script) {
		const match = this.grammar.match(script);
		if (!match.succeeded()) {
			const errorPosition = match.getRightmostFailurePosition();
			throw new Error(`Failed to parse script (position ${errorPosition})`);
		}
		const result = this.semantics(match).eval();
	}

	async performCall(command, commandArguments) {
		const handler = this.callHandlers[command];
		if (handler == null) {
			throw new Error(`Unknown command: ${command}`);
		}
		await handler(commandArguments);
	}
}