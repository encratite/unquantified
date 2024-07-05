"use strict";

const ace = window.ace;
const oop = ace.require("ace/lib/oop");
const TextHighlightRules = ace.require("ace/mode/text_highlight_rules").TextHighlightRules;

const UnquantifiedHighlightRules = function() {
	this.$rules = {
		"start": [
			{
				token: "keyword",
				regex: /(?:true|false|first|last|now|daily|all|to|step)\b/
			}, {
				token: "operator",
				regex: "="
			}, {
				token: "date",
				regex: /\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}(?::\d{2})?)?\b/
			}, {
				token: "timeframe",
				regex: /\d+(?:m|h)/
			}, {
				token: "date-offset",
				regex: /(?:\+|-)\d+(?:m|h|d|w|mo|y)\b/
			}, {
				token: "numeric",
				regex: /-?\d+(?:\.\d+)?\b/
			}, {
				token : "variable",
				regex : /\$[A-Za-z_][A-Za-z0-9_]*/
			}, {
				token : "ticker",
				regex : /[A-Z]{2,}/
			}, {
				token : "command",
				regex : /^[A-Za-z]+\b/
			}
		]
	};

	this.normalizeRules();
};

oop.inherits(UnquantifiedHighlightRules, TextHighlightRules);

export const UnquantifiedMode = function() {
	this.HighlightRules = UnquantifiedHighlightRules;
};
const TextMode = ace.require("ace/mode/text").Mode;
oop.inherits(UnquantifiedMode, TextMode);

UnquantifiedMode.prototype.getTokenizer = function() {
	const tokenizer = ace.require("ace/tokenizer");
	const rules = new UnquantifiedHighlightRules().getRules();
	return new tokenizer.Tokenizer(rules);
};