//>>excludeStart('excludeAfterBuild', pragmas.excludeAfterBuild)
define(['handlebars', 'underscore'], function(Handlebars, _) {

	function replaceLocaleStrings(ast, mapping, options) {
		options = options || {};
		mapping = mapping || {};
		if (!ast || ast.type !== 'Program' || !ast.body) {
			return ast;
		}
		_(ast.body).forEach(function(statement, i) {
			var newString = '<!-- i18n error -->';
			if (statement.type === 'MustacheStatement' && statement.path &&
					statement.path.original === '$') {
				if (statement.params.length && statement.params[0]) {
					var p0 = statement.params[0];
					var key = p0.type === 'StringLiteral' ? p0.value :
						(p0.string || p0.original);
					newString = mapping[key] ||
						(options.originalKeyFallback ? key : newString);
				}
				ast.body[i] = {
					type: 'ContentStatement',
					value: newString,
					original: newString,
				};
			} else if (statement.type === 'BlockStatement' ||
					statement.type === 'DecoratorBlock') {
				if (statement.program) {
					replaceLocaleStrings(statement.program, mapping, options);
				}
				if (statement.inverse) {
					replaceLocaleStrings(statement.inverse, mapping, options);
				}
			} else if (statement.type === 'PartialBlockStatement' &&
					statement.program) {
				replaceLocaleStrings(statement.program, mapping, options);
			}
		});
		return ast;
	}

	return function precompile(string, mapping, options) {
		var ast;

		options = options || {};

		if (!('data' in options)) {
			options.data = true;
		}

		if (options.compat) {
			options.useDepths = true;
		}

		ast = Handlebars.parse(string);

		if (mapping !== false) {
			ast = replaceLocaleStrings(ast, mapping, options);
		}

		return Handlebars.precompile(ast, options);
	};
});
//>>excludeEnd('excludeAfterBuild')
