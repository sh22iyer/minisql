whitespace		[ \t\n]
asterisk		[\*]
comma			[,]
eq			"="
gt			">"
ne                      "!="
ob                      "("
cb			")"
underscore		[_]
letter			[A-Za-z]
digit			[0-9]
intconst		[+\-]?{digit}+
stringliteral		\".*\"
identifier		{letter}({letter}|{digit}|{underscore})*
%%
select			return SELECT;
from			return FROM;
where			return WHERE;
order			return ORDER;
group			return GROUP;
by			return BY;
sum			return SUM;
count			return COUNT;
having                  return HAVING;
{eq}			return EQ;
{gt}			return GT;
{ne}			return NE;
{ob}			return OB;
{cb}			return CB;
{comma}			return COMMA;
{identifier}		{ strcpy(s_id_names[s_id_index],yytext);
			  yylval = s_id_index; s_id_index++;
			  return IDENTIFIER; }
{intconst}		{ yylval = atoi(yytext); return INTEGER; }
{stringliteral}		{ strcpy(s_id_names[s_id_index],yytext);
                          yylval = s_id_index; s_id_index++;
                          return STRINGLITERAL; }
{asterisk}		return ASTERISK;
{whitespace}		;
%%
